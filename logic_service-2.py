"""
logic_service.py

QA Logic Service — orchestrates the full QA workflow:

  Turn 1  : Receive CSV mapping file content
  Turn 2  : Validation type selection (1-5)
  Turn 3  : Environment selection (dev / build / ist)
             → optional, defaults to config.json default_environment
             → configures source/target project, dataset, table references
  Turn 4+ : Requirements agent → SQL (parallel batched)
             → Supervisor critique → Executor (BQ)

Agents:
  - dynamic_agent      : requirements parser  (InMemoryRunner)
  - sql_agent          : SQL generator        (InMemoryRunner, parallel batched)
  - supervisor_agent   : SQL critic + fix     (InMemoryRunner)
  - executor_agent     : BQ test execution    (InMemoryRunner)
"""

import os
import re
import json
import asyncio
import traceback
from typing import List, Dict, Any, Tuple, Optional

from google.genai import types
from google.adk.runners import InMemoryRunner

from requirements_parsing_agent.agent import dynamic_agent
from tests_to_sql_agent.agent import root_agent as sql_agent
from supervisor_agent.agent import root_agent as supervisor_agent
from executor_agent.agent import root_agent as executor_agent
from executor.config_loader import (
    load_environment_config,
    get_default_environment,
    list_environments,
)

from tests_to_sql_agent.patterns import (
    DDL_VALIDATION_PATTERN,
    COLUMN_TRANSFORMATION_PATTERN,
    COMPLETENESS_PATTERN,
    UNIQUENESS_PATTERN,
)

from gcs_utils import generate_default_filename, save_to_gcs, parse_gcs_path

# ── SQL pattern lookup ────────────────────────────────────────────────────────
_ALL_PATTERNS = (
    COLUMN_TRANSFORMATION_PATTERN + "\n"
    + DDL_VALIDATION_PATTERN + "\n"
    + COMPLETENESS_PATTERN + "\n"
    + UNIQUENESS_PATTERN
)

SQL_PATTERNS = {
    "1": DDL_VALIDATION_PATTERN,        "ddl_validation":        DDL_VALIDATION_PATTERN,
    "2": COMPLETENESS_PATTERN,          "completeness":          COMPLETENESS_PATTERN,
    "3": UNIQUENESS_PATTERN,            "uniqueness":            UNIQUENESS_PATTERN,
    "4": COLUMN_TRANSFORMATION_PATTERN, "column_transformation": COLUMN_TRANSFORMATION_PATTERN,
    "5": _ALL_PATTERNS,                 "all":                   _ALL_PATTERNS,
}

VALIDATION_NAMES = {
    "1": "ddl_validation", "2": "completeness", "3": "uniqueness",
    "4": "column_transformation", "5": "all",
}

VALID_CHOICES = set(VALIDATION_NAMES.keys()) | set(VALIDATION_NAMES.values())

VALIDATION_MENU = """Hi, Welcome to QA Test Case Generation!

Please select a validation type:
1. ddl_validation          - Verify target table DDL and datatypes
2. completeness            - Check row counts align after business rules
3. uniqueness              - Verify primary keys (no duplicates/nulls)
4. column_transformation   - Verify source-to-target mappings
5. all                     - Run all validations

Please type 1, 2, 3, 4, or 5 to continue."""


def _env_menu() -> str:
    envs    = list_environments()
    default = get_default_environment()
    lines   = [
        "Please select an environment to run against:\n",
        *[f"  • {e}" + (" (default)" if e == default else "") for e in envs],
        f"\nType the environment name or press Enter to use default ({default}).",
    ]
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# QALogicService
# ─────────────────────────────────────────────────────────────────────────────

class QALogicService:

    def __init__(self):
        self.req_runner        = InMemoryRunner(agent=dynamic_agent.agent)
        self.sql_runner        = InMemoryRunner(agent=sql_agent)
        self.supervisor_runner = InMemoryRunner(agent=supervisor_agent)
        self.executor_runner   = InMemoryRunner(agent=executor_agent)

        # Workflow state
        self.stored_csv_content:       Optional[str]  = None
        self.selected_validation_type: Optional[str]  = None
        self.selected_environment:     Optional[str]  = None
        self.env_config:               Optional[Dict] = None  # loaded on env selection

    # ── Session initialisation ────────────────────────────────────────────────

    async def start_session(self, user_id: str, session_id: str) -> None:
        """Initialise sessions for all four runners."""
        for runner in (
            self.req_runner, self.sql_runner,
            self.supervisor_runner, self.executor_runner,
        ):
            await runner.session_service.create_session(
                app_name=runner.app_name,
                user_id=user_id,
                session_id=session_id,
            )

    # ── Main entry point ──────────────────────────────────────────────────────

    async def process_turn(
        self,
        user_id: str,
        session_id: str,
        message_text: str,
    ) -> Dict[str, Any]:
        """
        Route each turn of the dialogue:

          Turn 1  → CSV content detected   → store, show validation menu
          Turn 2  → Validation choice      → store, show environment menu
          Turn 3  → Environment choice     → load config, start req agent
          Turn 4+ → Forward to req agent
        """
        stripped = message_text.strip()

        # ── Turn 1: CSV upload ────────────────────────────────────────────────
        if self.selected_validation_type is None and self._is_csv_content(message_text):
            self.stored_csv_content = message_text
            return {"text": VALIDATION_MENU, "outputs": {}}

        # ── Turn 2: Validation selection ──────────────────────────────────────
        if self.selected_validation_type is None and stripped in VALID_CHOICES:
            return await self._handle_validation_selection(stripped)

        # ── Turn 3: Environment selection ─────────────────────────────────────
        if self.selected_validation_type and self.env_config is None:
            return await self._handle_env_selection(
                user_id, session_id, stripped
            )

        # ── Turn 4+: Requirements agent ───────────────────────────────────────
        return await self._run_req_agent(user_id, session_id, message_text)

    # ── Turn 2 handler ────────────────────────────────────────────────────────

    async def _handle_validation_selection(
        self, validation_choice: str
    ) -> Dict[str, Any]:
        """Store validation type, configure dynamic_agent, show env menu."""
        self.selected_validation_type = validation_choice
        dynamic_agent.update_for_validation_type(validation_choice)
        print(f"[DEBUG] Validation type set: {validation_choice}")
        return {"text": _env_menu(), "outputs": {}}

    # ── Turn 3 handler ────────────────────────────────────────────────────────

    async def _handle_env_selection(
        self,
        user_id: str,
        session_id: str,
        user_input: str,
    ) -> Dict[str, Any]:
        """
        Load environment config.
        Accepts: 'dev', 'build', 'ist' — or empty/Enter → uses default.
        Then immediately runs the requirements agent if CSV is already stored.
        """
        # Empty input → use default
        env_choice = user_input.lower() if user_input else get_default_environment()
        if not env_choice:
            env_choice = get_default_environment()

        try:
            self.env_config        = load_environment_config(env_choice)
            self.selected_environment = self.env_config["environment"]
        except ValueError as e:
            # Invalid env name — ask again
            return {
                "text": (
                    f"⚠️  {str(e)}\n\n"
                    + _env_menu()
                ),
                "outputs": {},
            }

        src = self.env_config["source"]
        tgt = self.env_config["target"]

        confirm = (
            f"✅ Environment set: **{self.selected_environment.upper()}**\n\n"
            f"  Source : `{src['project_id']}.{src['dataset']}`\n"
            f"  Target : `{tgt['project_id']}.{tgt['dataset']}`\n\n"
        )

        # If CSV already uploaded → start requirements agent immediately
        if self.stored_csv_content:
            confirm += "Processing your mapping file now...\n"
            result   = await self._run_req_agent(
                user_id, session_id, self.stored_csv_content
            )
            result["text"] = confirm + "\n" + result.get("text", "")
            return result

        confirm += "Please paste your mapping CSV content to begin."
        return {"text": confirm, "outputs": {}}

    # ── Requirements agent runner ─────────────────────────────────────────────

    async def _run_req_agent(
        self,
        user_id: str,
        session_id: str,
        message_text: str,
    ) -> Dict[str, Any]:
        """Send message to requirements parser agent and collect response."""
        message    = types.Content(parts=[types.Part(text=message_text)])
        agent_text = []
        outputs    = {}

        async for event in self.req_runner.run_async(
            user_id=user_id,
            session_id=session_id,
            new_message=message,
        ):
            if event.content and event.content.parts:
                for part in event.content.parts:
                    if part.text:
                        agent_text.append(part.text)
            if event.actions and event.actions.state_delta:
                outputs.update(event.actions.state_delta)

        return {"text": "\n".join(agent_text), "outputs": outputs}

    # ── Conversational Q&A ────────────────────────────────────────────────────

    async def handle_conversational_query(
        self,
        user_id: str,
        session_id: str,
        query: str,
        context_type: str = "general",
        current_test_cases: Optional[str] = None,
        current_sql: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Route conversational queries to req or SQL agent."""
        if context_type == "test_cases":
            ctx = (
                f"CONTEXT: Test case question/explanation request.\n"
                f"USER REQUEST: {query}\n\n"
                "Please answer the question about the test cases or provide explanation."
            )
            if current_test_cases:
                ctx += f"\nTEST CASES USED:\n{current_test_cases}"

        elif context_type == "sql":
            ctx = (
                "CONTEXT: SQL question/explanation — "
                "DO NOT GENERATE NEW SQL, ONLY ANSWER THE QUESTION.\n"
            )
            if current_test_cases:
                ctx += f"\nTEST CASES USED:\n{current_test_cases}"
            if current_sql:
                ctx += f"\nCURRENT GENERATED SQL:\n{current_sql}"
            ctx += (
                f"\nUSER QUESTION: {query}\n\n"
                "Please answer the question about the SQL. "
                "DO NOT regenerate or modify — just explain what exists."
            )
        else:
            ctx = query

        runner = (
            self.req_runner
            if context_type in ("test_cases", "general")
            else self.sql_runner
        )

        message    = types.Content(parts=[types.Part(text=ctx)])
        agent_text = []
        outputs    = {}

        async for event in runner.run_async(
            user_id=user_id,
            session_id=session_id,
            new_message=message,
        ):
            if event.content and event.content.parts:
                for part in event.content.parts:
                    if part.text:
                        agent_text.append(part.text)
            if event.actions and event.actions.state_delta:
                outputs.update(event.actions.state_delta)

        return {"text": "\n".join(agent_text), "outputs": outputs}

    # ── SQL Generation (parallel batching) ───────────────────────────────────

    async def generate_sql(
        self,
        user_id: str,
        session_id: str,
        test_cases_json: str,
        batch_size: int = 8,
    ) -> Dict[str, Any]:
        """
        Generate SQL from test cases JSON using parallel batching.
        Uses self.env_config for source/target project + dataset references.
        After generation: clean → validate PKs → supervisor → executor.
        """
        # Resolve env config — fall back to default if not set
        if self.env_config is None:
            default_env    = get_default_environment()
            self.env_config = load_environment_config(default_env)
            self.selected_environment = self.env_config["environment"]
            print(f"[DEBUG] No env selected — using default: {default_env}")

        env_config = self.env_config

        if test_cases_json:
            print(f"[DEBUG] First 200 chars: {test_cases_json[:200]}...")

        # Build env reference string injected into SQL agent prompt
        src = env_config["source"]
        tgt = env_config["target"]
        env_reference = (
            f"ENVIRONMENT: {self.selected_environment.upper()}\n"
            f"Source project : {src['project_id']}\n"
            f"Source dataset : {src['dataset']}\n"
            f"Target project : {tgt['project_id']}\n"
            f"Target dataset : {tgt['dataset']}\n"
            f"BigQuery location: {src.get('location', 'US')}\n"
        )

        # Build validation context + pattern
        validation_context = ""
        if self.selected_validation_type:
            v_name  = VALIDATION_NAMES.get(
                self.selected_validation_type, self.selected_validation_type
            )
            pattern = self._get_pattern_for_validation_type(
                self.selected_validation_type
            )
            validation_context = (
                f"VALIDATION TYPE: {v_name}\n\n{pattern}\n\n"
            )

        # Parse test cases
        try:
            cleaned    = self._clean_json_markdown(test_cases_json)
            test_data  = json.loads(cleaned)
            test_cases = test_data.get("test_cases", [])
            table_metadata = {
                "source_table":         test_data.get("source_table", src["dataset"]),
                "target_table":         test_data.get("target_table", tgt["dataset"]),
                "primary_key_mappings": test_data.get("primary_key_mappings", []),
                "source_primary_key":   test_data.get("source_primary_key", ""),
                "target_primary_key":   test_data.get("target_primary_key", ""),
            }
        except (json.JSONDecodeError, KeyError, TypeError):
            test_cases     = []
            table_metadata = {
                "source_table": src["dataset"], "target_table": tgt["dataset"],
                "primary_key_mappings": [],
                "source_primary_key": "", "target_primary_key": "",
            }

        total_cases = len(test_cases)
        if total_cases == 0:
            print("⚠️  No test cases to process")
            return {
                "text": "No test cases found in JSON.",
                "outputs": {"aql_tests_json": '{"test_cases": []}'},
            }

        print(f"🔄 PARALLEL BATCHING: {total_cases} test cases in batches of {batch_size}")
        result = await self._generate_sql_batched(
            user_id, session_id, test_cases,
            env_config, env_reference, validation_context,
            batch_size, table_metadata,
        )

        # ── Post-generation: supervisor only ─────────────────────────────────
        # Executor is NOT called automatically.
        # User is prompted in the CLI after GCS export — call
        # execute_test_cases() explicitly from the orchestrator.
        final_json_str = result.get("outputs", {}).get("aql_tests_json", "")

        if final_json_str:
            supervisor_result = await self._run_supervisor(
                user_id, session_id, test_cases_json, final_json_str
            )
            result["supervisor"] = supervisor_result

        return result

    async def _generate_sql_batched(
        self,
        user_id: str,
        session_id: str,
        test_cases: List[Dict],
        env_config: Dict,
        env_reference: str,
        validation_context: str,
        batch_size: int,
        table_metadata: Dict,
    ) -> Dict[str, Any]:
        """Execute all batches in parallel via asyncio.gather."""
        batches     = self._chunk_test_cases(test_cases, batch_size)
        print(f"🔀 Created {len(batches)} batches for parallel processing")

        batch_tasks = [
            self._process_batch(
                user_id, session_id, batch, idx,
                env_reference, validation_context, table_metadata,
            )
            for idx, batch in enumerate(batches)
        ]

        batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)

        all_cases       = []
        combined_text   = []
        all_pk_issues   = []

        for idx, result in enumerate(batch_results):
            if isinstance(result, Exception):
                print(f"❌ Batch {idx + 1} failed: {result}")
                continue
            all_cases.extend(result.get("test_cases", []))
            combined_text.append(result.get("agent_text", ""))
            all_pk_issues.extend(result.get("pk_validation_issues", []))
            print(f"✅ Batch {idx + 1}: {len(result.get('test_cases', []))} cases")

        if all_pk_issues:
            print(f"⚠️  PK VALIDATION ISSUES:")
            for issue in all_pk_issues:
                print(f"   - {issue}")
        else:
            print(f"✅ PRIMARY KEY VALIDATION: All batches passed")

        final_json = {
            "test_cases": all_cases,
            **{k: v for k, v in table_metadata.items() if k != "test_cases"},
        }
        final_json_str = json.dumps(final_json, indent=2)
        print(f"✅ BATCHING COMPLETE: {len(all_cases)} total cases")

        return {
            "text":    "\n".join(combined_text),
            "outputs": {"aql_tests_json": final_json_str},
        }

    async def _process_batch(
        self,
        user_id: str,
        session_id: str,
        batch: List[Dict],
        batch_idx: int,
        env_reference: str,
        validation_context: str,
        table_metadata: Dict,
    ) -> Dict[str, Any]:
        """Process a single batch of test cases."""
        batch_session_id = f"{session_id}_batch_{batch_idx}"

        await self.sql_runner.session_service.create_session(
            app_name=self.sql_runner.app_name,
            user_id=user_id,
            session_id=batch_session_id,
        )

        batch_json = json.dumps({
            **table_metadata,
            "test_cases": batch,
        }, indent=2)

        prompt = (
            f"{env_reference}\n\n"
            f"{validation_context}"
            f"JSON TEST CASES:\n{batch_json}"
        )

        message    = types.Content(parts=[types.Part(text=prompt)])
        agent_text = []
        outputs    = {}

        async for event in self.sql_runner.run_async(
            user_id=user_id,
            session_id=batch_session_id,
            new_message=message,
        ):
            if event.content and event.content.parts:
                for part in event.content.parts:
                    if part.text:
                        agent_text.append(part.text)
            if event.actions and event.actions.state_delta:
                outputs.update(event.actions.state_delta)

        if not outputs.get("aql_tests_json"):
            print(f"⚠️  Batch {batch_idx + 1}: No aql_tests_json in outputs")
            return {
                "test_cases": [], "agent_text": "\n".join(agent_text),
                "pk_validation_issues": [],
            }

        cleaned  = self._clean_sql_output(outputs["aql_tests_json"])
        repaired = self._repair_json_structure(cleaned)

        try:
            data       = json.loads(repaired)
            test_cases = data if isinstance(data, list) else data.get("test_cases", [])
        except json.JSONDecodeError as e:
            print(f"❌ Batch {batch_idx + 1} JSON decode failed: {e}")
            return {
                "test_cases": [], "agent_text": "\n".join(agent_text),
                "pk_validation_issues": [],
            }

        # PK validation
        pk_issues       = []
        validated_cases = []

        for i, tc in enumerate(test_cases):
            if tc.get("sql_query"):
                is_valid, msg = self._validate_pk_usage_in_sql(tc, tc["sql_query"])
                if not is_valid:
                    print(f"⚠️  TC{i + 1}: {msg}")
                    pk_issues.append(f"TC{i + 1}: {msg}")
                    tc["sql_generation_status"] = "PK_VALIDATION_FAILED"
                    tc["pk_validation_error"]   = msg
            validated_cases.append(tc)

        return {
            "test_cases":           validated_cases,
            "agent_text":           "\n".join(agent_text),
            "pk_validation_issues": pk_issues,
        }

    # ── Supervisor ────────────────────────────────────────────────────────────

    async def _run_supervisor(
        self,
        user_id: str,
        session_id: str,
        original_test_cases: str,
        generated_sql: str,
    ) -> Dict[str, Any]:
        """Run supervisor critique + auto-fix on generated SQL."""
        supervisor_session = f"{session_id}_supervisor"

        try:
            await self.supervisor_runner.session_service.create_session(
                app_name=self.supervisor_runner.app_name,
                user_id=user_id,
                session_id=supervisor_session,
            )
        except Exception:
            pass

        payload = json.dumps({
            "mapping_csv":     self.stored_csv_content or "",
            "validation_type": self.selected_validation_type or "unknown",
            "environment":     self.selected_environment or "unknown",
            "env_config": {
                "source": self.env_config.get("source", {}),
                "target": self.env_config.get("target", {}),
            } if self.env_config else {},
            "test_cases":    original_test_cases,
            "generated_sql": generated_sql,
        }, indent=2)

        message    = types.Content(parts=[types.Part(text=payload)])
        agent_text = []
        outputs    = {}

        async for event in self.supervisor_runner.run_async(
            user_id=user_id,
            session_id=supervisor_session,
            new_message=message,
        ):
            if event.content and event.content.parts:
                for part in event.content.parts:
                    if part.text:
                        agent_text.append(part.text)
            if event.actions and event.actions.state_delta:
                outputs.update(event.actions.state_delta)

        raw = outputs.get("supervisor_result") or "\n".join(agent_text)

        try:
            clean    = re.sub(r'```(?:json)?|```', '', raw).strip()
            critique = json.loads(clean)
            print(f"✅ Supervisor: {critique.get('overall_status')} | "
                  f"score: {critique.get('confidence_score')} | "
                  f"{critique.get('fix_summary', '')}")
            return critique
        except (json.JSONDecodeError, ValueError):
            print("⚠️  Supervisor returned non-JSON output")
            return {"overall_status": "unknown", "raw": raw}

    # ── Executor ──────────────────────────────────────────────────────────────

    async def _run_executor(
        self,
        user_id: str,
        session_id: str,
        sql_json: str,
    ) -> Dict[str, Any]:
        """
        Execute approved SQL test cases on BigQuery.
        Waits for agent to finish, then returns raw output.
        The orchestrator handles all display and user interaction.
        """
        executor_session = f"{session_id}_executor"

        try:
            await self.executor_runner.session_service.create_session(
                app_name=self.executor_runner.app_name,
                user_id=user_id,
                session_id=executor_session,
            )
        except Exception:
            pass

        payload = json.dumps({
            "approved_sql":    sql_json,
            "env_config":      self.env_config,
            "environment":     self.selected_environment,
            "validation_type": self.selected_validation_type or "unknown",
        }, indent=2)

        message    = types.Content(parts=[types.Part(text=payload)])
        agent_text = []
        outputs    = {}

        async for event in self.executor_runner.run_async(
            user_id=user_id,
            session_id=executor_session,
            new_message=message,
        ):
            if event.content and event.content.parts:
                for part in event.content.parts:
                    if part.text:
                        agent_text.append(part.text)
            if event.actions and event.actions.state_delta:
                outputs.update(event.actions.state_delta)

        return {
            "text":    "\n".join(agent_text),
            "outputs": outputs,
            # Store session info so user can ask follow-up questions
            "session": {
                "user_id":    user_id,
                "session_id": executor_session,
            }
        }

    async def chat_with_executor(
        self,
        user_id: str,
        executor_session_id: str,
        message_text: str,
    ) -> Dict[str, Any]:
        """
        Send a follow-up message to the executor agent on its existing session.
        Used when user wants to ask questions about execution results.
        """
        message    = types.Content(parts=[types.Part(text=message_text)])
        agent_text = []
        outputs    = {}

        async for event in self.executor_runner.run_async(
            user_id=user_id,
            session_id=executor_session_id,
            new_message=message,
        ):
            if event.content and event.content.parts:
                for part in event.content.parts:
                    if part.text:
                        agent_text.append(part.text)
            if event.actions and event.actions.state_delta:
                outputs.update(event.actions.state_delta)

        return {"text": "\n".join(agent_text), "outputs": outputs}

    # ── Helpers ───────────────────────────────────────────────────────────────

    def get_selected_validation_type(self) -> Optional[str]:
        return self.selected_validation_type

    def get_selected_environment(self) -> Optional[str]:
        return self.selected_environment

    def get_env_config(self) -> Optional[Dict]:
        return self.env_config

    def _is_csv_content(self, text: str) -> bool:
        """Detect if message looks like CSV mapping file content."""
        return any(
            kw in text for kw in
            ("Primary key", "target_table", "source_table", "business_rules")
        ) or text.count(",") > 10

    def _get_pattern_for_validation_type(self, validation_type: str) -> str:
        return SQL_PATTERNS.get(validation_type) or SQL_PATTERNS.get("4", "")

    def _chunk_test_cases(
        self, test_cases: List[Dict], batch_size: int
    ) -> List[List[Dict]]:
        return [
            test_cases[i: i + batch_size]
            for i in range(0, len(test_cases), batch_size)
        ]

    def _validate_pk_usage_in_sql(
        self, test_case: Dict, sql_query: str
    ) -> Tuple[bool, str]:
        """Validate all PK columns appear in SQL JOIN conditions."""
        try:
            source_pk = test_case.get("source_primary_key", "")
            target_pk = test_case.get("target_primary_key", "")

            if not source_pk or not target_pk:
                return True, "No primary key validation needed"

            source_cols = [c.strip() for c in source_pk.split("+")]
            target_cols = [c.strip() for c in target_pk.split("+")]

            if len(source_cols) != len(target_cols):
                return False, (
                    f"PK column count mismatch: "
                    f"source={len(source_cols)}, target={len(target_cols)}"
                )

            missing = []
            for src, tgt in zip(source_cols, target_cols):
                src_pat = f'"{re.escape(src)}"' if " " in src else re.escape(src)
                if not re.search(
                    rf's\.{src_pat}\s*=\s*t\.{re.escape(tgt)}',
                    sql_query, re.IGNORECASE
                ):
                    missing.append(f"{src}->{tgt}")

            if missing:
                return False, f"Missing PK columns in JOIN: {', '.join(missing)}"

            return True, f"All {len(source_cols)} PK columns found in JOIN"

        except Exception as e:
            return False, f"PK validation error: {str(e)}"

    def _repair_json_structure(self, raw: str) -> str:
        """Repair common LLM JSON output issues."""
        if not raw:
            return '{"test_cases": []}'

        cleaned = self._clean_json_markdown(raw)

        try:
            json.loads(cleaned)
            return cleaned
        except json.JSONDecodeError:
            pass

        stripped = cleaned.strip()
        if (stripped.startswith("[") and stripped.endswith("]")
                and not stripped.startswith('{"test_cases"')):
            return f'{{"test_cases": {stripped}}}'

        match = re.search(r'\{.*"test_cases".*\}', stripped, re.DOTALL)
        if match:
            return match.group(0)

        return '{"test_cases": []}'

    def _clean_json_markdown(self, text: str) -> str:
        """Strip markdown fences and fix common escape issues."""
        text = re.sub(r'```json\s*', '', text)
        text = re.sub(r'```\s*',     '', text)
        text = re.sub(r'\\t',        ' ', text)
        text = re.sub(r'\\n',        ' ', text)
        text = re.sub(r'(\\){2,}(["\\/])', r'\2', text)
        return text.strip()

    def _clean_sql_output(self, raw_json_str: str) -> str:
        """Strip backticks and stray whitespace from sql_query fields."""
        try:
            data = json.loads(raw_json_str)
            for tc in data.get("test_cases", []):
                if tc.get("sql_query"):
                    tc["sql_query"] = tc["sql_query"].strip()
            return json.dumps(data, indent=2)
        except json.JSONDecodeError:
            return raw_json_str
