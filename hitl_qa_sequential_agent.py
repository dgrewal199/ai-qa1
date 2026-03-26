"""
Human-in-the-Loop Sequential QA Agent
7-step workflow:
  1. File Upload
  2. Validation Selection
  3. Generate Test Cases          (requirements_parser / dynamic_agent)
  4. Test Case Review             (HITL)
  5. Generate SQL                 (sql_generator / dynamic_sql_agent)
  5b. Supervisor Critique + Fix   (supervisor / dynamic_supervisor_agent)
  6. SQL Review Loop              (HITL — sees original + fixed SQL + critique report)
"""

import os
import json
import re
from typing import Optional

from google.adk.runners import InMemoryRunner
from google.genai import types
from google.adk.agents import Agent

# Absolute imports
from requirements_parser.agent import dynamic_agent
from sql_generator.agent import root_agent as _base_sql_agent
from sql_generator.patterns import (
    DDL_VALIDATION_PATTERN,
    COMPLETENESS_PATTERN,
    UNIQUENESS_PATTERN,
    COLUMN_TRANSFORMATION_PATTERN,
)
from supervisor.agent import dynamic_supervisor_agent

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
INPUT_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "input")

# ---------------------------------------------------------------------------
# DynamicSQLAgent
# ---------------------------------------------------------------------------
_SQL_PATTERNS = {
    "ddl_validation":        DDL_VALIDATION_PATTERN,
    "completeness":          COMPLETENESS_PATTERN,
    "uniqueness":            UNIQUENESS_PATTERN,
    "column_transformation": COLUMN_TRANSFORMATION_PATTERN,
}
_SQL_BASE_INSTRUCTION = _base_sql_agent.instruction


class DynamicSQLAgent:
    def __init__(self):
        self.current_validation_type = "column_transformation"
        self.agent = self._build_agent("column_transformation")

    def _build_agent(self, validation_type: str) -> Agent:
        pattern = _SQL_PATTERNS.get(validation_type, COLUMN_TRANSFORMATION_PATTERN)
        if validation_type == "all":
            pattern = "\n\n".join(_SQL_PATTERNS.values())
        return Agent(
            name=_base_sql_agent.name,
            model=_base_sql_agent.model,
            description=_base_sql_agent.description,
            output_key=_base_sql_agent.output_key,
            after_model_callback=_base_sql_agent.after_model_callback,
            tools=_base_sql_agent.tools,
            instruction=f"{_SQL_BASE_INSTRUCTION}\n\n{pattern}",
        )

    def update_for_validation_type(self, validation_type: str):
        if validation_type != self.current_validation_type:
            self.agent = self._build_agent(validation_type)
            self.current_validation_type = validation_type
            print(f"[DEBUG] DynamicSQLAgent updated for: {validation_type}")


dynamic_sql_agent = DynamicSQLAgent()

# ---------------------------------------------------------------------------
# Global state
# ---------------------------------------------------------------------------
_uploaded_file_data: dict = {}

# Persistent SQL agent session — kept alive across the review loop
_sql_runner:     Optional[InMemoryRunner] = None
_sql_session_id: Optional[str]           = None
_sql_user_id:    str                     = "hitl_sql_user"


def _store_file_content(filename: str, content: str) -> None:
    global _uploaded_file_data
    validation_type = _uploaded_file_data.get("validation_type")
    _uploaded_file_data = {"filename": filename, "content": content}
    if validation_type:
        _uploaded_file_data["validation_type"] = validation_type


def _get_file_content() -> str:
    return _uploaded_file_data.get("content", "")


# ---------------------------------------------------------------------------
# Step 1 — File Upload
# ---------------------------------------------------------------------------

def _request_file_upload() -> str:
    """Prompt the user to provide their mapping filename."""
    return (
        "Please provide your mapping filename from the input/ folder "
        "(e.g. 'balances.csv' or 'mapping.xlsx')."
    )


def _process_file_upload(file_data: str) -> str:
    """Read the actual file from input/ folder. Pass user's exact message text."""
    print(f"[DEBUG] _process_file_upload called with: {repr(file_data)}")

    match = re.search(r'[\w\-. ]+\.(csv|xlsx|parquet|json)', file_data, re.IGNORECASE)
    if not match:
        return "Error: No valid filename found. Please provide a filename like 'mapping.csv'."

    filename = match.group(0).strip()
    file_path = os.path.join(INPUT_FOLDER, filename)

    if not os.path.exists(file_path):
        try:
            available = [
                f for f in os.listdir(INPUT_FOLDER)
                if f.endswith(('.csv', '.xlsx', '.parquet', '.json'))
            ]
            files_list = ', '.join(available) if available else 'none found'
        except FileNotFoundError:
            files_list = f"input/ folder not found at {INPUT_FOLDER}"
        return (
            f"Error: '{filename}' not found in input/. "
            f"Available files: {files_list}"
        )

    try:
        if filename.lower().endswith(('.csv', '.txt', '.json')):
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
        elif filename.lower().endswith('.xlsx'):
            try:
                import pandas as pd
                content = pd.read_excel(file_path).to_csv(index=False)
            except ImportError:
                return "Error: Run: pip install pandas openpyxl"
        elif filename.lower().endswith('.parquet'):
            try:
                import pandas as pd
                content = pd.read_parquet(file_path).to_csv(index=False)
            except ImportError:
                return "Error: Run: pip install pandas pyarrow"
        else:
            return f"Error: Unsupported format for '{filename}'."
    except Exception as e:
        return f"Error reading '{filename}': {str(e)}"

    if not content.strip():
        return f"Error: File '{filename}' is empty."

    _store_file_content(filename, content)
    print(f"[DEBUG] Loaded '{filename}'. First 200 chars:\n{content[:200]}")
    return f"Success: File '{filename}' uploaded and ready for processing."


# ---------------------------------------------------------------------------
# Step 2 — Validation Selection
# ---------------------------------------------------------------------------

def _request_validation_selection() -> str:
    """Show validation type menu."""
    return (
        "Please select validation type:\n"
        "1. ddl_validation          - DDL Validation (database schema)\n"
        "2. completeness            - Completeness Validation (null/row counts)\n"
        "3. uniqueness              - Uniqueness Validation (primary keys, duplicates)\n"
        "4. column_transformation   - Column Transformation (source-to-target mappings)\n"
        "5. all                     - All Validations"
    )


def _process_validation_selection(validation_choice: str) -> str:
    """
    Store validation type and prime ALL three agents with correct patterns.
    Pass user's exact text (e.g. '4' or 'column_transformation').
    """
    global _uploaded_file_data

    aliases = {
        "1": "ddl_validation", "2": "completeness",
        "3": "uniqueness",     "4": "column_transformation", "5": "all",
    }
    choice = aliases.get(validation_choice.strip(), validation_choice.strip())
    valid  = list(aliases.values())

    if choice not in valid:
        return (
            f"Error: Invalid choice '{validation_choice}'. "
            f"Please choose 1-5 or: {', '.join(valid)}"
        )

    _uploaded_file_data["validation_type"] = choice

    # Prime all three agents at once
    dynamic_agent.update_for_validation_type(choice)          # req parser
    dynamic_sql_agent.update_for_validation_type(choice)      # sql generator
    dynamic_supervisor_agent.update_for_validation_type(choice)  # supervisor

    print(f"[DEBUG] All 3 agents primed for: {choice}")
    return f"Success: Selected validation type '{choice}'. Generating test cases now..."


# ---------------------------------------------------------------------------
# Step 3 — Generate Test Cases
# ---------------------------------------------------------------------------

async def _generate_test_cases_from_file() -> str:
    """
    Generate QA test cases from the uploaded mapping file.
    Sends real CSV to dynamic_agent which parses it using loaded few-shot examples.
    No arguments needed.
    """
    file_content  = _get_file_content()
    validation_type = _uploaded_file_data.get("validation_type", "column_transformation")
    filename      = _uploaded_file_data.get("filename", "unknown")

    if not file_content:
        return "Error: No mapping file uploaded yet."
    if not validation_type:
        return "Error: No validation type selected."

    print(f"[DEBUG] Generating '{validation_type}' test cases for '{filename}'")

    if dynamic_agent.current_validation_type != validation_type:
        dynamic_agent.update_for_validation_type(validation_type)

    runner     = InMemoryRunner(agent=dynamic_agent.agent)
    user_id    = "hitl_req_user"
    session_id = f"hitl_req_session_{validation_type}"

    try:
        await runner.session_service.create_session(
            app_name=runner.app_name, user_id=user_id, session_id=session_id,
        )
    except Exception:
        pass

    agent_text: list[str] = []
    outputs:    dict      = {}

    async for event in runner.run_async(
        user_id=user_id, session_id=session_id,
        new_message=types.Content(parts=[types.Part(text=file_content)]),
    ):
        if event.content and event.content.parts:
            for part in event.content.parts:
                if part.text:
                    agent_text.append(part.text)
        if event.actions and event.actions.state_delta:
            outputs.update(event.actions.state_delta)

    requirements_json = outputs.get("requirements_json") or "\n".join(agent_text)
    if not requirements_json:
        return "Error: No test cases generated. Check your mapping file format."

    print(f"[DEBUG] Test cases (first 300):\n{str(requirements_json)[:300]}")
    _uploaded_file_data["requirements_json"] = requirements_json
    return str(requirements_json)


# ---------------------------------------------------------------------------
# Step 4 — Test Case Review (HITL)
# ---------------------------------------------------------------------------

def _confirm_test_cases(test_cases_json: str) -> str:
    """Present generated test cases for human review."""
    return (
        f"Please review the generated test cases:\n\n{test_cases_json}\n\n"
        "Choose an action:\n"
        "1. 'approve' - Approve and proceed to SQL generation\n"
        "2. 'reject'  - Reject and go back to validation selection\n"
        "3. 'modify'  - Provide your modified test cases JSON"
    )


def _process_test_cases_confirmation(confirmation_data: str) -> str:
    """Process user's test case review decision."""
    action   = ""
    modified = ""

    try:
        decision = json.loads(confirmation_data)
        action   = decision.get("action", "").lower()
        modified = decision.get("modified_content", "")
    except (json.JSONDecodeError, ValueError):
        action = confirmation_data.lower().strip()

    if "approve" in action:
        return "Success: Test cases approved. Proceeding to SQL generation."
    elif "modify" in action:
        if modified:
            _uploaded_file_data["requirements_json"] = modified
            return "Success: Test cases modified. Proceeding to SQL generation."
        return "Error: Modification requested but no modified_content provided."
    elif "reject" in action:
        return "Rejected: Please go back to step 2 and select a validation type again."
    else:
        return f"Error: Unrecognised response '{confirmation_data}'. Reply with 'approve', 'reject', or 'modify'."


# ---------------------------------------------------------------------------
# Step 5 — Generate SQL
# ---------------------------------------------------------------------------

async def _generate_sql_from_approved_test_cases() -> str:
    """
    Generate BigQuery SQL from approved test cases using DynamicSQLAgent
    (correct validation-type pattern already injected in step 2).
    Creates a persistent session reused throughout the SQL review loop.
    No arguments needed.
    """
    global _sql_runner, _sql_session_id

    requirements_json = _uploaded_file_data.get("requirements_json")
    validation_type   = _uploaded_file_data.get("validation_type", "column_transformation")

    if not requirements_json:
        return "Error: No approved test cases found."

    if dynamic_sql_agent.current_validation_type != validation_type:
        dynamic_sql_agent.update_for_validation_type(validation_type)

    print(f"[DEBUG] Generating SQL for '{validation_type}'")

    # Persistent runner — kept alive for the review loop
    _sql_runner    = InMemoryRunner(agent=dynamic_sql_agent.agent)
    _sql_session_id = f"hitl_sql_session_{validation_type}"

    try:
        await _sql_runner.session_service.create_session(
            app_name=_sql_runner.app_name,
            user_id=_sql_user_id,
            session_id=_sql_session_id,
        )
    except Exception:
        pass

    agent_text: list[str] = []
    outputs:    dict      = {}

    async for event in _sql_runner.run_async(
        user_id=_sql_user_id,
        session_id=_sql_session_id,
        new_message=types.Content(parts=[types.Part(text=str(requirements_json))]),
    ):
        if event.content and event.content.parts:
            for part in event.content.parts:
                if part.text:
                    agent_text.append(part.text)
        if event.actions and event.actions.state_delta:
            outputs.update(event.actions.state_delta)

    result = outputs.get("sql_generation_status") or "\n".join(agent_text)
    if not result:
        return "Error: No SQL generated. Check your test cases format."

    _uploaded_file_data["generated_sql"] = result
    return result


# ---------------------------------------------------------------------------
# Step 5b — Supervisor Critique + Auto-Fix
# ---------------------------------------------------------------------------

async def _run_supervisor_critique() -> str:
    """
    Run the supervisor agent against the generated SQL.
    Inputs: mapping CSV (source of truth) + test cases + generated SQL
    Outputs: structured JSON critique with original + fixed SQL side by side.
    No arguments needed — reads everything from state.
    """
    mapping_csv       = _get_file_content()
    validation_type   = _uploaded_file_data.get("validation_type", "column_transformation")
    test_cases        = _uploaded_file_data.get("requirements_json", "")
    generated_sql     = _uploaded_file_data.get("generated_sql", "")

    if not generated_sql:
        return "Error: No generated SQL found. Please run SQL generation first."
    if not mapping_csv:
        return "Error: No mapping file content found."

    if dynamic_supervisor_agent.current_validation_type != validation_type:
        dynamic_supervisor_agent.update_for_validation_type(validation_type)

    print(f"[DEBUG] Running supervisor critique for '{validation_type}'")

    # Build supervisor input payload
    supervisor_input = json.dumps({
        "mapping_csv":    mapping_csv,
        "validation_type": validation_type,
        "test_cases":     test_cases,
        "generated_sql":  generated_sql,
    }, indent=2)

    runner     = InMemoryRunner(agent=dynamic_supervisor_agent.agent)
    user_id    = "hitl_supervisor_user"
    session_id = f"hitl_supervisor_session_{validation_type}"

    try:
        await runner.session_service.create_session(
            app_name=runner.app_name, user_id=user_id, session_id=session_id,
        )
    except Exception:
        pass

    agent_text: list[str] = []
    outputs:    dict      = {}

    async for event in runner.run_async(
        user_id=user_id,
        session_id=session_id,
        new_message=types.Content(parts=[types.Part(text=supervisor_input)]),
    ):
        if event.content and event.content.parts:
            for part in event.content.parts:
                if part.text:
                    agent_text.append(part.text)
        if event.actions and event.actions.state_delta:
            outputs.update(event.actions.state_delta)

    raw = outputs.get("supervisor_result") or "\n".join(agent_text)

    # Parse and store supervisor result
    try:
        # Strip markdown fences if model wrapped output despite instructions
        clean = re.sub(r'```(?:json)?|```', '', raw).strip()
        critique = json.loads(clean)
        _uploaded_file_data["supervisor_critique"] = critique

        # Store fixed SQL as the candidate for human review
        if critique.get("sql_fixed"):
            _uploaded_file_data["sql_fixed"] = critique["sql_fixed"]

        print(f"[DEBUG] Supervisor status: {critique.get('overall_status')} | "
              f"score: {critique.get('confidence_score')} | "
              f"{critique.get('fix_summary')}")

        return _format_supervisor_report(critique)

    except (json.JSONDecodeError, ValueError) as e:
        print(f"[DEBUG] Supervisor JSON parse error: {e}\nRaw: {raw[:300]}")
        # Return raw output so HITL can still review
        return f"Supervisor completed (raw output):\n{raw}"


def _format_supervisor_report(critique: dict) -> str:
    """
    Format the supervisor JSON into a readable report for the human reviewer.
    Shows: status, score, issues grouped by severity, fix summary, manual review notes.
    """
    status  = critique.get("overall_status", "unknown").upper()
    score   = critique.get("confidence_score", "N/A")
    summary = critique.get("fix_summary", "")
    notes   = critique.get("manual_review_notes", "")
    issues  = critique.get("issues", [])

    # Group issues by severity
    critical = [i for i in issues if i.get("severity") == "critical"]
    warnings = [i for i in issues if i.get("severity") == "warning"]
    infos    = [i for i in issues if i.get("severity") == "info"]

    lines = [
        f"╔══════════════════════════════════════╗",
        f"  SUPERVISOR REPORT — {status}",
        f"  Confidence Score: {score}/100",
        f"  {summary}",
        f"╚══════════════════════════════════════╝",
    ]

    if critical:
        lines.append(f"\n🔴 CRITICAL ISSUES ({len(critical)}):")
        for i in critical:
            lines.append(f"  [{i.get('test_case_id','?')}] {i.get('check_category','')}")
            lines.append(f"    Finding:  {i.get('finding','')}")
            if i.get("fix_status") == "auto_fixed":
                lines.append(f"    ✅ Fixed:  {i.get('fix_description','')}")
            else:
                lines.append(f"    ❌ Unable to fix: {i.get('fix_blocked_reason','')}")

    if warnings:
        lines.append(f"\n🟡 WARNINGS ({len(warnings)}):")
        for i in warnings:
            lines.append(f"  [{i.get('test_case_id','?')}] {i.get('finding','')}")
            if i.get("fix_status") == "auto_fixed":
                lines.append(f"    ✅ Fixed: {i.get('fix_description','')}")

    if infos:
        lines.append(f"\nℹ️  INFO ({len(infos)}):")
        for i in infos:
            lines.append(f"  [{i.get('test_case_id','?')}] {i.get('finding','')}")

    if notes:
        lines.append(f"\n📋 MANUAL REVIEW NOTES:\n  {notes}")

    lines.append("\nBoth original and fixed SQL are available for your review below.")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Step 6 — SQL Review Loop (HITL)
# Shows supervisor report + original + fixed SQL
# ---------------------------------------------------------------------------

def _confirm_sql_with_critique(supervisor_report: str) -> str:
    """
    Present the supervisor critique report + both SQL versions for human review.
    """
    critique    = _uploaded_file_data.get("supervisor_critique", {})
    sql_original = critique.get("sql_original", _uploaded_file_data.get("generated_sql", ""))
    sql_fixed    = critique.get("sql_fixed", {})
    overall      = critique.get("overall_status", "unknown")

    # Format SQL sections
    original_section = (
        json.dumps(sql_original, indent=2)
        if isinstance(sql_original, dict)
        else str(sql_original)
    )
    fixed_section = (
        json.dumps(sql_fixed, indent=2)
        if isinstance(sql_fixed, dict)
        else str(sql_fixed)
    )

    status_note = {
        "pass":                    "✅ No issues found — SQL looks good.",
        "fixed":                   "✅ All issues were auto-fixed.",
        "manual_review_required":  "⚠️  Some issues could not be auto-fixed — please review carefully.",
    }.get(overall, "")

    return (
        f"{supervisor_report}\n\n"
        f"{status_note}\n\n"
        f"{'─'*50}\n"
        f"ORIGINAL SQL:\n{original_section}\n\n"
        f"{'─'*50}\n"
        f"FIXED SQL (supervisor auto-fixes applied):\n{fixed_section}\n\n"
        f"{'─'*50}\n"
        "Choose an action:\n"
        "1. 'approve fixed'    - Approve the supervisor-fixed SQL\n"
        "2. 'approve original' - Approve the original SQL as-is\n"
        "3. 'reject'           - Reject and regenerate SQL from scratch\n"
        "4. Ask any question   - Ask the SQL agent anything about the generated SQL\n"
        "                        (e.g. 'Why did you use SAFE_CAST here?', 'Fix test case 3')\n"
        "                        The SQL agent remembers the full context."
    )


async def _process_sql_review(user_input: str) -> str:
    """
    Handle the SQL review loop.

    Routes:
    - 'approve fixed'    → store fixed SQL, workflow complete
    - 'approve original' → store original SQL, workflow complete
    - 'reject'           → clear session, trigger fresh SQL generation
    - anything else      → send to SQL agent on persistent session (follow-up Q&A / modifications)

    Pass user's exact text as user_input.
    """
    global _sql_runner, _sql_session_id

    action = user_input.lower().strip()

    # ── Approve fixed ─────────────────────────────────────────────────────
    if "approve" in action and "fixed" in action:
        fixed = _uploaded_file_data.get("sql_fixed", {})
        _uploaded_file_data["approved_sql"] = fixed
        return "Success: Supervisor-fixed SQL approved. Workflow complete! ✓"

    # ── Approve original ──────────────────────────────────────────────────
    if "approve" in action and "original" in action:
        original = _uploaded_file_data.get("generated_sql", "")
        _uploaded_file_data["approved_sql"] = original
        return "Success: Original SQL approved. Workflow complete! ✓"

    # Shorthand 'approve' with no qualifier — default to fixed if available
    if action == "approve" or action == "1":
        fixed = _uploaded_file_data.get("sql_fixed")
        approved = fixed if fixed else _uploaded_file_data.get("generated_sql", "")
        _uploaded_file_data["approved_sql"] = approved
        return "Success: SQL approved. Workflow complete! ✓"

    # ── Reject — reset session, trigger fresh generation ──────────────────
    if action in ("reject", "2"):
        _sql_runner     = None
        _sql_session_id = None
        _uploaded_file_data.pop("generated_sql", None)
        _uploaded_file_data.pop("sql_fixed", None)
        _uploaded_file_data.pop("supervisor_critique", None)
        return "Rejected: Regenerating SQL from test cases. Please wait..."

    # ── Follow-up question or modification — send to SQL agent ────────────
    if not _sql_runner or not _sql_session_id:
        return "Error: No active SQL session. Please regenerate SQL first."

    # Strip 'modify:' prefix if present
    message_text = re.sub(
        r'^(modify\s*[:\-]?\s*)', '', user_input, flags=re.IGNORECASE
    ).strip() or user_input

    print(f"[DEBUG] SQL follow-up on session '{_sql_session_id}': {message_text[:100]}")

    agent_text: list[str] = []
    outputs:    dict      = {}

    async for event in _sql_runner.run_async(
        user_id=_sql_user_id,
        session_id=_sql_session_id,
        new_message=types.Content(parts=[types.Part(text=message_text)]),
    ):
        if event.content and event.content.parts:
            for part in event.content.parts:
                if part.text:
                    agent_text.append(part.text)
        if event.actions and event.actions.state_delta:
            outputs.update(event.actions.state_delta)

    response = outputs.get("sql_generation_status") or "\n".join(agent_text)
    if not response:
        return "Error: No response from SQL agent."

    # Update stored SQL with latest agent output
    _uploaded_file_data["generated_sql"] = response

    return (
        f"{response}\n\n"
        "─────────────────────────────────────────\n"
        "You can continue asking questions, or:\n"
        "  'approve fixed'    — approve supervisor-fixed version\n"
        "  'approve original' — approve this latest version\n"
        "  'reject'           — regenerate from scratch"
    )


# ---------------------------------------------------------------------------
# Root Sequential Agent
# ---------------------------------------------------------------------------

class HITLQASequentialAgent:

    def __init__(self, name: str = "HITLQASequentialAgent"):
        self.name   = name
        self._agent: Optional[Agent] = None

    def _ensure_agent(self) -> Agent:
        if self._agent is None:
            self._agent = Agent(
                name=self.name,
                model="gemini-2.5-flash",
                description=(
                    "7-step HITL QA workflow with supervisor critique: "
                    "file upload → validation selection → test case generation → "
                    "test case review → SQL generation → supervisor critique → SQL review loop"
                ),
                instruction="""
You are a sequential workflow orchestrator managing a 7-step Human-in-the-Loop QA process.

STRICT RULES:
- Follow steps IN ORDER. Never skip or reorder.
- NEVER generate test cases or SQL yourself — always call the designated tool.
- Present ALL tool outputs verbatim without modification.
- Wait for user input ONLY at steps 1, 2, 4, and 6.
- Between steps 2→3, 3→4, 4b→5, 5→5b, 5b→6: proceed immediately without waiting.

WORKFLOW:

STEP 1 — File Upload:
  - Call _request_file_upload.
  - When user provides filename, call _process_file_upload(file_data="<their exact text>").
  - Proceed only when result starts with "Success:".

STEP 2 — Validation Selection:
  - Call _request_validation_selection.
  - When user picks option, call _process_validation_selection(validation_choice="<their input>").
  - When result starts with "Success:", IMMEDIATELY call _generate_test_cases_from_file.

STEP 3 — Generate Test Cases (no user wait):
  - Call _generate_test_cases_from_file() — no arguments.
  - IMMEDIATELY pass exact output to _confirm_test_cases.

STEP 4 — Test Case Review (HITL):
  - Call _confirm_test_cases(test_cases_json="<step 3 output>").
  - Wait for user: 'approve', 'reject', or 'modify'.
  - Call _process_test_cases_confirmation(confirmation_data="<user response>").
  - If "Success:" → IMMEDIATELY call _generate_sql_from_approved_test_cases.
  - If "Rejected:" → return to step 2.

STEP 5 — Generate SQL (no user wait):
  - Call _generate_sql_from_approved_test_cases() — no arguments.
  - When complete, IMMEDIATELY call _run_supervisor_critique — do NOT show SQL to user yet.

STEP 5b — Supervisor Critique (no user wait):
  - Call _run_supervisor_critique() — no arguments.
  - When complete, IMMEDIATELY pass its output to _confirm_sql_with_critique.

STEP 6 — SQL Review Loop (HITL):
  - Call _confirm_sql_with_critique(supervisor_report="<step 5b output>").
  - Show full output to user and wait for their response.
  - Call _process_sql_review(user_input="<exact user text>") for EVERY response.
  - If result contains "Workflow complete" → congratulate user, workflow done.
  - If result contains "Regenerating SQL" → go back to step 5 (call _generate_sql_from_approved_test_cases).
  - Otherwise → show response, call _confirm_sql_with_critique again to continue the loop.
  - STAY IN THIS LOOP until user approves or rejects.
""",
                tools=[
                    _request_file_upload,
                    _process_file_upload,
                    _request_validation_selection,
                    _process_validation_selection,
                    _generate_test_cases_from_file,
                    _confirm_test_cases,
                    _process_test_cases_confirmation,
                    _generate_sql_from_approved_test_cases,
                    _run_supervisor_critique,
                    _confirm_sql_with_critique,
                    _process_sql_review,
                ],
                output_key="workflow_results",
            )
        return self._agent

    def ensure_agent(self) -> Agent:
        return self._ensure_agent()


# Singleton export
_hitl_agent = HITLQASequentialAgent()
root_agent  = _hitl_agent.ensure_agent()
