"""
Human-in-the-Loop Sequential QA Agent
5-step workflow: File Upload -> Validation Selection -> Generate Test Cases
               -> Human Review (test cases) -> Generate SQL
               -> SQL Review Loop (approve/reject/modify + follow-up questions)

KEY FIXES IN THIS VERSION:
1. SQL agent now receives validation-type-specific pattern dynamically (fixes divergence)
2. DynamicSQLAgent wrapper mirrors DynamicRequirementsAgent pattern
3. SQL review is a persistent-session loop — agent remembers generated SQL across follow-ups
4. Full approve/reject/modify + free-form Q&A loop with SQL agent
5. Absolute imports throughout
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

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
INPUT_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "input")

# ---------------------------------------------------------------------------
# DynamicSQLAgent — mirrors DynamicRequirementsAgent pattern
# Swaps validation-type-specific few-shot pattern into instruction before use
# ---------------------------------------------------------------------------

# Map validation type -> pattern constant from patterns.py
_SQL_PATTERNS = {
    "ddl_validation":        DDL_VALIDATION_PATTERN,
    "completeness":          COMPLETENESS_PATTERN,
    "uniqueness":            UNIQUENESS_PATTERN,
    "column_transformation": COLUMN_TRANSFORMATION_PATTERN,
}

# Base instruction shared across all validation types — pulled from root_agent
_SQL_BASE_INSTRUCTION = _base_sql_agent.instruction


class DynamicSQLAgent:
    """
    Wraps the SQL generator agent and dynamically injects the correct
    validation-type pattern into its instruction before each run.
    Mirrors the DynamicRequirementsAgent pattern exactly.
    """

    def __init__(self):
        # Start with a copy of the base agent using default (column_transformation)
        self.current_validation_type = "column_transformation"
        self.agent = self._build_agent("column_transformation")

    def _build_agent(self, validation_type: str) -> Agent:
        pattern = _SQL_PATTERNS.get(validation_type, COLUMN_TRANSFORMATION_PATTERN)
        instruction = f"{_SQL_BASE_INSTRUCTION}\n\n{pattern}"
        return Agent(
            name=_base_sql_agent.name,
            model=_base_sql_agent.model,
            description=_base_sql_agent.description,
            output_key=_base_sql_agent.output_key,        # "sql_generation_status"
            after_model_callback=_base_sql_agent.after_model_callback,
            tools=_base_sql_agent.tools,
            instruction=instruction,
        )

    def update_for_validation_type(self, validation_type: str):
        """Inject the correct pattern for this validation type — call before running."""
        if validation_type == "all":
            # Combine all patterns for 'all' validation
            combined = "\n\n".join(_SQL_PATTERNS.values())
            validation_type_key = "all"
        else:
            combined = None
            validation_type_key = validation_type

        if validation_type_key != self.current_validation_type:
            if combined:
                instruction = f"{_SQL_BASE_INSTRUCTION}\n\n{combined}"
                self.agent = Agent(
                    name=_base_sql_agent.name,
                    model=_base_sql_agent.model,
                    description=_base_sql_agent.description,
                    output_key=_base_sql_agent.output_key,
                    after_model_callback=_base_sql_agent.after_model_callback,
                    tools=_base_sql_agent.tools,
                    instruction=instruction,
                )
            else:
                self.agent = self._build_agent(validation_type_key)
            self.current_validation_type = validation_type_key
            print(f"[DEBUG] DynamicSQLAgent updated for: {validation_type_key}")


# Singleton SQL agent wrapper
dynamic_sql_agent = DynamicSQLAgent()

# ---------------------------------------------------------------------------
# Global state
# ---------------------------------------------------------------------------
_uploaded_file_data: dict = {}

# Persistent SQL runner + session — kept alive for review loop follow-ups
_sql_runner: Optional[InMemoryRunner] = None
_sql_session_id: Optional[str] = None
_sql_user_id: str = "hitl_sql_user"


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
    """
    Read the actual file from input/ folder.
    Pass the user's exact message text as file_data.
    """
    print(f"[DEBUG] _process_file_upload called with: {repr(file_data)}")

    match = re.search(r'[\w\-. ]+\.(csv|xlsx|parquet|json)', file_data, re.IGNORECASE)
    if not match:
        return (
            "Error: No valid filename found. "
            "Please provide a filename like 'mapping.csv' or 'balances.xlsx'."
        )

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
            f"Error: File '{filename}' not found in input/ folder. "
            f"Available files: {files_list}"
        )

    try:
        if filename.lower().endswith(('.csv', '.txt', '.json')):
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
        elif filename.lower().endswith('.xlsx'):
            try:
                import pandas as pd
                df = pd.read_excel(file_path)
                content = df.to_csv(index=False)
            except ImportError:
                return "Error: pandas required. Run: pip install pandas openpyxl"
        elif filename.lower().endswith('.parquet'):
            try:
                import pandas as pd
                df = pd.read_parquet(file_path)
                content = df.to_csv(index=False)
            except ImportError:
                return "Error: pandas required. Run: pip install pandas pyarrow"
        else:
            return f"Error: Unsupported format for '{filename}'."
    except Exception as e:
        return f"Error reading '{filename}': {str(e)}"

    if not content.strip():
        return f"Error: File '{filename}' is empty."

    _store_file_content(filename, content)
    print(f"[DEBUG] Loaded '{filename}'. First 200 chars:\n{content[:200]}")
    return f"Success: File '{filename}' uploaded successfully and ready for processing."


# ---------------------------------------------------------------------------
# Step 2 — Validation Selection
# ---------------------------------------------------------------------------

def _request_validation_selection() -> str:
    """Show validation type menu."""
    return (
        "Please select validation type:\n"
        "1. ddl_validation          - DDL Validation (database schema validation)\n"
        "2. completeness            - Completeness Validation (data completeness checks)\n"
        "3. uniqueness              - Uniqueness Validation (primary keys, no duplicates/nulls)\n"
        "4. column_transformation   - Column Transformation Validation (source-to-target mappings)\n"
        "5. all                     - All Validations"
    )


def _process_validation_selection(validation_choice: str) -> str:
    """
    Store validation type and update BOTH agents with correct patterns.
    Pass the user's exact text (e.g. '4' or 'column_transformation').
    """
    global _uploaded_file_data

    aliases = {
        "1": "ddl_validation",
        "2": "completeness",
        "3": "uniqueness",
        "4": "column_transformation",
        "5": "all",
    }
    choice = aliases.get(validation_choice.strip(), validation_choice.strip())
    valid = list(aliases.values())

    if choice not in valid:
        return (
            f"Error: Invalid choice '{validation_choice}'. "
            f"Please choose 1-5 or one of: {', '.join(valid)}"
        )

    _uploaded_file_data["validation_type"] = choice

    # Update requirements parser agent with correct few-shot examples
    dynamic_agent.update_for_validation_type(choice)

    # Update SQL agent with correct pattern NOW — ready for step 5
    dynamic_sql_agent.update_for_validation_type(choice)

    print(f"[DEBUG] Both agents updated for validation type: {choice}")
    return f"Success: Selected validation type '{choice}'. Generating test cases now..."


# ---------------------------------------------------------------------------
# Step 3 — Generate Test Cases
# ---------------------------------------------------------------------------

async def _generate_test_cases_from_file() -> str:
    """
    Generate QA test cases from the uploaded mapping file.
    Sends real CSV content to dynamic_agent which parses it using its
    loaded few-shot examples. No arguments needed.
    """
    file_content = _get_file_content()
    validation_type = _uploaded_file_data.get("validation_type", "column_transformation")
    filename = _uploaded_file_data.get("filename", "unknown")

    if not file_content:
        return "Error: No mapping file uploaded yet. Please upload a file first."
    if not validation_type:
        return "Error: No validation type selected. Please select a validation type first."

    print(f"[DEBUG] Generating '{validation_type}' test cases for '{filename}'")

    # Safety net — re-apply if session was restarted
    if dynamic_agent.current_validation_type != validation_type:
        dynamic_agent.update_for_validation_type(validation_type)

    runner = InMemoryRunner(agent=dynamic_agent.agent)
    user_id = "hitl_req_user"
    session_id = f"hitl_req_session_{validation_type}"

    try:
        await runner.session_service.create_session(
            app_name=runner.app_name,
            user_id=user_id,
            session_id=session_id,
        )
    except Exception:
        pass

    cnv_message = types.Content(parts=[types.Part(text=file_content)])
    agent_text: list[str] = []
    outputs: dict = {}

    async for event in runner.run_async(
        user_id=user_id,
        session_id=session_id,
        new_message=cnv_message,
    ):
        if event.content and event.content.parts:
            for part in event.content.parts:
                if part.text:
                    agent_text.append(part.text)
        if event.actions and event.actions.state_delta:
            outputs.update(event.actions.state_delta)

    requirements_json = outputs.get("requirements_json") or "\n".join(agent_text)

    if not requirements_json:
        return "Error: No test cases generated. Please check your mapping file format."

    print(f"[DEBUG] Test cases captured (first 300):\n{str(requirements_json)[:300]}")
    _uploaded_file_data["requirements_json"] = requirements_json
    return str(requirements_json)


# ---------------------------------------------------------------------------
# Step 4 — Human Review of Test Cases
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
    """
    Process user's test case review decision.
    Accepts: 'approve', 'reject', or JSON {"action":"modify","modified_content":"..."}
    """
    action = ""
    modified = ""

    try:
        decision = json.loads(confirmation_data)
        action = decision.get("action", "").lower()
        modified = decision.get("modified_content", "")
    except (json.JSONDecodeError, ValueError):
        action = confirmation_data.lower().strip()

    if "approve" in action:
        return "Success: Test cases approved. Proceeding to SQL generation."
    elif "modify" in action:
        if modified:
            _uploaded_file_data["requirements_json"] = modified
            return "Success: Test cases modified and approved. Proceeding to SQL generation."
        return "Error: Modification requested but no modified_content provided."
    elif "reject" in action:
        return "Rejected: Please go back to step 2 and select a validation type again."
    else:
        return (
            f"Error: Unrecognised response '{confirmation_data}'. "
            "Please reply with 'approve', 'reject', or 'modify'."
        )


# ---------------------------------------------------------------------------
# Step 5 — Generate SQL
# Persistent session kept alive for the SQL review loop in step 6
# ---------------------------------------------------------------------------

async def _generate_sql_from_approved_test_cases() -> str:
    """
    Generate BigQuery SQL from approved test cases.
    Uses DynamicSQLAgent which has the correct validation-type pattern injected.
    Creates a persistent session reused throughout the SQL review loop.
    No arguments needed.
    """
    global _sql_runner, _sql_session_id

    requirements_json = _uploaded_file_data.get("requirements_json")
    validation_type = _uploaded_file_data.get("validation_type", "column_transformation")

    if not requirements_json:
        return "Error: No approved test cases found. Please complete test case review first."

    # Safety net — ensure SQL agent has correct pattern loaded
    if dynamic_sql_agent.current_validation_type != validation_type:
        dynamic_sql_agent.update_for_validation_type(validation_type)

    print(f"[DEBUG] Generating SQL for '{validation_type}'. Test cases (first 150):\n{str(requirements_json)[:150]}")

    # Create persistent runner + session for the entire SQL review loop
    _sql_runner = InMemoryRunner(agent=dynamic_sql_agent.agent)
    _sql_session_id = f"hitl_sql_session_{validation_type}"

    try:
        await _sql_runner.session_service.create_session(
            app_name=_sql_runner.app_name,
            user_id=_sql_user_id,
            session_id=_sql_session_id,
        )
    except Exception:
        pass

    sql_message = types.Content(parts=[types.Part(text=str(requirements_json))])
    agent_text: list[str] = []
    outputs: dict = {}

    async for event in _sql_runner.run_async(
        user_id=_sql_user_id,
        session_id=_sql_session_id,
        new_message=sql_message,
    ):
        if event.content and event.content.parts:
            for part in event.content.parts:
                if part.text:
                    agent_text.append(part.text)
        if event.actions and event.actions.state_delta:
            outputs.update(event.actions.state_delta)

    result = outputs.get("sql_generation_status") or "\n".join(agent_text)

    if not result:
        return "Error: No SQL generated. Please check your test cases format."

    # Store generated SQL for review step
    _uploaded_file_data["generated_sql"] = result
    return result


# ---------------------------------------------------------------------------
# Step 6 — SQL Review Loop
# approve/reject/modify + free-form Q&A with SQL agent (same persistent session)
# ---------------------------------------------------------------------------

def _confirm_sql(generated_sql: str) -> str:
    """Present generated SQL for human review."""
    return (
        f"Please review the generated SQL:\n\n{generated_sql}\n\n"
        "Choose an action:\n"
        "1. 'approve'          - Approve SQL. Workflow complete.\n"
        "2. 'reject'           - Reject and regenerate SQL from test cases\n"
        "3. 'modify'           - Provide specific modification instructions\n"
        "4. Ask any question   - Ask the SQL agent anything (e.g. 'Why did you use SAFE_CAST here?')\n"
        "                        The agent remembers the SQL it generated."
    )


async def _process_sql_review(user_input: str) -> str:
    """
    Handle the SQL review loop.
    Routes to approve/reject/modify or passes free-form questions
    directly to the SQL agent on the same persistent session.

    Pass the user's exact text as user_input.
    """
    global _sql_runner, _sql_session_id

    action = user_input.lower().strip()

    # ── Approve ──────────────────────────────────────────────────────────
    if action == "approve" or action == "1":
        return "Success: SQL approved. Workflow complete! ✓"

    # ── Reject — regenerate SQL fresh ────────────────────────────────────
    if action == "reject" or action == "2":
        # Reset persistent session so next generation is clean
        _sql_runner = None
        _sql_session_id = None
        _uploaded_file_data.pop("generated_sql", None)
        return "Rejected: Regenerating SQL. Please wait..."

    # ── Modify or free-form question — send to SQL agent on same session ─
    # This covers:
    #   - "modify: add a null check for account_id"
    #   - "why did you use SAFE_CAST here?"
    #   - "can you add a comment explaining the JOIN?"
    #   - "3" (modify option)
    if not _sql_runner or not _sql_session_id:
        return (
            "Error: No active SQL session. "
            "Please go back and generate SQL first."
        )

    print(f"[DEBUG] Sending to SQL agent on session '{_sql_session_id}': {user_input[:100]}")

    # Strip "modify:" prefix if present for cleaner agent input
    message_text = re.sub(r'^(modify\s*[:\-]?\s*)', '', user_input, flags=re.IGNORECASE).strip()
    if not message_text:
        message_text = user_input

    follow_up = types.Content(parts=[types.Part(text=message_text)])
    agent_text: list[str] = []
    outputs: dict = {}

    async for event in _sql_runner.run_async(
        user_id=_sql_user_id,
        session_id=_sql_session_id,
        new_message=follow_up,
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

    # Update stored SQL if agent produced new output
    _uploaded_file_data["generated_sql"] = response

    return (
        f"{response}\n\n"
        "---\n"
        "You can continue asking questions, type 'approve' to finish, "
        "or 'reject' to regenerate."
    )


# ---------------------------------------------------------------------------
# Root Sequential Agent
# ---------------------------------------------------------------------------

class HITLQASequentialAgent:
    """
    HITL QA Sequential Agent.
    6-step workflow orchestrated by the root LLM agent via tool calls.
    """

    def __init__(self, name: str = "HITLQASequentialAgent"):
        self.name = name
        self._agent: Optional[Agent] = None

    def _ensure_agent(self) -> Agent:
        if self._agent is None:
            self._agent = Agent(
                name=self.name,
                model="gemini-2.5-flash",
                description=(
                    "6-step HITL QA workflow: file upload → validation selection → "
                    "test case generation → test case review → SQL generation → SQL review loop"
                ),
                instruction="""
You are a sequential workflow orchestrator managing a 6-step Human-in-the-Loop QA process.

STRICT RULES:
- Follow the steps IN ORDER. Do not skip or reorder steps.
- NEVER generate test cases or SQL yourself — always call the designated tool.
- Present ALL tool outputs verbatim to the user without modification.
- Wait for user input ONLY at steps 1, 2, 4, and 6.
- NEVER pause between steps 2→3, 3→4, or 5→6.

WORKFLOW:

STEP 1 — File Upload:
  - Call _request_file_upload to prompt the user.
  - When the user provides a filename, call _process_file_upload(file_data="<their exact text>").
  - Only proceed when result starts with "Success:".

STEP 2 — Validation Selection:
  - Call _request_validation_selection to show the menu.
  - When user picks an option, call _process_validation_selection(validation_choice="<their input>").
  - When result starts with "Success:", IMMEDIATELY call _generate_test_cases_from_file — do NOT wait.

STEP 3 — Generate Test Cases (no user wait):
  - Call _generate_test_cases_from_file() — no arguments.
  - IMMEDIATELY pass its exact output to _confirm_test_cases — do NOT wait for user.

STEP 4 — Test Case Review:
  - Call _confirm_test_cases(test_cases_json="<exact step 3 output>").
  - Show result and wait for user: 'approve', 'reject', or 'modify'.
  - Call _process_test_cases_confirmation(confirmation_data="<user response>").
  - If "Success:" → IMMEDIATELY call _generate_sql_from_approved_test_cases — do NOT wait.
  - If "Rejected:" → return to step 2.

STEP 5 — Generate SQL (no user wait):
  - Call _generate_sql_from_approved_test_cases() — no arguments.
  - IMMEDIATELY pass its exact output to _confirm_sql — do NOT wait for user.

STEP 6 — SQL Review Loop:
  - Call _confirm_sql(generated_sql="<exact step 5 output>").
  - Show result and wait for user input.
  - Call _process_sql_review(user_input="<exact user text>") for EVERY user response.
  - If result is "Success: SQL approved" → workflow complete, congratulate the user.
  - If result contains "Regenerating SQL" → call _generate_sql_from_approved_test_cases again (restart step 5).
  - Otherwise → show the response and call _confirm_sql again with the updated SQL to continue the loop.
  - STAY IN THIS LOOP until the user approves or rejects.
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
                    _confirm_sql,
                    _process_sql_review,
                ],
                output_key="workflow_results",
            )
        return self._agent

    def ensure_agent(self) -> Agent:
        return self._ensure_agent()


# Singleton export
_hitl_agent = HITLQASequentialAgent()
root_agent = _hitl_agent.ensure_agent()
