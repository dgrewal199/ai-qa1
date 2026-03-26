"""
Human-in-the-Loop Sequential QA Agent
5-step workflow: File Upload -> Validation Selection -> Generate Test Cases -> Human Review -> Generate SQL

FIX SUMMARY:
- _process_file_upload: reads REAL file from input/ folder, NO mock content
- _generate_test_cases_from_file: calls dynamic_agent.update_for_validation_type() FIRST,
  then sends the RAW CSV content as the message (agent parses it internally)
- Absolute imports to avoid "relative import beyond top-level package" error
- Removed mock_content entirely
"""

import os
import json
import asyncio
import re
from typing import Optional

from google.adk.runners import InMemoryRunner
from google.genai import types
from google.adk.agents import Agent

# ── Absolute imports (fixes "relative import beyond top-level package") ──────
from requirements_parser.agent import dynamic_agent
from sql_generator.agent import root_agent as sql_agent

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

# Folder where user CSV/XLSX files are stored — adjust if needed
INPUT_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "input")

# ---------------------------------------------------------------------------
# Global state
# ---------------------------------------------------------------------------
_uploaded_file_data: dict = {}


def _store_file_content(filename: str, content: str) -> None:
    global _uploaded_file_data
    validation_type = _uploaded_file_data.get("validation_type")
    _uploaded_file_data = {"filename": filename, "content": content}
    if validation_type:
        _uploaded_file_data["validation_type"] = validation_type


def _get_file_content() -> str:
    return _uploaded_file_data.get("content", "")


# ---------------------------------------------------------------------------
# Step 1 tools — File Upload
# ---------------------------------------------------------------------------

def _request_file_upload() -> str:
    """Prompt the user to provide their mapping file."""
    return (
        "Please provide your mapping filename from the input/ folder "
        "(e.g. 'balances.csv' or 'mapping.xlsx')."
    )


def _process_file_upload(file_data: str) -> str:
    """
    Read the actual CSV/XLSX file from the input/ folder.
    Pass the user's exact message text as file_data (e.g. 'balances.csv').
    """
    print(f"[DEBUG] _process_file_upload called with: {repr(file_data)}")

    # ── Extract filename from whatever the user typed ──────────────────────
    match = re.search(r'[\w\-. ]+\.(csv|xlsx|parquet|json)', file_data, re.IGNORECASE)
    if not match:
        return (
            "Error: No valid filename found. "
            "Please provide a filename like 'mapping.csv' or 'balances.xlsx'."
        )

    filename = match.group(0).strip()
    file_path = os.path.join(INPUT_FOLDER, filename)

    print(f"[DEBUG] Looking for file at: {file_path}")

    # ── Read the REAL file — no mock content ──────────────────────────────
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
                return "Error: pandas is required to read .xlsx files. Run: pip install pandas openpyxl"

        elif filename.lower().endswith('.parquet'):
            try:
                import pandas as pd
                df = pd.read_parquet(file_path)
                content = df.to_csv(index=False)
            except ImportError:
                return "Error: pandas is required to read .parquet files. Run: pip install pandas pyarrow"

        else:
            return f"Error: Unsupported file format for '{filename}'."

    except Exception as e:
        return f"Error reading file '{filename}': {str(e)}"

    if not content.strip():
        return f"Error: File '{filename}' is empty."

    _store_file_content(filename, content)
    print(f"[DEBUG] File loaded. First 200 chars:\n{content[:200]}")
    return f"Success: File '{filename}' uploaded successfully and ready for processing."


# ---------------------------------------------------------------------------
# Step 2 tools — Validation Selection
# ---------------------------------------------------------------------------

def _request_validation_selection() -> str:
    """Show validation type menu to user."""
    return (
        "Please select validation type for test case generation:\n"
        "1. ddl_validation          - DDL Validation (database schema validation)\n"
        "2. completeness            - Completeness Validation (data completeness checks)\n"
        "3. uniqueness              - Uniqueness Validation (primary keys, no duplicates/nulls)\n"
        "4. column_transformation   - Column Transformation Validation (source-to-target mappings)\n"
        "5. all                     - All Validations"
    )


def _process_validation_selection(validation_choice: str) -> str:
    """
    Store the user's validation type and update dynamic_agent instructions.
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

    # ── Update dynamic_agent NOW so correct few-shot examples are loaded ──
    # This calls get_instructions_with_patterns(validation_type) internally
    dynamic_agent.update_for_validation_type(choice)
    print(f"[DEBUG] dynamic_agent updated for validation type: {choice}")

    return f"Success: Selected validation type '{choice}'. Generating test cases now..."


# ---------------------------------------------------------------------------
# Step 3 tool — Generate Test Cases
# ---------------------------------------------------------------------------

async def _generate_test_cases_from_file() -> str:
    """
    Generate QA test cases from the uploaded mapping file.
    - Reads REAL CSV content from state (loaded in step 1)
    - dynamic_agent already has correct validation instructions (set in step 2)
    - Sends raw CSV as message; dynamic_agent parses it using prompts_integrated.py
    """
    file_content = _get_file_content()
    validation_type = _uploaded_file_data.get("validation_type", "column_transformation")
    filename = _uploaded_file_data.get("filename", "unknown")

    if not file_content:
        return "Error: No mapping file uploaded yet. Please upload a file first."
    if not validation_type:
        return "Error: No validation type selected. Please select a validation type first."

    print(f"[DEBUG] Generating '{validation_type}' test cases for '{filename}'")
    print(f"[DEBUG] CSV content (first 300 chars):\n{file_content[:300]}")

    # Safety net: re-apply validation type if session was restarted
    if dynamic_agent.current_validation_type != validation_type:
        dynamic_agent.update_for_validation_type(validation_type)
        print(f"[DEBUG] Re-applied validation type: {validation_type}")

    # ── Fresh InMemoryRunner session ──────────────────────────────────────
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
        pass  # Session already exists

    # ── Send REAL CSV content — dynamic_agent parses it internally ────────
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
        # Capture output_key="requirements_json" from state_delta
        if event.actions and event.actions.state_delta:
            outputs.update(event.actions.state_delta)

    # Prefer structured state_delta output, fall back to raw agent text
    requirements_json = outputs.get("requirements_json") or "\n".join(agent_text)

    if not requirements_json:
        return "Error: No test cases generated. Please check your mapping file format."

    print(f"[DEBUG] requirements_json captured (first 300):\n{str(requirements_json)[:300]}")

    _uploaded_file_data["requirements_json"] = requirements_json
    return str(requirements_json)


# ---------------------------------------------------------------------------
# Step 4 tools — Human Review
# ---------------------------------------------------------------------------

def _confirm_test_cases(test_cases_json: str) -> str:
    """Present generated test cases for human review."""
    return (
        f"Please review the generated test cases:\n\n{test_cases_json}\n\n"
        "Choose an action:\n"
        "1. 'approve' - Approve test cases as-is\n"
        "2. 'reject'  - Reject and regenerate\n"
        "3. 'modify'  - Provide your modified test cases JSON"
    )


def _process_test_cases_confirmation(confirmation_data: str) -> str:
    """
    Process the user's review decision.
    Accepts: 'approve', 'reject', or JSON like {"action":"modify","modified_content":"..."}
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
        return "Success: Test cases approved and ready for SQL generation."

    elif "modify" in action:
        if modified:
            _uploaded_file_data["requirements_json"] = modified
            return "Success: Test cases modified and approved. Proceeding to SQL generation."
        return "Error: Modification requested but no modified_content provided."

    elif "reject" in action:
        return "Test cases rejected. Please go back to step 2 and select a validation type again."

    else:
        return (
            f"Error: Unrecognised response '{confirmation_data}'. "
            "Please reply with 'approve', 'reject', or 'modify'."
        )


# ---------------------------------------------------------------------------
# Step 5 tool — Generate SQL
# ---------------------------------------------------------------------------

async def _generate_sql_from_approved_test_cases() -> str:
    """
    Generate BigQuery SQL from the approved test cases.
    Reads from stored approved test cases — no arguments needed.
    """
    requirements_json = _uploaded_file_data.get("requirements_json")
    if not requirements_json:
        return "Error: No approved test cases found. Please complete the review step first."

    print(f"[DEBUG] Generating SQL. Test cases (first 150):\n{str(requirements_json)[:150]}")

    runner = InMemoryRunner(agent=sql_agent)
    user_id = "hitl_sql_user"
    session_id = "hitl_sql_session"

    try:
        await runner.session_service.create_session(
            app_name=runner.app_name,
            user_id=user_id,
            session_id=session_id,
        )
    except Exception:
        pass

    sql_message = types.Content(parts=[types.Part(text=str(requirements_json))])
    agent_text: list[str] = []
    outputs: dict = {}

    async for event in runner.run_async(
        user_id=user_id,
        session_id=session_id,
        new_message=sql_message,
    ):
        if event.content and event.content.parts:
            for part in event.content.parts:
                if part.text:
                    agent_text.append(part.text)
        if event.actions and event.actions.state_delta:
            outputs.update(event.actions.state_delta)

    result = outputs.get("sql_tests_json") or "\n".join(agent_text)
    if not result:
        return "Error: No SQL generated. Please check your test cases format."
    return result


# ---------------------------------------------------------------------------
# Root Sequential Agent
# ---------------------------------------------------------------------------

class HITLQASequentialAgent:
    """
    HITL QA Sequential Agent.
    Root ADK Agent whose LLM orchestrates the 5-step workflow via tool calls.
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
                    "5-step HITL QA workflow: file upload → validation selection → "
                    "test case generation → human review → SQL generation"
                ),
                instruction="""
You are a sequential workflow orchestrator managing a 5-step Human-in-the-Loop QA process.

STRICT RULES:
- Follow the steps IN ORDER. Do not skip or reorder steps.
- For steps 3 and 5, you MUST call the designated tool — do NOT generate test cases or SQL yourself.
- Present ALL tool outputs verbatim to the user without modification.
- Wait for user input ONLY at steps 1, 2, and 4.
- NEVER pause between steps 2 and 3 — call _generate_test_cases_from_file immediately after step 2 succeeds.

WORKFLOW:

STEP 1 — File Upload:
  - Call _request_file_upload to prompt the user.
  - When the user provides a filename, call _process_file_upload(file_data="<their exact text>").
  - Only proceed to step 2 when the result starts with "Success:".

STEP 2 — Validation Selection:
  - Call _request_validation_selection to show the menu.
  - When the user picks an option, call _process_validation_selection(validation_choice="<their input>").
  - When the result starts with "Success:", IMMEDIATELY call _generate_test_cases_from_file in the same turn — do NOT wait.

STEP 3 — Generate Test Cases:
  - Call _generate_test_cases_from_file() — no arguments.
  - This reads the real CSV file and sends it to the requirements parser agent automatically.
  - Pass the EXACT output to _confirm_test_cases.

STEP 4 — Human Review:
  - Call _confirm_test_cases(test_cases_json="<exact output from step 3>").
  - Show the result to the user and wait for: 'approve', 'reject', or 'modify'.
  - Call _process_test_cases_confirmation(confirmation_data="<user response>").
  - If result starts with "Success:" → proceed to step 5.
  - If "rejected" → return to step 2.

STEP 5 — Generate SQL:
  - Call _generate_sql_from_approved_test_cases() — no arguments.
  - Present the generated SQL to the user.
  - Workflow complete.
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
                ],
                output_key="workflow_results",
            )
        return self._agent

    def ensure_agent(self) -> Agent:
        return self._ensure_agent()


# Singleton export
_hitl_agent = HITLQASequentialAgent()
root_agent = _hitl_agent.ensure_agent()
