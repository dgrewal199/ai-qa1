"""
Human-in-the-Loop Sequential QA Agent
5-step workflow: File Upload -> Validation Selection -> Generate Test Cases -> Human Review -> Generate SQL
"""

from typing import Dict, Any, Optional
import json
import asyncio
import re

from google.adk.runners import InMemoryRunner
from google.genai import types
from google.adk.agents import Agent

# Import your existing agents
from ..requirements_parser.agent import dynamic_agent
from ..sql_generator.agent import root_agent as sql_agent

# ---------------------------------------------------------------------------
# Global state (mirrors logic_service.py pattern)
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
# Step 1 tools  — File Upload
# ---------------------------------------------------------------------------

def _request_file_upload() -> str:
    """Tool to request mapping file upload from user."""
    return "Please upload your mapping file (.xlsx or .csv format). Accepted formats: .xlsx, .csv"


def _process_file_upload(file_data: str) -> str:
    """
    Process the user's file upload.
    Pass the user's exact message text as file_data (e.g. 'balances.csv').
    """
    print(f"[DEBUG] _process_file_upload called with: {repr(file_data)}")
    try:
        file_info = json.loads(file_data)
        if isinstance(file_info, dict):
            filename = file_info.get("filename", "")
            content = file_info.get("content", "")
            if not filename or not content:
                return "Error: Invalid file data. Please ensure the file contains valid data."
            _store_file_content(filename, content)
            return f"Success: File '{filename}' uploaded successfully and ready for processing."
        # json.loads succeeded but returned a plain string — treat as filename
        file_data = str(file_info)
    except (json.JSONDecodeError, ValueError):
        pass

    # Extract filename from whatever the LLM passed (handles quotes, extra text, etc.)
    match = re.search(r'([\w\-. ]+\.(csv|xlsx))', file_data, re.IGNORECASE)
    if not match:
        return "Error: No valid .csv or .xlsx filename found. Please provide a filename like 'mapping.csv'."

    filename = match.group(0).strip()

    # Testing fallback: generate mock CSV content matching requirements parser expectations
    base = filename.replace('.csv', '').replace('.xlsx', '')
    mock_content = f"""Primary key : account_id + balance_date
source_table,target_table,source_column_name,target_column_name,source_column_datatype,target_column_datatype,business_rules
{base},iam_account_balances,account_number,account_id,STRING,STRING,Direct mapping from source account number to target account ID
{base},iam_account_balances,balance_amount,current_balance,DECIMAL,DECIMAL,SAFE_CAST balance amount to decimal with 2 decimal places
{base},iam_account_balances,balance_date,balance_date,DATE,DATE,Direct mapping of balance date
{base},iam_account_balances,,client_id,STRING,STRING,Lookup client ID from reference table client_master on account_number = contract_no
{base},iam_account_balances,,country_code,STRING,STRING,Default value 'MK' for all records
{base},iam_account_balances,,created_timestamp,TIMESTAMP,TIMESTAMP,System generated current timestamp"""

    _store_file_content(filename, mock_content)
    return f"Success: File '{filename}' uploaded successfully and ready for processing."


# ---------------------------------------------------------------------------
# Step 2 tools  — Validation Selection
# ---------------------------------------------------------------------------

def _request_validation_selection() -> str:
    """Tool to show validation type menu to user."""
    return (
        "Please select validation type for test case generation.\n"
        "1. ddl_validation  - DDL Validation (database schema validation)\n"
        "2. completeness    - Completeness Validation (data completeness checks)\n"
        "3. uniqueness      - Uniqueness Validation (verify primary keys, no duplicates/nulls)\n"
        "4. column_transformation - Column Transformation Validation (verify source-to-target mappings)\n"
        "5. all             - All Validations"
    )


def _process_validation_selection(validation_choice: str) -> str:
    """
    Store the user's validation type choice.
    Pass the user's exact text as validation_choice (e.g. '4' or 'column_transformation').
    """
    aliases = {
        "1": "ddl_validation",
        "2": "completeness",
        "3": "uniqueness",
        "4": "column_transformation",
        "5": "all",
    }
    choice = aliases.get(validation_choice.strip(), validation_choice.strip())
    valid = ["ddl_validation", "completeness", "uniqueness", "column_transformation", "all"]
    if choice not in valid:
        return f"Error: Invalid choice '{validation_choice}'. Please choose 1-5 or: {', '.join(valid)}"

    global _uploaded_file_data
    _uploaded_file_data["validation_type"] = choice
    return f"Success: Selected validation type '{choice}' for test case generation."


# ---------------------------------------------------------------------------
# Step 3 tool  — Generate Test Cases  (THIS is the key fix)
# Called as a tool by the root_agent LLM.
# Uses InMemoryRunner + run_async to properly capture output_key="requirements_json"
# from the session state delta, exactly like logic_service.py
# ---------------------------------------------------------------------------

async def _generate_test_cases_from_file() -> str:
    """
    Tool to generate QA test cases from the uploaded mapping file.
    Calls the requirements parser agent with the file content and selected validation type.
    No arguments needed — reads from uploaded file and selected validation type.
    """
    file_content = _get_file_content()
    validation_type = _uploaded_file_data.get("validation_type", "column_transformation")

    if not file_content:
        return "Error: No mapping file uploaded yet. Please upload a file first."
    if not validation_type:
        return "Error: No validation type selected. Please select a validation type first."

    print(f"[DEBUG] Generating test cases. Validation: {validation_type}")
    print(f"[DEBUG] File content (first 150): {file_content[:150]}")

    # Configure agent for validation type — same as logic_service.py
    dynamic_agent.update_for_validation_type(validation_type)

    # Create a fresh InMemoryRunner session for this call — same as logic_service.py
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
        pass  # Session already exists from a prior call

    # Wrap as types.Content — same as logic_service.py
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
        # output_key="requirements_json" is emitted here as a state_delta
        if event.actions and event.actions.state_delta:
            outputs.update(event.actions.state_delta)

    # Prefer state_delta result (structured JSON), fall back to text
    requirements_json = outputs.get("requirements_json") or "\n".join(agent_text)
    print(f"[DEBUG] requirements_json captured: {str(requirements_json)[:300]}")

    # Store for SQL generation step
    _uploaded_file_data["requirements_json"] = requirements_json
    return str(requirements_json)


# ---------------------------------------------------------------------------
# Step 4 tools  — Human Review
# ---------------------------------------------------------------------------

def _confirm_test_cases(test_cases_json: str) -> str:
    """Tool for human review of generated test cases."""
    return (
        f"Please review the generated test cases:\n\n{test_cases_json}\n\n"
        "Choose an action:\n"
        "1. 'approve'  - Approve test cases as-is\n"
        "2. 'reject'   - Reject and restart\n"
        "3. 'modify'   - Provide modified test cases"
    )


def _process_test_cases_confirmation(confirmation_data: str) -> str:
    """
    Process the user's approval decision.
    Pass the user's exact text as confirmation_data (e.g. 'approve', 'reject', or 'modify').
    """
    try:
        decision = json.loads(confirmation_data)
        action = decision.get("action", "reject").lower()
    except (json.JSONDecodeError, ValueError):
        action = confirmation_data.lower().strip()

    if action == "approve":
        return "Success: Test cases approved by user and ready for SQL generation."
    elif action == "modify":
        try:
            decision_dict = json.loads(confirmation_data)
            modified = decision_dict.get("modified_content", "")
        except (json.JSONDecodeError, AttributeError):
            modified = ""
        if modified:
            _uploaded_file_data["requirements_json"] = modified
            return f"Success: Test cases modified and approved. Updated test cases: {modified}"
        return "Error: Modification requested but no changes provided."
    else:
        return "Test cases rejected by user. Please regenerate test cases or modify requirements."


# ---------------------------------------------------------------------------
# Step 5 tool  — Generate SQL  (calls SQL generator via InMemoryRunner)
# ---------------------------------------------------------------------------

async def _generate_sql_from_approved_test_cases() -> str:
    """
    Tool to generate BigQuery SQL from the approved test cases.
    No arguments needed — reads from the stored approved test cases.
    """
    requirements_json = _uploaded_file_data.get("requirements_json")
    if not requirements_json:
        return "Error: No approved test cases found. Please complete test case review first."

    print(f"[DEBUG] Generating SQL from test cases: {str(requirements_json)[:150]}")

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
        pass  # Session already exists from a prior call

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

    return outputs.get("sql_tests_json") or "\n".join(agent_text)


# ---------------------------------------------------------------------------
# Sequential Agent wrapper + root_agent export
# ---------------------------------------------------------------------------

class HITLQASequentialAgent:
    """
    HITL QA Sequential Agent.

    The root_agent is an ADK Agent whose LLM orchestrates the 5-step workflow
    by calling the registered tool functions in order.  Steps 3 and 5 use
    async tools that call the requirements parser and SQL generator via
    InMemoryRunner (same pattern as logic_service.py).
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
- Wait for user input ONLY at steps 1, 2, and 4. NEVER pause or return to the user between steps 2 and 3.

WORKFLOW:

STEP 1 — File Upload:
  - Call _request_file_upload to prompt the user.
  - When the user responds with a filename (e.g. "balances.csv"), call _process_file_upload(file_data="balances.csv") passing the user's exact text.
  - Only proceed to step 2 when _process_file_upload returns "Success:".

STEP 2 — Validation Selection:
  - Call _request_validation_selection to show the menu.
  - When the user selects an option (number 1-5 or the type name), call _process_validation_selection with their input.
  - When _process_validation_selection returns "Success:", you MUST IMMEDIATELY call _generate_test_cases_from_file in the SAME response without stopping.

STEP 3 — Generate Test Cases:
  - Call _generate_test_cases_from_file (no arguments).
  - This tool reads the uploaded file and validation type automatically.
  - Pass the EXACT output of this tool to _confirm_test_cases.

STEP 4 — Human Review:
  - Call _confirm_test_cases with the test cases string from step 3.
  - Show the output and wait for user to respond with 'approve', 'reject', or 'modify'.
  - Call _process_test_cases_confirmation with the user's response.
  - If approved → proceed to step 5.
  - If rejected  → go back to step 2.
  - If modified  → proceed to step 5 with the modified test cases.

STEP 5 — Generate SQL:
  - Call _generate_sql_from_approved_test_cases (no arguments).
  - Present the generated SQL to the user.
  - Workflow complete.
""",
                tools=[
                    _request_file_upload,
                    _process_file_upload,
                    _request_validation_selection,
                    _process_validation_selection,
                    _generate_test_cases_from_file,   # async — calls req parser via InMemoryRunner
                    _confirm_test_cases,
                    _process_test_cases_confirmation,
                    _generate_sql_from_approved_test_cases,  # async — calls sql generator via InMemoryRunner
                ],
                output_key="workflow_results",
            )
        return self._agent

    def ensure_agent(self) -> Agent:
        return self._ensure_agent()


# Singleton — imported by your ADK app / runner
_hitl_agent = HITLQASequentialAgent()
root_agent = _hitl_agent.ensure_agent()
