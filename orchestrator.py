"""
orchestrator.py

CLI runner for the QA Testing System.
Thin interface layer — all business logic lives in logic_service.QALogicService.

Workflow:
  1. Read CSV mapping file from disk
  2. Turn 1 → Upload CSV → get validation menu
  3. Turn 2 → Choose validation type (1-5)
  4. Turn 3 → Choose environment (dev/build/ist) or Enter for default
  5. Review & refine test cases (HITL loop)
  6. Generate SQL → supervisor + executor run automatically
  7. SQL review & refinement loop
  8. Optional GCS export
"""

import asyncio
import os
import json
from dotenv import load_dotenv

from logic_service import QALogicService
from report_formatter import (
    print_supervisor_report,
    print_execution_report,
    print_sql_summary,
    print_single_sql,
)

load_dotenv()

DIVIDER = "=" * 60
SUBDIV  = "-" * 60


def _print_phase(title: str) -> None:
    print(f"\n{DIVIDER}\n  {title}\n{DIVIDER}")


async def main():
    _print_phase("QA TESTING SYSTEM: CLI MODE")

    USER_ID    = "cli_user"
    SESSION_ID = "cli_session_001"
    qa_service = QALogicService()

    try:
        await qa_service.start_session(USER_ID, SESSION_ID)

        # ── 1. Read CSV ───────────────────────────────────────────────────────
        file_path = input("\n[INPUT] CSV mapping file path: ").strip()
        if not os.path.exists(file_path):
            print(f"[ERROR] File not found: {file_path}")
            return

        with open(file_path, 'r') as f:
            csv_content = f.read()

        # ── 2. Turn 1: CSV upload → validation menu ───────────────────────────
        print("\n[SYSTEM] Analysing requirements...")
        result = await qa_service.process_turn(USER_ID, SESSION_ID, csv_content)
        print(result["text"])

        # ── 3. Turn 2: Validation type ────────────────────────────────────────
        choice = input("\n[INPUT] Your choice (1-5): ").strip()
        result = await qa_service.process_turn(USER_ID, SESSION_ID, choice)
        print(result["text"])

        # ── 4. Turn 3: Environment selection ─────────────────────────────────
        env_input = input(
            "\n[INPUT] Environment (dev/build/ist) — "
            "press Enter for default: "
        ).strip()
        result = await qa_service.process_turn(
            USER_ID, SESSION_ID, env_input or ""
        )
        print(result["text"])
        current_outputs = result.get("outputs", {})

        # If env turn didn't trigger test case generation yet, do it now
        if not current_outputs.get("requirements_json"):
            print("\n[SYSTEM] Generating test cases...")
            result          = await qa_service.process_turn(
                USER_ID, SESSION_ID, csv_content
            )
            current_outputs = result.get("outputs", {})

        # ── 5. Test case review loop ──────────────────────────────────────────
        while True:
            req_json = current_outputs.get("requirements_json")

            _print_phase("STEP: TEST CASE REVIEW & REFINEMENT")
            print(f"\n{req_json or '[No JSON generated yet]'}")
            print(f"\n{SUBDIV}")
            print("  'yes' to approve  |  'no' to exit  |  type feedback to modify")

            feedback = input("\nYour response: ").strip()

            if feedback.lower() == "yes":
                print("\n[SUCCESS] Test cases approved.")
                break
            elif feedback.lower() == "no":
                print("\n[HALT] Exited by user.")
                return
            else:
                print(f"\n[SYSTEM] Applying feedback: '{feedback}'...")
                result = await qa_service.process_turn(
                    USER_ID, SESSION_ID, feedback
                )
                current_outputs.update(result.get("outputs", {}))

        # ── 6. SQL generation (supervisor + executor automatic) ───────────────
        _print_phase("PHASE 2: SQL GENERATION")
        print("\n[SYSTEM] Generating SQL (parallel batching)...")

        approved_json = current_outputs.get("requirements_json")
        sql_result    = await qa_service.generate_sql(
            USER_ID, SESSION_ID, approved_json
        )

        # Supervisor report (clean formatted)
        supervisor = sql_result.get("supervisor")
        if supervisor:
            _print_phase("SUPERVISOR REPORT")
            print_supervisor_report(supervisor)

        # ── 7. SQL review loop ────────────────────────────────────────────────
        while True:
            sql_json = sql_result["outputs"].get("aql_tests_json")

            _print_phase("STEP: SQL REVIEW & REFINEMENT")

            # Show clean summary table — never raw JSON
            if sql_json:
                print_sql_summary(sql_json)
            else:
                print("\n  [No SQL generated yet]")

            print(f"\n{SUBDIV}")
            print("  'yes' to approve  |  'no' to exit  |  type feedback to modify")
            print("  or type a test case ID (e.g. 'tc_002') to inspect its SQL")

            sql_feedback = input("\nYour response: ").strip()

            if sql_feedback.lower() == "yes":
                print("\n[SUCCESS] SQL approved.")
                approved_sql_json = sql_json
                break

            elif sql_feedback.lower() == "no":
                print("\n[HALT] Exited by user.")
                return

            # Check if user typed a test case ID to inspect
            elif sql_json and sql_feedback.lower().startswith("tc"):
                print_single_sql(sql_json, sql_feedback)
                # Stay in loop — don't regenerate

            else:
                print(f"\n[SYSTEM] Refining SQL: '{sql_feedback}'...")
                sql_result = await qa_service.generate_sql(
                    USER_ID, SESSION_ID, sql_feedback
                )
                # Show updated supervisor report
                supervisor = sql_result.get("supervisor")
                if supervisor:
                    _print_phase("UPDATED SUPERVISOR REPORT")
                    print_supervisor_report(supervisor)



        # ── 8. GCS export (optional) ──────────────────────────────────────────
        _print_phase("PHASE 3: EXPORT TO GCS (optional)")

        from gcs_utils import generate_default_filename
        default_name = generate_default_filename(approved_sql_json or "{}")
        print(f"\n  Default filename: {default_name}")

        gcs_path = input(
            "\n[INPUT] GCS path (e.g. gs://my-bucket/qa-artifacts/) "
            "— press Enter to skip: "
        ).strip()

        if gcs_path:
            custom_name = input(
                f"[INPUT] File name (Enter to use '{default_name}'): "
            ).strip()
            filename = custom_name or default_name
            try:
                uri = qa_service.save_artifact_to_gcs(
                    approved_sql_json, gcs_path, filename
                )
                print(f"\n[SUCCESS] Saved to: {uri}")
            except Exception as e:
                print(f"\n[ERROR] GCS upload failed: {e}")
                print(f"[INFO]  Save manually as '{default_name}'.")
        else:
            print("\n[INFO] GCS export skipped.")

        # ── 9. Execute test cases prompt ──────────────────────────────────────
        _print_phase("PHASE 4: EXECUTE TEST CASES")
        print("\n  Would you like to execute the test cases against BigQuery?")
        print(f"  Environment : {qa_service.get_selected_environment()}")
        supervisor = sql_result.get("supervisor", {})
        sup_status = supervisor.get("overall_status", "unknown")
        if sup_status == "manual_review_required":
            print(f"\n  ⚠️   Supervisor flagged issues requiring manual review.")
            print(    "      Execution is still possible but review the supervisor report first.")
        print()
        print("  'yes' — execute test cases on BigQuery")
        print("  'no'  — finish without executing")

        exec_choice = input("\nYour response: ").strip().lower()

        if exec_choice == "yes":
            print("\n[SYSTEM] Executing test cases on BigQuery...")
            # Use supervisor-fixed SQL if available, otherwise approved SQL
            sql_to_execute = (
                json.dumps(supervisor.get("sql_fixed"), indent=2)
                if supervisor.get("sql_fixed")
                else approved_sql_json
            )
            executor_result = await qa_service._run_executor(
                USER_ID, SESSION_ID, sql_to_execute
            )
            exec_output = (
                executor_result.get("outputs", {}).get("execution_report")
                or executor_result.get("text", "")
            )
            _print_phase("EXECUTION REPORT")
            print_execution_report(exec_output)
        else:
            print("\n[INFO] Execution skipped.")

        # ── Final summary ─────────────────────────────────────────────────────
        print(f"\n{DIVIDER}")
        print("  ✅  QA WORKFLOW COMPLETE")
        print(f"  Environment  : {qa_service.get_selected_environment()}")
        print(f"  Validation   : {qa_service.get_selected_validation_type()}")
        print(f"{DIVIDER}\n")

    except Exception as e:
        print(f"\n[CRITICAL ERROR] {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
