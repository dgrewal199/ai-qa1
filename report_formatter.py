"""
report_formatter.py

Clean terminal report formatter for QA test execution results.
Used by orchestrator.py to display executor + supervisor output.
"""

import json
from typing import Union


DIVIDER  = "=" * 60
SUBDIV   = "-" * 60
TICK     = "✅"
CROSS    = "❌"
WARN     = "⚠️ "
INFO     = "ℹ️ "


def _safe_parse(raw: Union[str, dict]) -> dict:
    """Parse JSON string or return dict as-is."""
    if isinstance(raw, dict):
        return raw
    try:
        import re
        cleaned = re.sub(r'```(?:json)?|```', '', str(raw)).strip()
        return json.loads(cleaned)
    except (json.JSONDecodeError, TypeError):
        return {}


# ─────────────────────────────────────────────────────────────────────────────
# Supervisor report
# ─────────────────────────────────────────────────────────────────────────────

def print_supervisor_report(raw: Union[str, dict]) -> None:
    """Print a clean supervisor critique report to terminal."""
    report = _safe_parse(raw)
    if not report:
        print(f"\n{WARN} Could not parse supervisor report.\nRaw output:\n{raw}")
        return

    status   = report.get("overall_status", "unknown").upper()
    score    = report.get("confidence_score", "N/A")
    summary  = report.get("fix_summary", "")
    notes    = report.get("manual_review_notes", "")
    issues   = report.get("issues", [])

    critical = [i for i in issues if i.get("severity") == "critical"]
    warnings = [i for i in issues if i.get("severity") == "warning"]
    infos    = [i for i in issues if i.get("severity") == "info"]

    status_icon = TICK if status in ("PASS", "FIXED") else CROSS

    print(f"\n{DIVIDER}")
    print(f"  SUPERVISOR REPORT  {status_icon} {status}")
    print(f"  Confidence Score : {score}/100")
    print(f"  {summary}")
    print(DIVIDER)

    if not issues:
        print(f"\n  {TICK} No issues found — SQL looks good.")
    else:
        if critical:
            print(f"\n  🔴 CRITICAL ({len(critical)}):")
            for i in critical:
                print(f"\n    [{i.get('test_case_id','?')}] "
                      f"{i.get('check_category','')}")
                print(f"    Finding : {i.get('finding','')}")
                if i.get("fix_status") == "auto_fixed":
                    print(f"    {TICK} Fixed  : {i.get('fix_description','')}")
                else:
                    print(f"    {CROSS} Blocked: {i.get('fix_blocked_reason','')}")

        if warnings:
            print(f"\n  🟡 WARNINGS ({len(warnings)}):")
            for i in warnings:
                print(f"\n    [{i.get('test_case_id','?')}] {i.get('finding','')}")
                if i.get("fix_status") == "auto_fixed":
                    print(f"    {TICK} Fixed: {i.get('fix_description','')}")

        if infos:
            print(f"\n  {INFO} INFO ({len(infos)}):")
            for i in infos:
                print(f"    [{i.get('test_case_id','?')}] {i.get('finding','')}")

    if notes:
        print(f"\n  📋 Manual Review Notes:\n    {notes}")

    print(f"\n{SUBDIV}\n")


# ─────────────────────────────────────────────────────────────────────────────
# Execution report
# ─────────────────────────────────────────────────────────────────────────────

def _fmt_table(rows: list, max_col: int = 28) -> str:
    """Format a list of dicts as an ASCII table."""
    if not rows:
        return "    (no example rows)"

    headers = list(rows[0].keys())

    def trunc(v):
        s = str(v) if v is not None else "NULL"
        return (s[:max_col] + "…") if len(s) > max_col else s

    widths = {
        h: max(len(h), max(len(trunc(r.get(h, ""))) for r in rows))
        for h in headers
    }

    def sep(l, m, r):
        return l + m.join("─" * (widths[h] + 2) for h in headers) + r

    def row_str(row):
        return "│ " + " │ ".join(
            trunc(row.get(h, "")).ljust(widths[h]) for h in headers
        ) + " │"

    hdr = "│ " + " │ ".join(h.ljust(widths[h]) for h in headers) + " │"

    lines = [
        "    " + sep("┌", "┬", "┐"),
        "    " + hdr,
        "    " + sep("├", "┼", "┤"),
    ]
    for r in rows:
        lines.append("    " + row_str(r))
    lines.append("    " + sep("└", "┴", "┘"))
    return "\n".join(lines)


def print_sql_summary(raw: Union[str, dict]) -> None:
    """
    Print a clean summary table of generated test cases + SQL.
    Does NOT dump raw JSON or raw SQL strings.
    User can type a test_case_id to inspect individual SQL.
    """
    data = _safe_parse(raw)
    if not data:
        print(f"\n{WARN} Could not parse SQL output.")
        return

    test_cases = (
        data if isinstance(data, list)
        else data.get("test_cases", [])
    )

    if not test_cases:
        print(f"\n{WARN} No test cases found in output.")
        return

    total     = len(test_cases)
    pk_issues = sum(
        1 for tc in test_cases
        if tc.get("sql_generation_status") == "PK_VALIDATION_FAILED"
    )
    ready     = total - pk_issues

    print(f"\n  {total} SQL quer{'y' if total == 1 else 'ies'} generated  "
          f"|  {ready} ready  "
          + (f"|  {WARN} {pk_issues} PK issues" if pk_issues else ""))

    # Column widths
    id_w   = max(len("Test Case"), max(len(tc.get("test_case_id", "?"))
                 for tc in test_cases))
    desc_w = 48
    st_w   = 10

    def sep(l, m, r):
        return l + m.join(["─" * (id_w + 2),
                            "─" * (desc_w + 2),
                            "─" * (st_w + 2)]) + r

    def row(tc_id, desc, status):
        return (f"│ {tc_id:<{id_w}} │ "
                f"{desc:<{desc_w}} │ "
                f"{status:<{st_w}} │")

    print(f"\n  {sep('┌', '┬', '┐')}")
    print(f"  {row('Test Case', 'Description', 'Status')}")
    print(f"  {sep('├', '┼', '┤')}")

    for tc in test_cases:
        tc_id  = tc.get("test_case_id", "?")
        desc   = (tc.get("description") or tc.get("test_case_name") or "")[:desc_w]
        pk_fail = tc.get("sql_generation_status") == "PK_VALIDATION_FAILED"
        status = f"{WARN} PK issue" if pk_fail else f"{TICK} Ready"
        print(f"  {row(tc_id, desc, status)}")

    print(f"  {sep('└', '┴', '┘')}")
    print(f"\n  Type a test case ID to inspect its SQL (e.g. '{test_cases[0].get('test_case_id','tc_001')}')")


def print_single_sql(raw: Union[str, dict], test_case_id: str) -> bool:
    """
    Print the SQL for a single test case ID cleanly.
    Returns True if found, False if not found.
    """
    data = _safe_parse(raw)
    test_cases = (
        data if isinstance(data, list)
        else data.get("test_cases", [])
    )

    for tc in test_cases:
        if tc.get("test_case_id", "").lower() == test_case_id.lower():
            desc = tc.get("description") or tc.get("test_case_name") or ""
            sql  = tc.get("sql_query") or tc.get("sql") or ""

            # Clean up the SQL — convert literal \n to real newlines,
            # strip excess whitespace, normalise indentation
            sql = sql.replace("\\n", "\n").replace("\\t", "    ")
            sql = "\n".join(
                line for line in sql.splitlines()
                if line.strip()  # remove blank lines caused by \n\n
            )

            pk_err = tc.get("pk_validation_error", "")

            print(f"\n{SUBDIV}")
            print(f"  {tc.get('test_case_id')} — {desc}")
            print(SUBDIV)
            print(f"\n{sql}\n")
            if pk_err:
                print(f"  {WARN} PK Issue: {pk_err}")
            print(SUBDIV)
            return True

    print(f"\n  {WARN} Test case '{test_case_id}' not found.")
    return False


def print_execution_report(raw: Union[str, dict]) -> None:
    """
    Print a clean execution report from the ExecuteBQSQLAgent output.

    The executor agent (output_key="execution_report_yaml") appends these
    fields to each test case:
        execution_status : PASSED | FAILED | SOFT_FAILURE
        actual_values    : what BQ returned
        expected_value   : what was expected
        failure_reason   : why it failed

    And ends with a FINAL EXECUTION SUMMARY section.

    Handles both:
      - JSON format: {"test_cases": [...], "summary": {...}}
      - Plain text / YAML-like format from the agent
    """
    import re as _re

    # ── Try structured JSON first ─────────────────────────────────────────────
    data = _safe_parse(raw)
    test_cases = []
    summary    = {}

    if data and isinstance(data, dict):
        test_cases = data.get("test_cases", [])
        summary    = data.get("summary", data.get("execution_summary", {}))
    elif data and isinstance(data, list):
        test_cases = data

    # ── If JSON worked and has test cases — use structured path ───────────────
    if test_cases:
        passed_list = [
            tc for tc in test_cases
            if str(tc.get("execution_status", "")).upper() == "PASSED"
        ]
        failed_list = [
            tc for tc in test_cases
            if str(tc.get("execution_status", "")).upper()
            in ("FAILED", "SOFT_FAILURE")
        ]

        total     = len(test_cases)
        passed    = len(passed_list)
        failed    = len(failed_list)
        pass_rate = round((passed / total * 100), 1) if total > 0 else 0

        # Override with summary block if present
        if summary:
            total     = summary.get("total_test_cases_executed",
                        summary.get("total", total))
            passed    = summary.get("total_passed",
                        summary.get("passed", passed))
            failed    = summary.get("total_failed",
                        summary.get("failed", failed))
            pass_rate = round((passed / total * 100), 1) if total > 0 else 0

        # Header
        bar_len = 30
        filled  = round((passed / total) * bar_len) if total > 0 else 0
        bar     = ("█" * filled) + ("░" * (bar_len - filled))

        print(f"\n{DIVIDER}")
        print(f"  QA EXECUTION REPORT")
        print(DIVIDER)
        print(f"\n  [{bar}] {pass_rate:.1f}% passed\n")
        print(f"  Total        : {total}")
        print(f"  {TICK} Passed : {passed}")
        print(f"  {CROSS} Failed : {failed}")

        # Passed (compact)
        if passed_list:
            print(f"\n{SUBDIV}")
            print(f"  {TICK}  PASSED ({len(passed_list)})\n")
            for tc in passed_list:
                tc_id = tc.get("test_case_id", "?")
                desc  = (tc.get("description") or
                         tc.get("test_case_name") or "")[:48]
                print(f"  {TICK}  {tc_id:12s}  {desc}")

        # Failed (detailed)
        if failed_list:
            print(f"\n{SUBDIV}")
            print(f"  {CROSS}  FAILED ({len(failed_list)})\n")
            for tc in failed_list:
                tc_id   = tc.get("test_case_id", "?")
                desc    = (tc.get("description") or
                           tc.get("test_case_name") or "")
                status  = str(tc.get("execution_status", "FAILED")).upper()
                reason  = tc.get("failure_reason", "")
                actual  = tc.get("actual_values", "")
                expected= tc.get("expected_value", "")

                icon = WARN if status == "SOFT_FAILURE" else CROSS
                print(f"  {icon}  {tc_id} — {desc}")
                if status == "SOFT_FAILURE":
                    print(f"       Status   : SOFT FAILURE (missing rows)")
                if reason:
                    print(f"       Reason   : {reason}")
                if expected:
                    print(f"       Expected : {expected}")
                if actual:
                    print(f"       Actual   : {actual}")

                # Show diagnostic sample rows if present
                samples = tc.get("sample_values") or tc.get("diagnostic_rows")
                if samples and isinstance(samples, list):
                    print(f"       Sample mismatches:")
                    print(_fmt_table(samples[:2]))
                print()

        print(f"\n{DIVIDER}\n")
        return

    # ── Fallback: parse plain text / YAML-like output from agent ─────────────
    # The agent sometimes returns formatted text instead of JSON.
    # Parse it into sections and display cleanly.
    raw_str = str(raw).strip()
    if not raw_str:
        print(f"\n{WARN} No executor output received.")
        return

    print(f"\n{DIVIDER}")
    print(f"  QA EXECUTION REPORT")
    print(DIVIDER)

    # Extract summary line counts
    total_match  = _re.search(r"total[_\s]test[_\s]cases[_\s]executed[:\s]+([\d]+)",
                               raw_str, _re.IGNORECASE)
    passed_match = _re.search(r"total[_\s]passed[:\s]+([\d]+)",
                               raw_str, _re.IGNORECASE)
    failed_match = _re.search(r"total[_\s]failed[:\s]+([\d]+)",
                               raw_str, _re.IGNORECASE)

    if total_match or passed_match or failed_match:
        total  = int(total_match.group(1))  if total_match  else "?"
        passed = int(passed_match.group(1)) if passed_match else "?"
        failed = int(failed_match.group(1)) if failed_match else "?"

        if isinstance(total, int) and isinstance(passed, int):
            pass_rate = round((passed / total * 100), 1) if total > 0 else 0
            bar_len   = 30
            filled    = round((passed / total) * bar_len) if total > 0 else 0
            bar       = ("█" * filled) + ("░" * (bar_len - filled))
            print(f"\n  [{bar}] {pass_rate:.1f}% passed\n")

        print(f"  Total        : {total}")
        print(f"  {TICK} Passed : {passed}")
        print(f"  {CROSS} Failed : {failed}")

    # Extract individual test case blocks
    # Look for patterns like "test_case_id: tc_001" or "- test_case_id: tc_001"
    tc_blocks = _re.split(
        r"(?=(?:-\s+)?test_case_id\s*:|(?:-\s+)?"test_case_id"\s*:)",
        raw_str
    )

    passed_tcs = []
    failed_tcs = []

    for block in tc_blocks:
        if not block.strip():
            continue

        tc_id_m  = _re.search(r"test_case_id["\s]*:["\s]*([\w\-]+)",
                               block, _re.IGNORECASE)
        status_m = _re.search(r"execution_status["\s]*:["\s]*(\w+)",
                               block, _re.IGNORECASE)
        reason_m = _re.search(r"failure_reason["\s]*:["\s]*(.+?)(?:\n|$)",
                               block, _re.IGNORECASE)
        actual_m = _re.search(r"actual_values["\s]*:["\s]*(.+?)(?:\n|$)",
                               block, _re.IGNORECASE)
        expect_m = _re.search(r"expected_value["\s]*:["\s]*(.+?)(?:\n|$)",
                               block, _re.IGNORECASE)

        if not tc_id_m:
            continue

        tc_id  = tc_id_m.group(1).strip()
        status = status_m.group(1).strip().upper() if status_m else "UNKNOWN"

        if status == "PASSED":
            passed_tcs.append(tc_id)
        else:
            failed_tcs.append({
                "tc_id":    tc_id,
                "status":   status,
                "reason":   reason_m.group(1).strip() if reason_m else "",
                "actual":   actual_m.group(1).strip() if actual_m else "",
                "expected": expect_m.group(1).strip() if expect_m else "",
            })

    if passed_tcs:
        print(f"\n{SUBDIV}")
        print(f"  {TICK}  PASSED ({len(passed_tcs)})\n")
        for tc_id in passed_tcs:
            print(f"  {TICK}  {tc_id}")

    if failed_tcs:
        print(f"\n{SUBDIV}")
        print(f"  {CROSS}  FAILED ({len(failed_tcs)})\n")
        for tc in failed_tcs:
            icon = WARN if tc["status"] == "SOFT_FAILURE" else CROSS
            print(f"  {icon}  {tc['tc_id']}")
            if tc["status"] == "SOFT_FAILURE":
                print(f"       Status   : SOFT FAILURE (missing rows)")
            if tc["reason"]:
                print(f"       Reason   : {tc['reason']}")
            if tc["expected"]:
                print(f"       Expected : {tc['expected']}")
            if tc["actual"]:
                print(f"       Actual   : {tc['actual']}")
            print()

    # If we couldn't parse anything meaningful, show raw but cleaned up
    if not passed_tcs and not failed_tcs and not total_match:
        clean = raw_str.replace("\\n", "\n").replace("\\t", "  ")
        print(f"\n{clean}")

    print(f"\n{DIVIDER}\n")
