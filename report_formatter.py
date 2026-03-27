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
    Print a clean, human-readable execution report to terminal.

    Expects executor output in format:
    {
      "report_metadata": {...},
      "summary": {
          "total_tests": N, "passed": N, "failed": N,
          "errors": N, "pass_rate_pct": N
      },
      "results": [
          {
            "test_case_id": "tc_001",
            "description": "...",
            "status": "PASS"|"FAIL"|"ERROR",
            "mismatch_row_count": N,
            "mismatch_examples": [...],
            "error_message": "...",
            "execution_time_ms": N
          }
      ]
    }
    """
    report = _safe_parse(raw)

    # Handle case where executor returned plain text (not JSON)
    if not report or "results" not in report:
        print(f"\n{WARN} Executor output (raw):\n{raw}")
        return

    meta    = report.get("report_metadata", {})
    summary = report.get("summary", {})
    results = report.get("results", [])

    env          = meta.get("environment", "unknown").upper()
    mapping_file = meta.get("mapping_file", "unknown")
    val_type     = meta.get("validation_type", "unknown")
    executed_at  = meta.get("executed_at", "")

    total     = summary.get("total_tests", len(results))
    passed    = summary.get("passed", 0)
    failed    = summary.get("failed", 0)
    errors    = summary.get("errors", 0)
    pass_rate = summary.get("pass_rate_pct", 0)

    src = meta.get("source", {})
    tgt = meta.get("target", {})

    # ── Header ────────────────────────────────────────────────────────────────
    print(f"\n{DIVIDER}")
    print(f"  QA EXECUTION REPORT  —  {env}")
    print(f"  Mapping      : {mapping_file}")
    print(f"  Validation   : {val_type}")
    if src:
        print(f"  Source       : {src.get('project_id','')}.{src.get('dataset','')}")
    if tgt:
        print(f"  Target       : {tgt.get('project_id','')}.{tgt.get('dataset','')}")
    if executed_at:
        print(f"  Executed at  : {executed_at}")
    print(DIVIDER)

    # ── Summary bar ───────────────────────────────────────────────────────────
    bar_len  = 30
    filled   = round((passed / total) * bar_len) if total > 0 else 0
    bar      = ("█" * filled) + ("░" * (bar_len - filled))

    print(f"\n  [{bar}] {pass_rate:.1f}% passed\n")
    print(f"  Total   : {total}")
    print(f"  {TICK} Passed : {passed}")
    print(f"  {CROSS} Failed : {failed}")
    if errors:
        print(f"  {WARN} Errors : {errors}")

    # ── Passed tests (compact) ────────────────────────────────────────────────
    passed_results = [r for r in results if r.get("status") == "PASS"]
    if passed_results:
        print(f"\n{SUBDIV}")
        print(f"  {TICK}  PASSED ({len(passed_results)})\n")
        for r in passed_results:
            ms  = r.get("execution_time_ms", 0)
            print(f"  {TICK}  {r.get('test_case_id','?'):10s}  "
                  f"{r.get('description','')[:45]:<45}  "
                  f"({ms}ms)")

    # ── Failed tests (detailed) ───────────────────────────────────────────────
    failed_results = [r for r in results if r.get("status") == "FAIL"]
    if failed_results:
        print(f"\n{SUBDIV}")
        print(f"  {CROSS}  FAILED ({len(failed_results)})\n")
        for r in failed_results:
            ms      = r.get("execution_time_ms", 0)
            count   = r.get("mismatch_row_count", 0)
            examples = r.get("mismatch_examples", [])

            print(f"  {CROSS}  {r.get('test_case_id','?')} — "
                  f"{r.get('description','')}")
            print(f"       Mismatches : {count} row(s)  ({ms}ms)")

            if examples:
                print(f"       Example mismatches (up to {len(examples)}):")
                print(_fmt_table(examples))
            else:
                print(f"       (No example rows available)")
            print()

    # ── Errors ────────────────────────────────────────────────────────────────
    error_results = [r for r in results if r.get("status") == "ERROR"]
    if error_results:
        print(f"\n{SUBDIV}")
        print(f"  {WARN}  ERRORS ({len(error_results)})\n")
        for r in error_results:
            print(f"  {WARN}  {r.get('test_case_id','?')} — "
                  f"{r.get('description','')}")
            print(f"       {r.get('error_message','Unknown error')}\n")

    print(f"\n{DIVIDER}\n")
