"""
report_formatter.py

Clean terminal report formatter for QA test execution results.
Used by orchestrator.py to display executor + supervisor output.

Functions:
  print_supervisor_report()     - supervisor critique report
  print_sql_summary()           - clean table of generated test cases
  print_single_sql()            - show SQL for a single test case ID
  extract_tc_descriptions()     - extract {tc_id: description} from SQL JSON
  ExecutionProgressTracker      - live per-query progress as agent streams
  print_execution_report()      - final pass/fail summary report
"""

import json
import re
from typing import Union, Dict

DIVIDER = "=" * 60
SUBDIV  = "-" * 60
TICK    = "✅"
CROSS   = "❌"
WARN    = "⚠️ "
INFO    = "ℹ️ "


def _safe_parse(raw: Union[str, dict]) -> dict:
    """Parse JSON string or return dict as-is."""
    if isinstance(raw, dict):
        return raw
    try:
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

    status      = report.get("overall_status", "unknown").upper()
    score       = report.get("confidence_score", "N/A")
    summary     = report.get("fix_summary", "")
    notes       = report.get("manual_review_notes", "")
    issues      = report.get("issues", [])
    critical    = [i for i in issues if i.get("severity") == "critical"]
    warnings    = [i for i in issues if i.get("severity") == "warning"]
    infos       = [i for i in issues if i.get("severity") == "info"]
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
                print(f"\n    [{i.get('test_case_id', '?')}] {i.get('check_category', '')}")
                print(f"    Finding : {i.get('finding', '')}")
                if i.get("fix_status") == "auto_fixed":
                    print(f"    {TICK} Fixed  : {i.get('fix_description', '')}")
                else:
                    print(f"    {CROSS} Blocked: {i.get('fix_blocked_reason', '')}")
        if warnings:
            print(f"\n  🟡 WARNINGS ({len(warnings)}):")
            for i in warnings:
                print(f"\n    [{i.get('test_case_id', '?')}] {i.get('finding', '')}")
                if i.get("fix_status") == "auto_fixed":
                    print(f"    {TICK} Fixed: {i.get('fix_description', '')}")
        if infos:
            print(f"\n  {INFO} INFO ({len(infos)}):")
            for i in infos:
                print(f"    [{i.get('test_case_id', '?')}] {i.get('finding', '')}")

    if notes:
        print(f"\n  📋 Manual Review Notes:\n    {notes}")
    print(f"\n{SUBDIV}\n")


# ─────────────────────────────────────────────────────────────────────────────
# SQL summary table
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

    hdr   = "│ " + " │ ".join(h.ljust(widths[h]) for h in headers) + " │"
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
    Print a clean summary table of generated test cases.
    Does NOT dump raw JSON or SQL strings.
    """
    data = _safe_parse(raw)
    if not data:
        print(f"\n{WARN} Could not parse SQL output.")
        return

    test_cases = data if isinstance(data, list) else data.get("test_cases", [])
    if not test_cases:
        print(f"\n{WARN} No test cases found in output.")
        return

    total     = len(test_cases)
    pk_issues = sum(1 for tc in test_cases
                    if tc.get("sql_generation_status") == "PK_VALIDATION_FAILED")
    ready     = total - pk_issues

    suffix = f"  |  {WARN} {pk_issues} PK issues" if pk_issues else ""
    print(f"\n  {total} SQL {'query' if total == 1 else 'queries'} generated  |  {ready} ready{suffix}")

    id_w   = max(len("Test Case"), max(len(tc.get("test_case_id", "?")) for tc in test_cases))
    desc_w = 48
    st_w   = 10

    def sep(l, m, r):
        return l + m.join(["─" * (id_w + 2), "─" * (desc_w + 2), "─" * (st_w + 2)]) + r

    def row(tc_id, desc, status):
        return f"│ {tc_id:<{id_w}} │ {desc:<{desc_w}} │ {status:<{st_w}} │"

    print(f"\n  {sep('┌', '┬', '┐')}")
    print(f"  {row('Test Case', 'Description', 'Status')}")
    print(f"  {sep('├', '┼', '┤')}")
    for tc in test_cases:
        tc_id   = tc.get("test_case_id", "?")
        desc    = (tc.get("description") or tc.get("test_case_name") or "")[:desc_w]
        pk_fail = tc.get("sql_generation_status") == "PK_VALIDATION_FAILED"
        status  = f"{WARN} PK issue" if pk_fail else f"{TICK} Ready"
        print(f"  {row(tc_id, desc, status)}")
    print(f"  {sep('└', '┴', '┘')}")
    print(f"\n  Type a test case ID to inspect its SQL (e.g. '{test_cases[0].get('test_case_id', 'tc_001')}')")


def print_single_sql(raw: Union[str, dict], test_case_id: str) -> bool:
    """
    Print the SQL for a single test case ID cleanly.
    Returns True if found, False if not.
    """
    data       = _safe_parse(raw)
    test_cases = data if isinstance(data, list) else data.get("test_cases", [])

    for tc in test_cases:
        if tc.get("test_case_id", "").lower() == test_case_id.lower():
            desc   = tc.get("description") or tc.get("test_case_name") or ""
            sql    = tc.get("sql_query") or tc.get("sql") or ""
            sql    = sql.replace("\\n", "\n").replace("\\t", "    ")
            sql    = "\n".join(line for line in sql.splitlines() if line.strip())
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


# ─────────────────────────────────────────────────────────────────────────────
# Live execution progress tracker
# ─────────────────────────────────────────────────────────────────────────────

def extract_tc_descriptions(sql_json: Union[str, dict]) -> Dict[str, str]:
    """
    Extract {tc_id: description} mapping from approved SQL JSON.
    Used to populate descriptions in progress lines.
    """
    data = _safe_parse(sql_json) if isinstance(sql_json, str) else sql_json
    if not data:
        return {}
    test_cases = data if isinstance(data, list) else data.get("test_cases", [])
    return {
        tc.get("test_case_id", "").lower(): (
            tc.get("description") or tc.get("test_case_name") or ""
        )
        for tc in test_cases
        if tc.get("test_case_id")
    }


class ExecutionProgressTracker:
    """
    Parses streamed agent event text fragments to detect test case
    execution status and prints a clean progress line immediately.

    Usage:
        tracker = ExecutionProgressTracker(total=10, descriptions=desc_dict)
        async for event in runner.run_async(...):
            if event.content and event.content.parts:
                for part in event.content.parts:
                    if part.text:
                        tracker.process_event(part.text)
                        agent_text.append(part.text)
        tracker.print_divider()
    """

    # Patterns that indicate a test case just completed
    # Each tuple: (regex_pattern, fixed_status_or_None)
    _STATUS_PATTERNS = [
        (r'(tc_[\w\-]+)[\s:\-]+(?:status[\s:]+)?(PASSED|PASS)\b',        "PASSED"),
        (r'(tc_[\w\-]+)[\s:\-]+(?:status[\s:]+)?(FAILED|FAIL)\b',        "FAILED"),
        (r'(tc_[\w\-]+)[\s:\-]+(?:status[\s:]+)?(SOFT_FAILURE)\b',       "SOFT_FAILURE"),
        (r'(tc_[\w\-]+)[\s:\-]+(?:status[\s:]+)?(ERROR)\b',              "ERROR"),
        (r'execution_status[\s:]+(\w+).*?(tc_[\w\-]+)',                   None),
        (r'test[\s_]case[\s]+(tc_[\w\-]+).*?\b(PASSED|FAILED|SOFT_FAILURE|ERROR)\b', None),
    ]

    # Patterns that indicate a test case is starting
    _RUNNING_PATTERNS = [
        r'[Ee]xecut\w+\s+(tc_[\w\-]+)',
        r'[Rr]unning\s+(tc_[\w\-]+)',
        r'[Pp]rocess\w+\s+(tc_[\w\-]+)',
        r'[Qq]uer\w+\s+(tc_[\w\-]+)',
    ]

    _STATUS_NORM = {"PASS": "PASSED", "FAIL": "FAILED"}

    def __init__(self, total: int, descriptions: Dict[str, str] = None):
        self.total        = total
        self.descriptions = {k.lower(): v for k, v in (descriptions or {}).items()}
        self.completed    = {}   # {tc_id: status}
        self._current     = None
        self._printed     = set()
        plural = "s" if total != 1 else ""
        print(f"\n  Executing {total} test case{plural}...\n")

    def process_event(self, text: str) -> None:
        """
        Parse one streamed text fragment.
        Prints a ⏳ spinner when a test case starts running,
        then overwrites with ✅/❌ when its result arrives.
        """
        if not text or not text.strip():
            return

        # ── Check for running indicator ───────────────────────────────────────
        for pattern in self._RUNNING_PATTERNS:
            m = re.search(pattern, text, re.IGNORECASE)
            if m:
                tc_id = m.group(1).lower()
                if tc_id not in self.completed and tc_id != self._current:
                    self._current = tc_id
                    desc = self.descriptions.get(tc_id, "")[:45]
                    idx  = len(self.completed) + 1
                    print(
                        f"  [{idx:2d}/{self.total}] {tc_id:12s}  {desc:<45}  ⏳",
                        end="\r", flush=True
                    )
                break

        # ── Check for completion status ───────────────────────────────────────
        for pattern, fixed_status in self._STATUS_PATTERNS:
            m = re.search(pattern, text, re.IGNORECASE)
            if not m:
                continue

            if fixed_status:
                tc_id  = m.group(1).lower()
                status = fixed_status
            else:
                groups = [g for g in m.groups() if g]
                tc_ids = [g for g in groups if g.lower().startswith("tc_")]
                stats  = [g for g in groups
                          if g.upper() in ("PASSED", "FAILED", "SOFT_FAILURE",
                                           "ERROR", "PASS", "FAIL")]
                if not tc_ids or not stats:
                    continue
                tc_id  = tc_ids[0].lower()
                status = stats[0].upper()

            status = self._STATUS_NORM.get(status, status)

            if tc_id in self._printed:
                continue

            self._printed.add(tc_id)
            self.completed[tc_id] = status

            desc = self.descriptions.get(tc_id, "")[:45]
            idx  = len(self.completed)
            icon = (TICK if status == "PASSED"
                    else WARN if status == "SOFT_FAILURE"
                    else CROSS)

            # Extract failure detail from the same event fragment
            detail = ""
            if status in ("FAILED", "SOFT_FAILURE"):
                exp_m = re.search(r'expected[\s:]+([^\|,\n]{1,40})', text, re.IGNORECASE)
                act_m = re.search(r'actual[\s:]+([^\|,\n]{1,40})',   text, re.IGNORECASE)
                if exp_m or act_m:
                    exp    = exp_m.group(1).strip() if exp_m else "?"
                    act    = act_m.group(1).strip() if act_m else "?"
                    detail = f"\n           └─ Expected: {exp}  |  Actual: {act}"

            # Overwrite spinner line with final status
            print(
                f"  [{idx:2d}/{self.total}] {tc_id:12s}  {desc:<45}  {icon} {status}"
                + detail
            )
            return

    def print_divider(self) -> None:
        """Print a summary line after all events are processed."""
        done   = len(self.completed)
        passed = sum(1 for s in self.completed.values() if s == "PASSED")
        failed = done - passed
        print(f"\n  {SUBDIV}")
        print(f"  Completed {done}/{self.total}  |  {TICK} {passed} passed  |  {CROSS} {failed} failed")
        print(f"  {SUBDIV}\n")


# ─────────────────────────────────────────────────────────────────────────────
# Final execution report
# ─────────────────────────────────────────────────────────────────────────────


def print_execution_summary(raw: Union[str, dict]) -> None:
    """
    Print a lightweight pass/fail summary — no per-test-case detail.
    Used for menu option 2.
    """
    data       = _safe_parse(raw)
    test_cases = []
    summary    = {}

    if isinstance(data, dict):
        test_cases = data.get("test_cases", [])
        summary    = data.get("execution_summary", data.get("summary", {}))
    elif isinstance(data, list):
        test_cases = data

    total  = summary.get("total_test_cases_executed",
             summary.get("total", len(test_cases)))
    passed = summary.get("total_passed",
             summary.get("passed",
             sum(1 for tc in test_cases
                 if str(tc.get("execution_status","")).upper() == "PASSED")))
    failed = total - passed
    pass_rate = round((passed / total * 100), 1) if total > 0 else 0

    bar_len = 30
    filled  = round((passed / total) * bar_len) if total > 0 else 0
    bar     = ("█" * filled) + ("░" * (bar_len - filled))

    print(f"\n{DIVIDER}")
    print("  EXECUTION SUMMARY")
    print(DIVIDER)
    print(f"\n  [{bar}] {pass_rate:.1f}% passed\n")
    print(f"  Total        : {total}")
    print(f"  {TICK} Passed : {passed}")
    print(f"  {CROSS} Failed : {failed}")

    # Just list failed test case IDs — no detail
    failed_tcs = [
        tc for tc in test_cases
        if str(tc.get("execution_status","")).upper()
        in ("FAILED", "SOFT_FAILURE", "ERROR")
    ]
    if failed_tcs:
        print(f"\n  Failed test cases:")
        for tc in failed_tcs:
            tc_id  = tc.get("test_case_id", "?")
            status = str(tc.get("execution_status","")).upper()
            icon   = WARN if status == "SOFT_FAILURE" else CROSS
            desc   = (tc.get("description") or tc.get("test_case_name") or "")[:50]
            print(f"    {icon}  {tc_id:12s}  {desc}")

    print(f"\n{DIVIDER}\n")

def print_execution_report(raw: Union[str, dict]) -> None:
    """
    Print the final structured execution report.

    Handles the ExecuteBQSQLAgent output format (output_key="execution_report_yaml"):
    - JSON: {"test_cases": [...with execution_status/actual_values/expected_value...],
             "execution_summary": {"total_test_cases_executed": N, ...}}
    - Plain text fallback: regex-parses YAML-like agent output
    """
    # ── Try structured JSON ───────────────────────────────────────────────────
    data       = _safe_parse(raw)
    test_cases = []
    summary    = {}

    if isinstance(data, dict):
        test_cases = data.get("test_cases", [])
        summary    = data.get("execution_summary", data.get("summary", {}))
    elif isinstance(data, list):
        test_cases = data

    if test_cases:
        passed_list = [tc for tc in test_cases
                       if str(tc.get("execution_status", "")).upper() == "PASSED"]
        failed_list = [tc for tc in test_cases
                       if str(tc.get("execution_status", "")).upper()
                       in ("FAILED", "SOFT_FAILURE", "ERROR")]

        total     = summary.get("total_test_cases_executed",
                    summary.get("total", len(test_cases)))
        passed    = summary.get("total_passed", summary.get("passed", len(passed_list)))
        failed    = summary.get("total_failed", summary.get("failed", len(failed_list)))
        pass_rate = round((passed / total * 100), 1) if total > 0 else 0

        bar_len = 30
        filled  = round((passed / total) * bar_len) if total > 0 else 0
        bar     = ("█" * filled) + ("░" * (bar_len - filled))

        print(f"\n{DIVIDER}")
        print("  QA EXECUTION REPORT — FINAL SUMMARY")
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
                desc  = (tc.get("description") or tc.get("test_case_name") or "")[:48]
                print(f"  {TICK}  {tc_id:12s}  {desc}")

        # Failed (detailed)
        if failed_list:
            print(f"\n{SUBDIV}")
            print(f"  {CROSS}  FAILED ({len(failed_list)})\n")
            for tc in failed_list:
                tc_id   = tc.get("test_case_id", "?")
                desc    = tc.get("description") or tc.get("test_case_name") or ""
                status  = str(tc.get("execution_status", "FAILED")).upper()
                reason  = tc.get("failure_reason", "")
                actual  = tc.get("actual_values", "")
                expected= tc.get("expected_value", "")
                icon    = WARN if status == "SOFT_FAILURE" else CROSS

                print(f"  {icon}  {tc_id} — {desc}")
                if status == "SOFT_FAILURE":
                    print(f"       Status   : SOFT FAILURE (missing rows)")
                if reason:
                    print(f"       Reason   : {reason}")
                if expected:
                    print(f"       Expected : {expected}")
                if actual:
                    print(f"       Actual   : {actual}")
                samples = tc.get("sample_values") or tc.get("diagnostic_rows")
                if samples and isinstance(samples, list):
                    print(f"       Sample mismatches:")
                    print(_fmt_table(samples[:2]))
                print()

        print(f"\n{DIVIDER}\n")
        return

    # ── Fallback: parse plain text / YAML-like agent output ───────────────────
    raw_str = str(raw).strip()
    if not raw_str:
        print(f"\n{WARN} No executor output received.")
        return

    print(f"\n{DIVIDER}")
    print("  QA EXECUTION REPORT — FINAL SUMMARY")
    print(DIVIDER)

    # Extract summary counts
    total_m  = re.search(r'total[\s_]test[\s_]cases[\s_]executed[\s:]+(\d+)', raw_str, re.IGNORECASE)
    passed_m = re.search(r'total[\s_]passed[\s:]+(\d+)',                       raw_str, re.IGNORECASE)
    failed_m = re.search(r'total[\s_]failed[\s:]+(\d+)',                       raw_str, re.IGNORECASE)

    if total_m or passed_m or failed_m:
        total  = int(total_m.group(1))  if total_m  else "?"
        passed = int(passed_m.group(1)) if passed_m else "?"
        failed = int(failed_m.group(1)) if failed_m else "?"
        if isinstance(total, int) and isinstance(passed, int) and total > 0:
            pass_rate = round((passed / total * 100), 1)
            bar_len   = 30
            filled    = round((passed / total) * bar_len)
            bar       = ("█" * filled) + ("░" * (bar_len - filled))
            print(f"\n  [{bar}] {pass_rate:.1f}% passed\n")
        print(f"  Total        : {total}")
        print(f"  {TICK} Passed : {passed}")
        print(f"  {CROSS} Failed : {failed}")

    # Extract individual test case blocks
    tc_blocks = re.split(r'(?=(?:-\s+)?test_case_id\s*:)', raw_str)
    passed_tcs = []
    failed_tcs = []

    for block in tc_blocks:
        if not block.strip():
            continue
        tc_id_m  = re.search(r'test_case_id[\s]*:[\s]*([\w\-]+)',        block, re.IGNORECASE)
        status_m = re.search(r'execution_status[\s]*:[\s]*(\w+)',         block, re.IGNORECASE)
        reason_m = re.search(r'failure_reason[\s]*:[\s]*(.+?)(?:\n|$)',   block, re.IGNORECASE)
        actual_m = re.search(r'actual_values[\s]*:[\s]*(.+?)(?:\n|$)',    block, re.IGNORECASE)
        expect_m = re.search(r'expected_value[\s]*:[\s]*(.+?)(?:\n|$)',   block, re.IGNORECASE)

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

    if not passed_tcs and not failed_tcs and not total_m:
        clean = raw_str.replace("\\n", "\n").replace("\\t", "  ")
        print(f"\n{clean}")

    print(f"\n{DIVIDER}\n")
