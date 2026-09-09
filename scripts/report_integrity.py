"""Turn the crawl integrity ledger into a single long-lived GitHub issue.

Reads integrity-report.json (written by scripts/API_generation.py) and
creates, updates, comments on or closes one tracking issue. Never fails
the workflow: publishing course data matters more than alerting about it.
"""
from dataclasses import dataclass
from enum import Enum
import json
import os
from pathlib import Path
import re
from typing import Optional

import requests

REPORT_PATH = Path("integrity-report.json")
LABEL = "data-integrity"
SIGNATURE_PATTERN = re.compile(r"<!-- integrity-signature: ([0-9a-f]+) -->")
API_ROOT = "https://api.github.com"

# GitHub rejects an issue body over 65536 characters with a 422. A schema
# change touching one field on every course would render ~2810 rows (~155KB),
# so the whole alert would be lost exactly when something catastrophic
# happened. The full detail is published as integrity.json alongside the data.
MAX_TABLE_ROWS = 50


class Action(Enum):
    NOOP = "noop"
    CREATE = "create"
    UPDATE_BODY = "update_body"
    UPDATE_AND_COMMENT = "update_and_comment"
    CLOSE = "close"


@dataclass(frozen=True)
class ExistingIssue:
    number: int
    signature: Optional[str]


def decide_action(report: dict, existing: Optional[ExistingIssue]) -> Action:
    """Choose what to do with the tracking issue.

    Updating the body without commenting is what keeps an hourly schedule
    from sending a notification every hour for an unchanged problem.
    """
    if report["complete"]:
        return Action.CLOSE if existing else Action.NOOP

    if existing is None:
        return Action.CREATE

    if existing.signature == report["signature"]:
        return Action.UPDATE_BODY

    return Action.UPDATE_AND_COMMENT


def extract_signature(body: str) -> Optional[str]:
    match = SIGNATURE_PATTERN.search(body or "")
    return match.group(1) if match else None


def build_issue_title(report: dict) -> str:
    """Name the anomaly categories that are actually present.

    Drift or parse failures alone set `complete = False` with `missing`
    still 0, so a fixed "缺少 N 筆課程" title would read «缺少 0 筆課程» —
    it looks like a bug in the alerter and invites dismissal of the very
    signal that caught the incident.
    """
    parts = []
    if report["missing"]:
        parts.append(f"缺少 {report['missing']} 筆課程")
    if report["schema_drift"]:
        parts.append(f"{len(report['schema_drift'])} 筆未知欄位值")
    if report["parse_failures"]:
        parts.append(f"{len(report['parse_failures'])} 筆解析失敗")

    prefix = f"[Data] {report['academic_year']} 資料不完整"
    if not parts:
        # Incomplete for a reason with no count of its own (lost pages while
        # the declared total is unknown). The body carries the detail.
        return prefix

    return f"{prefix}：{'、'.join(parts)}"


def _cell(value) -> str:
    """Make an arbitrary upstream value safe inside a markdown table cell.

    `parse_failures.reason` is `str(AssertionError)` and embeds raw upstream
    values, so an unescaped `|` or a newline would break the table apart.
    """
    text = str(value).replace("\r\n", " ").replace("\r", " ").replace("\n", " ")
    return text.replace("|", r"\|")


def _overflow_note(rows: list) -> list[str]:
    """The '…another N' line for a table truncated at MAX_TABLE_ROWS."""
    hidden = len(rows) - MAX_TABLE_ROWS
    return [f"…另有 {hidden} 筆，詳見 integrity.json"] if hidden > 0 else []


def render_issue_body(report: dict) -> str:
    lines = [
        "## 資料完整性警報",
        "",
        "爬蟲本次產出的資料不完整。**資料仍已發布** —— 缺漏的課程是少數，"
        "停止發布會影響全部使用者。",
        "",
        "| 項目 | 數值 |",
        "| --- | --- |",
        f"| 學年期 | `{_cell(report['academic_year'])}` |",
        f"| 檢查時間 | {_cell(report['checked_at'])} |",
        f"| 官方宣告總數 | {_cell(report['expected_total'])} |",
        f"| 實際發布筆數 | {_cell(report['actual_total'])} |",
        f"| 缺漏 | **{_cell(report['missing'])}** |",
        f"| 上游總頁數 | {_cell(report['total_pages'])} |",
        f"| 重掃輪數 | {_cell(report['rescan_rounds'])} |",
        "",
    ]

    if report["lost_pages"]:
        lines += [
            "### 重試後仍無法取得的頁面",
            "",
            f"`{report['lost_pages']}`",
            "",
            "每頁 20 筆。上游對被拒絕的請求會回傳 HTTP 200 + "
            "`Wrong Validation Code`，因此這些頁面沒有觸發任何錯誤。",
            "",
        ]

    if report["schema_drift"]:
        lines += ["### 未知的欄位值（課程已保留）", "", "| 欄位 | 值 | 課號 | 系所 |", "| --- | --- | --- | --- |"]
        for drift in report["schema_drift"][:MAX_TABLE_ROWS]:
            lines.append(
                f"| `{_cell(drift['field'])}` | `{_cell(drift['value'])}` | "
                f"{_cell(drift['course_id'])} | {_cell(drift['department'])} |"
            )
        lines += _overflow_note(report["schema_drift"])
        lines.append("")

    if report["parse_failures"]:
        lines += ["### 解析失敗（課程已遺失）", "", "| 頁 | 原因 |", "| --- | --- |"]
        for failure in report["parse_failures"][:MAX_TABLE_ROWS]:
            lines.append(f"| {_cell(failure['page'])} | `{_cell(failure['reason'])}` |")
        lines += _overflow_note(report["parse_failures"])
        lines.append("")

    lines += [
        "---",
        "",
        "此 issue 由 `scripts/report_integrity.py` 自動維護：狀態未變化時只更新內文，"
        "資料恢復完整時自動關閉。",
        "",
        f"<!-- integrity-signature: {report['signature']} -->",
    ]
    return "\n".join(lines)


def _session(token: str) -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
    )
    return session


def _find_existing(session: requests.Session, repo: str) -> Optional[ExistingIssue]:
    response = session.get(
        f"{API_ROOT}/repos/{repo}/issues",
        params={"labels": LABEL, "state": "open", "sort": "created", "direction": "desc"},
        timeout=30,
    )
    response.raise_for_status()
    issues = [item for item in response.json() if "pull_request" not in item]
    if not issues:
        return None

    if len(issues) > 1:
        print(f"Note: {len(issues)} open '{LABEL}' issues; using #{issues[0]['number']}")

    return ExistingIssue(issues[0]["number"], extract_signature(issues[0].get("body") or ""))


def _ensure_label(session: requests.Session, repo: str) -> None:
    response = session.get(f"{API_ROOT}/repos/{repo}/labels/{LABEL}", timeout=30)
    if response.status_code != 404:
        return

    created = session.post(
        f"{API_ROOT}/repos/{repo}/labels",
        json={"name": LABEL, "color": "d73a4a", "description": "爬蟲資料完整性警報"},
        timeout=30,
    )
    # Report the failure but do not raise: the caller is about to create the
    # issue, and GitHub accepts labels on issue creation. Aborting here would
    # turn a cosmetic label problem into a lost alert.
    if not created.ok:
        print(f"Could not create the {LABEL!r} label ({created.status_code}); creating the issue anyway.")


def main() -> None:
    if not REPORT_PATH.is_file():
        print(f"{REPORT_PATH} not found; nothing to report.")
        return

    token = os.getenv("GITHUB_TOKEN")
    repo = os.getenv("GITHUB_REPOSITORY")
    if not token or not repo:
        print("GITHUB_TOKEN or GITHUB_REPOSITORY unset; skipping.")
        return

    report = json.loads(REPORT_PATH.read_text(encoding="utf-8"))
    session = _session(token)

    existing = _find_existing(session, repo)
    action = decide_action(report, existing)
    print(f"Integrity action: {action.value}")

    if action is Action.NOOP:
        return

    body = render_issue_body(report)
    title = build_issue_title(report)

    if action is Action.CREATE:
        _ensure_label(session, repo)
        session.post(
            f"{API_ROOT}/repos/{repo}/issues",
            json={"title": title, "body": body, "labels": [LABEL]},
            timeout=30,
        ).raise_for_status()
        return

    assert existing is not None
    issue_url = f"{API_ROOT}/repos/{repo}/issues/{existing.number}"

    if action is Action.CLOSE:
        session.post(
            f"{issue_url}/comments",
            json={
                "body": "✅ 最新一次爬取的資料已完整，自動關閉。\n\n"
                f"檢查時間：{report['checked_at']}",
            },
            timeout=30,
        )
        session.patch(issue_url, json={"state": "closed"}, timeout=30).raise_for_status()
        return

    # Comment BEFORE the body PATCH. The PATCH persists the new signature, so
    # a comment that failed after it would be lost for good: the next hourly
    # run would compare equal signatures and decide UPDATE_BODY, and the
    # "state changed" notification would never be sent. Commenting first
    # leaves the old signature in place, so a failure here simply means the
    # next run retries the whole notification.
    if action is Action.UPDATE_AND_COMMENT:
        session.post(
            f"{issue_url}/comments",
            json={
                "body": "⚠️ 缺漏狀態有變化。\n\n"
                f"- 缺漏筆數：{report['missing']}\n"
                f"- 無法取得的頁面：`{report['lost_pages']}`\n"
                f"- 檢查時間：{report['checked_at']}",
            },
            timeout=30,
        ).raise_for_status()

    session.patch(issue_url, json={"title": title, "body": body}, timeout=30).raise_for_status()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001 - alerting must never fail the run
        print(f"Integrity reporting failed (data publishing is unaffected): {exc}")
