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


def render_issue_body(report: dict) -> str:
    lines = [
        "## 資料完整性警報",
        "",
        "爬蟲本次產出的資料不完整。**資料仍已發布** —— 缺漏的課程是少數，"
        "停止發布會影響全部使用者。",
        "",
        "| 項目 | 數值 |",
        "| --- | --- |",
        f"| 學年期 | `{report['academic_year']}` |",
        f"| 檢查時間 | {report['checked_at']} |",
        f"| 官方宣告總數 | {report['expected_total']} |",
        f"| 實際發布筆數 | {report['actual_total']} |",
        f"| 缺漏 | **{report['missing']}** |",
        f"| 上游總頁數 | {report['total_pages']} |",
        f"| 重掃輪數 | {report['rescan_rounds']} |",
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
        for drift in report["schema_drift"]:
            lines.append(
                f"| `{drift['field']}` | `{drift['value']}` | "
                f"{drift['course_id']} | {drift['department']} |"
            )
        lines.append("")

    if report["parse_failures"]:
        lines += ["### 解析失敗（課程已遺失）", "", "| 頁 | 原因 |", "| --- | --- |"]
        for failure in report["parse_failures"]:
            lines.append(f"| {failure['page']} | `{failure['reason']}` |")
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
    if response.status_code == 404:
        session.post(
            f"{API_ROOT}/repos/{repo}/labels",
            json={"name": LABEL, "color": "d73a4a", "description": "爬蟲資料完整性警報"},
            timeout=30,
        )


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
    title = (
        f"[Data] {report['academic_year']} 資料不完整："
        f"缺少 {report['missing']} 筆課程"
    )

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

    session.patch(issue_url, json={"title": title, "body": body}, timeout=30).raise_for_status()

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
        )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001 - alerting must never fail the run
        print(f"Integrity reporting failed (data publishing is unaffected): {exc}")
