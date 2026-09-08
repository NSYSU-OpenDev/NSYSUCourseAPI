import pytest

from scripts.report_integrity import (
    Action,
    ExistingIssue,
    decide_action,
    extract_signature,
    render_issue_body,
)


def _report(**kwargs):
    base = {
        "academic_year": "1151",
        "checked_at": "2026-09-08T06:45:04Z",
        "expected_total": 2810,
        "actual_total": 2750,
        "missing": 60,
        "complete": False,
        "total_pages": 141,
        "rescan_rounds": 3,
        "lost_pages": [47, 123, 127],
        "schema_drift": [],
        "parse_failures": [],
        "signature": "aaa",
    }
    base.update(kwargs)
    return base


def test_clean_run_with_no_issue_does_nothing():
    assert decide_action(_report(complete=True, missing=0), None) is Action.NOOP


def test_clean_run_closes_an_open_issue():
    existing = ExistingIssue(number=7, signature="aaa")
    assert decide_action(_report(complete=True, missing=0), existing) is Action.CLOSE


def test_shortfall_with_no_issue_creates_one():
    assert decide_action(_report(), None) is Action.CREATE


def test_unchanged_shortfall_updates_the_body_without_commenting():
    existing = ExistingIssue(number=7, signature="aaa")
    assert decide_action(_report(signature="aaa"), existing) is Action.UPDATE_BODY


def test_changed_shortfall_also_comments():
    existing = ExistingIssue(number=7, signature="aaa")
    assert decide_action(_report(signature="bbb"), existing) is Action.UPDATE_AND_COMMENT


def test_missing_signature_on_the_existing_issue_is_treated_as_changed():
    existing = ExistingIssue(number=7, signature=None)
    assert decide_action(_report(signature="bbb"), existing) is Action.UPDATE_AND_COMMENT


def test_body_embeds_the_signature_and_round_trips():
    body = render_issue_body(_report(signature="deadbeef"))
    assert extract_signature(body) == "deadbeef"


def test_extract_signature_returns_none_when_absent():
    assert extract_signature("no marker here") is None


def test_body_reports_the_numbers_a_maintainer_needs():
    body = render_issue_body(_report())
    assert "1151" in body
    assert "2810" in body
    assert "2750" in body
    assert "47" in body


def test_body_lists_schema_drift():
    body = render_issue_body(
        _report(
            schema_drift=[
                {
                    "field": "change",
                    "value": "停開",
                    "course_id": "GEAI1854",
                    "department": "跨院選修(文)",
                }
            ]
        )
    )
    assert "停開" in body
    assert "GEAI1854" in body
