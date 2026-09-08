from scripts.report_integrity import (
    Action,
    ExistingIssue,
    build_issue_title,
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


def _drift(i):
    return {"field": "change", "value": f"值{i}", "course_id": f"C{i}", "department": "系"}


def test_title_names_the_shortfall_when_courses_are_missing():
    title = build_issue_title(_report())
    assert title == "[Data] 1151 資料不完整：缺少 60 筆課程"


def test_title_does_not_claim_zero_missing_courses_for_drift_only():
    """Drift alone sets complete=False with missing=0 — which is exactly what
    the live run produced (126 drift entries, 0 missing). A fixed title would
    read «資料不完整：缺少 0 筆課程», which looks like a bug in the alerter."""
    title = build_issue_title(
        _report(missing=0, actual_total=2810, schema_drift=[_drift(i) for i in range(126)])
    )
    assert title == "[Data] 1151 資料不完整：126 筆未知欄位值"
    assert "缺少 0" not in title


def test_title_names_parse_failures_alone():
    title = build_issue_title(
        _report(missing=0, parse_failures=[{"page": 13, "reason": "len = 4"}])
    )
    assert title == "[Data] 1151 資料不完整：1 筆解析失敗"


def test_title_joins_every_non_empty_category():
    title = build_issue_title(
        _report(
            schema_drift=[_drift(0), _drift(1)],
            parse_failures=[{"page": 13, "reason": "len = 4"}],
        )
    )
    assert title == "[Data] 1151 資料不完整：缺少 60 筆課程、2 筆未知欄位值、1 筆解析失敗"


def test_title_has_no_dangling_colon_when_nothing_is_countable():
    # Lost pages while the declared total is unknown: incomplete, but with no
    # count of its own. The body carries the detail.
    title = build_issue_title(_report(missing=0, expected_total=None))
    assert title == "[Data] 1151 資料不完整"


def test_drift_table_is_capped_and_reports_the_remainder():
    body = render_issue_body(_report(schema_drift=[_drift(i) for i in range(60)]))
    rows = [line for line in body.splitlines() if line.startswith("| `change`")]
    assert len(rows) == 50
    assert "…另有 10 筆，詳見 integrity.json" in body
    assert "值0" in body
    assert "值59" not in body


def test_parse_failure_table_is_capped_and_reports_the_remainder():
    failures = [{"page": i, "reason": f"reason {i}"} for i in range(75)]
    body = render_issue_body(_report(parse_failures=failures))
    rows = [line for line in body.splitlines() if line.startswith("| ") and "reason " in line]
    assert len(rows) == 50
    assert "…另有 25 筆，詳見 integrity.json" in body


def test_a_short_table_gets_no_remainder_note():
    body = render_issue_body(_report(schema_drift=[_drift(0)]))
    assert "另有" not in body


def test_a_catastrophic_drift_set_still_fits_githubs_body_limit():
    """A schema change touching one field on every course used to render
    ~2810 rows (~155KB); GitHub 422s over 65536 characters and the whole
    alert was lost precisely when it mattered most."""
    body = render_issue_body(_report(schema_drift=[_drift(i) for i in range(2810)]))
    assert len(body) < 65536


def test_pipes_in_a_value_are_escaped_so_the_table_survives():
    # parse_failures.reason is str(AssertionError) and embeds raw upstream
    # values, so an unescaped pipe would break the table apart.
    body = render_issue_body(
        _report(parse_failures=[{"page": 13, "reason": "len(x) = 4 | got a|b"}])
    )
    assert r"| 13 | `len(x) = 4 \| got a\|b` |" in body


def test_newlines_in_a_value_are_flattened_into_the_row():
    body = render_issue_body(_report(parse_failures=[{"page": 7, "reason": "one\ntwo"}]))
    assert "| 7 | `one two` |" in body
