from datetime import datetime, timezone

from utils.integrity import (
    CrawlReport,
    ParseCollector,
    ParseFailure,
    SchemaDrift,
    build_integrity_report,
    compute_signature,
)

AT = datetime(2026, 9, 8, 6, 45, 4, tzinfo=timezone.utc)


def _crawl_report(**kwargs):
    defaults = dict(
        total_pages=141,
        expected_total=2810,
        lost_pages=[],
        rescan_rounds=0,
        collector=ParseCollector(),
    )
    defaults.update(kwargs)
    return CrawlReport(**defaults)


def test_complete_crawl_reports_complete():
    report = build_integrity_report("1151", 2810, _crawl_report(), AT)
    assert report.missing == 0
    assert report.complete is True


def test_lost_pages_make_the_report_incomplete():
    report = build_integrity_report(
        "1151", 2750, _crawl_report(lost_pages=[47, 123, 127]), AT
    )
    assert report.missing == 60
    assert report.complete is False
    assert report.lost_pages == [47, 123, 127]


def test_more_courses_than_declared_is_not_a_shortfall():
    # Courses added mid-crawl; not a data loss.
    report = build_integrity_report("1151", 2815, _crawl_report(), AT)
    assert report.missing == 0
    assert report.complete is True


def test_unknown_expected_total_falls_back_to_lost_pages():
    clean = build_integrity_report("1151", 100, _crawl_report(expected_total=None), AT)
    assert clean.complete is True

    lossy = build_integrity_report(
        "1151", 100, _crawl_report(expected_total=None, lost_pages=[3]), AT
    )
    assert lossy.complete is False


def test_schema_drift_makes_the_report_incomplete():
    collector = ParseCollector()
    collector.add_drift("change", "停開", "GEAI1854", "跨院選修(文)")
    report = build_integrity_report("1151", 2810, _crawl_report(collector=collector), AT)
    assert report.complete is False
    assert report.schema_drift[0].value == "停開"


def test_signature_ignores_the_timestamp():
    later = datetime(2026, 9, 8, 7, 45, 4, tzinfo=timezone.utc)
    a = build_integrity_report("1151", 2750, _crawl_report(lost_pages=[47]), AT)
    b = build_integrity_report("1151", 2750, _crawl_report(lost_pages=[47]), later)
    assert a.signature == b.signature


def test_signature_changes_when_state_changes():
    a = build_integrity_report("1151", 2750, _crawl_report(lost_pages=[47]), AT)
    b = build_integrity_report("1151", 2730, _crawl_report(lost_pages=[47, 99]), AT)
    assert a.signature != b.signature


def test_signature_is_order_independent():
    assert compute_signature(
        lost_pages=[123, 47], missing=40, drifts=[], failures=[]
    ) == compute_signature(lost_pages=[47, 123], missing=40, drifts=[], failures=[])


def test_to_dict_is_json_serialisable_and_flattens_dataclasses():
    import json

    collector = ParseCollector()
    collector.add_drift("change", "停開", "GEAI1854", "跨院選修(文)")
    collector.add_failure(13, "len(original_data) = 4")
    report = build_integrity_report("1151", 2750, _crawl_report(collector=collector), AT)

    payload = json.loads(json.dumps(report.to_dict(), ensure_ascii=False))
    assert payload["academic_year"] == "1151"
    assert payload["checked_at"] == "2026-09-08T06:45:04Z"
    assert payload["schema_drift"][0]["field"] == "change"
    assert payload["parse_failures"][0]["page"] == 13
