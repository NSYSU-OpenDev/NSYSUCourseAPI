from bs4 import BeautifulSoup

from tests.conftest import fixture_text
from utils.integrity import ParseCollector
from utils.parse_info import parse_course_info


def _rows(fixture_name):
    html = BeautifulSoup(fixture_text(fixture_name), "html.parser")
    return html.select("table tr[bgcolor]")


def _parse_all(fixture_name, collector=None):
    page_html = fixture_text(fixture_name)
    return [
        parse_course_info(row, page_html, collector=collector, page=13)
        for row in _rows(fixture_name)
    ]


def test_change_stop_course_is_kept():
    collector = ParseCollector()
    courses = [c for c in _parse_all("page_with_change_stop.html", collector) if c]
    kept = [c for c in courses if c["id"] == "GEAI1854"]
    assert len(kept) == 1
    assert kept[0]["change"] == "停開"


def test_change_stop_is_not_recorded_as_drift_once_known():
    collector = ParseCollector()
    _parse_all("page_with_change_stop.html", collector)
    assert [d for d in collector.drifts if d.value == "停開"] == []


def test_every_row_in_the_fixture_now_parses():
    collector = ParseCollector()
    results = _parse_all("page_with_change_stop.html", collector)
    assert len(results) == 20
    assert all(r for r in results)
    assert collector.failures == []


def test_unknown_enum_value_keeps_the_course_and_records_drift(monkeypatch):
    """Simulate a value this parser has never seen by removing 停開 from the
    known set. Mutating the fixture's HTML instead would be unreliable:
    upstream's <tr> tags are unclosed, so the first row's cells nest every
    later row's cells."""
    import utils.parse_info as parse_info

    monkeypatch.setattr(parse_info, "KNOWN_CHANGE", {"", "異動", "新增"})

    collector = ParseCollector()
    courses = [c for c in _parse_all("page_with_change_stop.html", collector) if c]

    kept = [c for c in courses if c["id"] == "GEAI1854"]
    assert len(kept) == 1, "the course must survive an unknown enum value"
    assert kept[0]["change"] == "停開", "the raw value passes through unchanged"
    assert any(
        d.field == "change" and d.value == "停開" and d.course_id == "GEAI1854"
        for d in collector.drifts
    )


def test_structural_failure_still_discards_the_row():
    collector = ParseCollector()
    broken = BeautifulSoup(
        "<table><tr bgcolor='#ffffff'><td>only</td><td>two</td></tr></table>",
        "html.parser",
    ).select_one("tr")
    assert parse_course_info(broken, "<page/>", collector=collector, page=7) is False
    assert collector.failures[0].page == 7


def test_output_shape_is_unchanged_for_a_normal_course():
    courses = [c for c in _parse_all("valid_page.html") if c]
    assert courses, "fixture should contain courses"
    expected_keys = {
        "url", "change", "changeDescription", "multipleCompulsory", "department",
        "id", "grade", "class", "name", "credit", "yearSemester", "compulsory",
        "restrict", "select", "selected", "remaining", "teacher", "room",
        "classTime", "description", "tags", "english",
    }
    assert set(courses[0].keys()) == expected_keys
    assert isinstance(courses[0]["compulsory"], bool)
    assert isinstance(courses[0]["restrict"], int)
    assert len(courses[0]["classTime"]) == 7


def test_collector_is_optional():
    results = _parse_all("valid_page.html")
    assert any(r for r in results)
