# tests/test_page_validation.py
from tests.conftest import fixture_text
from utils.page_validation import (
    is_valid_course_page,
    parse_expected_total,
    parse_total_pages,
    select_invalid_pages,
)


def test_valid_page_is_valid():
    assert is_valid_course_page(fixture_text("valid_page.html")) is True


def test_rejection_page_is_not_valid():
    assert is_valid_course_page(fixture_text("wrong_validation_code.html")) is False


def test_empty_response_is_not_valid():
    assert is_valid_course_page("") is False


def test_page_without_pagination_footer_is_not_valid():
    assert is_valid_course_page("<html><body>maintenance</body></html>") is False


def test_a_result_page_with_no_courses_is_still_valid():
    # An empty result set is a legitimate answer; the footer is what proves
    # the server actually ran the query.
    assert is_valid_course_page("Showing page 1 of 1 pages 共 0 筆") is True


def test_parse_expected_total():
    assert parse_expected_total(fixture_text("valid_page.html")) == 2810


def test_parse_expected_total_missing_returns_none():
    assert parse_expected_total("no counts here") is None


def test_parse_total_pages():
    assert parse_total_pages(fixture_text("valid_page.html")) == 141


def test_parse_total_pages_missing_returns_none():
    assert parse_total_pages("no pagination here") is None


def test_select_invalid_pages_returns_rejected_pages():
    pages = {
        1: fixture_text("valid_page.html"),
        2: fixture_text("wrong_validation_code.html"),
        3: fixture_text("valid_page.html"),
    }
    assert select_invalid_pages(pages) == [2]


def test_select_invalid_pages_empty_when_all_valid():
    assert select_invalid_pages({1: fixture_text("valid_page.html")}) == []


def test_select_invalid_pages_treats_empty_response_as_invalid():
    pages = {1: "", 2: fixture_text("valid_page.html")}
    assert select_invalid_pages(pages) == [1]


def test_select_invalid_pages_result_is_sorted():
    bad = fixture_text("wrong_validation_code.html")
    good = fixture_text("valid_page.html")
    assert select_invalid_pages({3: bad, 1: bad, 2: good}) == [1, 3]
