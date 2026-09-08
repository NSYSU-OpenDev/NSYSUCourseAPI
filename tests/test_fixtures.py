from tests.conftest import fixture_text


def test_valid_page_fixture_has_pagination_footer():
    html = fixture_text("valid_page.html")
    assert "Showing page 1 of 141 pages" in html
    assert "共 2810 筆" in html


def test_wrong_validation_code_fixture_is_the_rejection_page():
    html = fixture_text("wrong_validation_code.html")
    assert "Wrong Validation Code" in html
    assert len(html) < 500


def test_change_stop_fixture_contains_the_drifted_row():
    html = fixture_text("page_with_change_stop.html")
    assert "GEAI1854" in html
    assert "停開" in html
