# tests/test_get_academic_year_wiring.py
"""Source-text guards for utils/get_academic_year.py.

The module is read as text rather than imported: importing it reaches
utils.parse_valid_code and therefore torch, which the test suite should not
need. Compiling it keeps a syntax error from slipping past the assertions.
"""
from pathlib import Path
import re

SOURCE_PATH = Path(__file__).resolve().parents[1] / "utils" / "get_academic_year.py"
SOURCE = SOURCE_PATH.read_text(encoding="utf-8")

# Any `<something>.text(...)` response-body read, with its arguments.
TEXT_CALL = re.compile(r"(\w+)\.text\(([^)]*)\)")


def _read_page_body() -> str:
    """The text of _read_page() alone."""
    start = SOURCE.index("async def _read_page(")
    end = SOURCE.index("\nasync def ", start + 1)
    return SOURCE[start:end]


def test_module_still_compiles():
    compile(SOURCE, str(SOURCE_PATH), "exec")


def test_every_response_read_forces_utf8():
    """Guards the incident this branch exists to fix.

    Upstream sends `Content-Type: text/html` with no charset parameter, so
    aiohttp falls back to charset auto-detection and was observed guessing
    `ptcp154` (Kazakh Cyrillic). That mojibaked every Chinese string on the
    affected pages and silently dropped 60 courses from the published API
    for two days. Every response body read must therefore name the encoding
    explicitly; a refactor that drops the argument reinstates the incident.
    """
    calls = TEXT_CALL.findall(SOURCE)
    assert calls, "expected at least one response body read in the crawler"
    for receiver, args in calls:
        assert 'encoding="utf-8"' in args, (
            f'{receiver}.text({args}) must pass encoding="utf-8"; '
            "charset auto-detection is what caused the 60-course data loss"
        )


def test_an_undecodable_page_does_not_abort_the_crawl():
    """`text(encoding=...)` decodes with errors="strict", so one stray byte
    on any one of ~141 pages raises UnicodeDecodeError. That is not in
    fetch()'s caught tuple, so it would propagate through tqdm_async.gather
    and past API_generation's `except ValueError`, failing the whole run:
    one bad byte would stop publishing for everyone. It must degrade to an
    empty page, which the rescan loop and lost_pages reporting handle."""
    body = _read_page_body()
    assert "except UnicodeDecodeError" in body
    assert 'return ""' in body

