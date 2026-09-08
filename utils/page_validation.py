import re
from typing import Optional

REJECTION_MARKER = "Wrong Validation Code"
PAGINATION_PATTERN = re.compile(r"Showing page \d+ of (\d+) pages")
TOTAL_PATTERN = re.compile(r"共\s*(\d+)\s*筆")


def is_valid_course_page(html: str) -> bool:
    """Whether a response is a genuine course listing page.

    The upstream server answers a rejected request with HTTP 200 and a
    short "Wrong Validation Code" body, so the status code proves
    nothing. The pagination footer is the evidence that the server
    actually ran the query.

    BOTH footers are required. The pagination footer is pure ASCII, so it
    survives a charset misdecode intact — the original incident, where
    aiohttp guessed `ptcp154` and mojibaked every Chinese string on a
    page, would have sailed straight past a pagination-only check and
    published unreadable courses. `TOTAL_PATTERN` contains Chinese, so a
    misdecoded page fails to match it and becomes a rescan and, failing
    that, a `lost_pages` alert.

    Deliberately does NOT require course rows: an empty result set is a
    legitimate answer, and its page still carries both footers.
    """
    if not html or REJECTION_MARKER in html:
        return False

    return (
        PAGINATION_PATTERN.search(html) is not None
        and TOTAL_PATTERN.search(html) is not None
    )


def parse_expected_total(html: str) -> Optional[int]:
    """The total course count the server declares, or None if absent."""
    matches = TOTAL_PATTERN.findall(html)
    return int(matches[-1]) if matches else None


def parse_total_pages(html: str) -> Optional[int]:
    """The total page count the server declares, or None if absent."""
    matches = PAGINATION_PATTERN.findall(html)
    return int(matches[-1]) if matches else None


def select_invalid_pages(pages: dict[int, str]) -> list[int]:
    """Page numbers whose response is not a genuine course listing.

    The upstream server answers a rejected request with HTTP 200, so a page
    that "succeeded" can still contain no courses. Pages like these silently
    removed 60 courses from the published data.
    """
    return sorted(number for number, html in pages.items() if not is_valid_course_page(html))
