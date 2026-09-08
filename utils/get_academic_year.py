import asyncio
import ssl
import time
from typing import Callable, Optional

from bs4 import BeautifulSoup
from tqdm import tqdm
from tqdm.asyncio import tqdm as tqdm_async
import aiohttp

from utils.integrity import CrawlReport, ParseCollector
from utils.page_validation import (
    is_valid_course_page,
    parse_expected_total,
    parse_total_pages,
    select_invalid_pages,
)
from utils.parse_info import parse_course_info
from utils.parse_valid_code import parse_valid_code

BASEURL = "https://selcrs.nsysu.edu.tw/menu1"
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
}
MAX_CONCURRENT_REQUESTS = 2  # Limit concurrent connections
MAX_RETRIES = 5  # Maximum retry attempts
MAX_RESCAN_ROUNDS = 3  # Rounds of re-fetching pages the server rejected


async def fetch(
    s: aiohttp.ClientSession,
    code: str,
    academic_year: str,
    index: int = 1,
    *,
    callback: Optional[Callable[[], None]] = None,
    semaphore: Optional[asyncio.Semaphore] = None,
) -> str:
    """
    Fetch the data with retry logic

    Args:
        s (aiohttp.ClientSession): The session
        code (str): The valid code
        academic_year (str): The academic year
        index (int): The index
        callback (Optional[Callable[[], None]]): The callback function
        semaphore (Optional[asyncio.Semaphore]): Semaphore to limit concurrent requests

    Returns:
        str: The response
    """
    for attempt in range(MAX_RETRIES):
        try:
            if semaphore:
                async with semaphore:
                    async with s.post(
                        f"{BASEURL}/dplycourse.asp?page={index}",
                        data={
                            "HIS": "",
                            "IDNO": "",
                            "ITEM": "",
                            "D0": academic_year,
                            "DEG_COD": "*",
                            "D1": "",
                            "D2": "",
                            "CLASS_COD": "",
                            "SECT_COD": "",
                            "TYP": "1",
                            "SDG_COD": "",
                            "teacher": "",
                            "crsname": "",
                            "T3": "",
                            "WKDAY": "",
                            "SECT": "",
                            "nowhis": "1",
                            "ValidCode": code,
                        },
                    ) as resp:
                        # Upstream sends `Content-Type: text/html` with no charset
                        # parameter, so aiohttp falls back to charset auto-detection.
                        # It has been observed guessing `ptcp154` (Kazakh Cyrillic)
                        # for some pages, mojibaking every Chinese string on them and
                        # previously causing those rows to be silently discarded.
                        # Force UTF-8, which is what the server actually sends.
                        result = await resp.text(encoding="utf-8")
                        if callback is not None:
                            callback()
                        return result
            else:
                async with s.post(
                    f"{BASEURL}/dplycourse.asp?page={index}",
                    data={
                        "HIS": "",
                        "IDNO": "",
                        "ITEM": "",
                        "D0": academic_year,
                        "DEG_COD": "*",
                        "D1": "",
                        "D2": "",
                        "CLASS_COD": "",
                        "SECT_COD": "",
                        "TYP": "1",
                        "SDG_COD": "",
                        "teacher": "",
                        "crsname": "",
                        "T3": "",
                        "WKDAY": "",
                        "SECT": "",
                        "nowhis": "1",
                        "ValidCode": code,
                    },
                ) as resp:
                    result = await resp.text(encoding="utf-8")
                    if callback is not None:
                        callback()
                    return result
        except (aiohttp.ClientOSError, aiohttp.ServerTimeoutError, asyncio.TimeoutError) as e:
            if attempt < MAX_RETRIES - 1:
                wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s, 8s, 16s
                await asyncio.sleep(wait_time)
            else:
                raise  # Re-raise on final attempt


async def get_academic_year(
    academic_year: Optional[str] = None,
    *,
    max_page: Optional[int] = None,
) -> tuple[list, str, CrawlReport]:
    """
    fetch the academic year all data

    Args:
        academic_year (Optional[str], optional): The academic year. Defaults to None.
        max_page (Optional[int], optional): The maximum page. Defaults to None.

    Raises:
        ValueError: No data (academic_year)
        ValueError: Max page is 0

    Returns:
        tuple[list, str, CrawlReport]: The courses, the academic year, and
            what the crawl observed.
    """
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    ctx.options |= 0x4  # OP_LEGACY_SERVER_CONNECT
    conn = aiohttp.TCPConnector(ssl=ctx)
    timeout = aiohttp.ClientTimeout(total=600, connect=60, sock_read=60)
    async with aiohttp.ClientSession(connector=conn, headers=DEFAULT_HEADERS, timeout=timeout) as s:
        out = await s.get(f"{BASEURL}/qrycourse.asp?HIS=2")

        if academic_year is None:
            out = await out.text(encoding="utf-8")
            soup = BeautifulSoup(out, "html.parser")

            if data := soup.select_one("#YRSM > option[value]:not([value=''])"):
                academic_year = data.attrs["value"]
            if academic_year is None:
                raise ValueError("No data (academic_year)")
            print("Current crawl:", academic_year)

        # try to get verification code
        while True:
            out = await s.get(f"{BASEURL}/validcode.asp?epoch={time.time()}")
            code = parse_valid_code(await out.read())
            out = await fetch(s, code, academic_year)
            print("Validation Code:", code)
            if "Wrong Validation Code" in out:
                print("Wrong Validation Code")
            else:
                break

        # Get the total number of pages
        if max_page is None:
            out = await fetch(s, code, academic_year)
            max_page = parse_total_pages(out)
            if max_page is None:
                raise ValueError("Could not determine the page count")

        if max_page == 0:
            raise ValueError("Max page is 0")

        # Create semaphore to limit concurrent requests
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

        # Phase 1: fetch every page in parallel
        tasks = [fetch(s, code, academic_year, i, semaphore=semaphore) for i in range(1, max_page + 1)]
        try:
            fetched = list(await tqdm_async.gather(*tasks, desc="Fetching data", unit="page"))
        except Exception as e:
            print(f"\nError during fetching: {e}")
            raise

        pages_by_number = {i + 1: page for i, page in enumerate(fetched)}

        # Phase 2: the server answers rejected requests with HTTP 200, so
        # re-fetch anything that is not a real listing page, with a fresh
        # validation code each round. Serial: there are few of these.
        rescan_rounds = 0
        invalid = select_invalid_pages(pages_by_number)
        while invalid and rescan_rounds < MAX_RESCAN_ROUNDS:
            rescan_rounds += 1
            print(f"Rescan round {rescan_rounds}: {len(invalid)} page(s) rejected: {invalid}")

            out = await s.get(f"{BASEURL}/validcode.asp?epoch={time.time()}")
            code = parse_valid_code(await out.read())
            print("Validation Code:", code)

            for number in invalid:
                try:
                    pages_by_number[number] = await fetch(s, code, academic_year, number)
                except Exception as e:  # noqa: BLE001 - a failed re-fetch must not abort the crawl
                    print(f"Could not re-fetch page {number}: {e}")

            invalid = select_invalid_pages(pages_by_number)

        lost_pages = invalid
        if lost_pages:
            print(f"WARNING: {len(lost_pages)} page(s) unrecoverable: {lost_pages}")

        # Re-read the declared total with a fresh request now that the crawl
        # has finished, so it reflects the catalogue at the END. Courses can
        # be added while 141 pages are being fetched; comparing against a
        # start-of-crawl number would raise a false shortfall.
        expected_total = None
        try:
            final = await fetch(s, code, academic_year, 1)
            if is_valid_course_page(final):
                expected_total = parse_expected_total(final)
        except Exception as e:  # noqa: BLE001 - a missing total is not fatal
            print(f"Could not re-read the declared total: {e}")

        if expected_total is None:
            # Fall back to whatever a successfully fetched page declared.
            for number in sorted(pages_by_number):
                if is_valid_course_page(pages_by_number[number]):
                    expected_total = parse_expected_total(pages_by_number[number])
                    break

    collector = ParseCollector()
    result = []
    for number in sorted(pages_by_number):
        page_html = pages_by_number[number]
        if not is_valid_course_page(page_html):
            continue

        html = BeautifulSoup(str(page_html), "html.parser")
        data = html.select("table tr[bgcolor]")
        result.extend(
            filter(
                bool,
                map(
                    lambda d: parse_course_info(
                        d, page_html, collector=collector, page=number
                    ),
                    data,
                ),
            )
        )

    crawl_report = CrawlReport(
        total_pages=max_page,
        expected_total=expected_total,
        lost_pages=lost_pages,
        rescan_rounds=rescan_rounds,
        collector=collector,
    )

    return list(filter(bool, result)), academic_year, crawl_report
