import re
import ssl
import time
from typing import Callable, Optional

from bs4 import BeautifulSoup
from tqdm import tqdm
from tqdm.asyncio import tqdm as tqdm_async
import aiohttp

from utils.parse_info import parse_course_info
from utils.parse_valid_code import parse_valid_code

BASEURL = "https://selcrs.nsysu.edu.tw/menu1"
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
}


async def fetch(
    s: aiohttp.ClientSession,
    code: str,
    academic_year: str,
    index: int = 1,
    *,
    callback: Optional[Callable[[], None]] = None,
) -> str:
    """
    Fetch the data

    Args:
        s (aiohttp.ClientSession): The session
        code (str): The valid code
        academic_year (str): The academic year
        index (int): The index
        callback (Optional[Callable[[], None]]): The callback function

    Returns:
        Coroutine[Any, Any, str]: The response
    """
    try:
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
            result = await resp.text()
            if callback is not None:
                callback()
            return result
    except aiohttp.ClientOSError:
        return await fetch(s, code, academic_year, index, callback=callback)


async def get_academic_year(
    academic_year: Optional[str] = None,
    *,
    max_page: Optional[int] = None,
) -> tuple[list, str]:
    """
    fetch the academic year all data

    Args:
        academic_year (Optional[str], optional): The academic year. Defaults to None.
        max_page (Optional[int], optional): The maximum page. Defaults to None.

    Raises:
        ValueError: No data (academic_year)
        ValueError: Max page is 0

    Returns:
        tuple[list, str]: The result and the academic year
    """
    ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    ctx.options |= 0x4  # OP_LEGACY_SERVER_CONNECT
    conn = aiohttp.TCPConnector(ssl=ctx)
    async with aiohttp.ClientSession(connector=conn, headers=DEFAULT_HEADERS) as s:
        out = await s.get(f"{BASEURL}/qrycourse.asp?HIS=2")

        if academic_year is None:
            out = await out.text()
            soup = BeautifulSoup(out, "html.parser")

            if data := soup.select_one("#YRSM > option[value]:not([value=''])"):
                academic_year = data.attrs["value"]
            else:
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
            max_page = int(re.findall(r"Showing page \d+ of (\d+) pages", out)[-1])

        if max_page == 0:
            raise ValueError("Max page is 0")

        # Generate crawling tasks
        tasks = map(lambda i: fetch(s, code, academic_year, i), range(1, max_page + 1))
        pages = list(await tqdm_async.gather(*tasks, desc="Fetching data", unit="page"))

    result = []
    for page in tqdm(pages, desc="Parsing data", unit="page"):
        html = BeautifulSoup(str(page), "html.parser")
        data = html.select("table tr[bgcolor]")

        result.extend(map(lambda d: parse_course_info(d, page), data))

    return list(filter(bool, result)), academic_year
