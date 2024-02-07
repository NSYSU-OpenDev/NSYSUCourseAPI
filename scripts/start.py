import os
import re
import time
import asyncio
import json
from pathlib import Path
from typing import Callable, Optional

from bs4 import BeautifulSoup, Tag
from tqdm.asyncio import tqdm as tqdm_async
from tqdm import tqdm
import aiohttp

from utils.parse_valid_code import parse_valid_code

BASEURL = "https://selcrs.nsysu.edu.tw/menu1"
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 ",
}

now = max_page = 0
academic_year = os.getenv("ACADEMIC_YEAR")


async def fetch(
    s: aiohttp.ClientSession,
    code: str,
    index: int = 1,
    *,
    callback: Optional[Callable[[], None]] = None,
):
    """
    Fetch the data
    :param s: The session (aiohttp.ClientSession)
    :param index: The index (str)
    :param code: The valid code (str)
    :return: The response (Coroutine[Any, Any, str])
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
        return await fetch(s, code, index, callback=callback)


async def main():
    global max_page, academic_year

    tasks = []
    pages = []

    async with aiohttp.ClientSession(headers=DEFAULT_HEADERS) as s:
        out = await s.get(f"{BASEURL}/qrycourse.asp?HIS=2")

        if not academic_year:
            out = await out.text()
            soup = BeautifulSoup(out, "html.parser")

            if data := soup.select_one("#YRSM > option[value]:not([value=''])"):
                academic_year = data.attrs["value"]
            else:
                print("No data (academic_year)")
                return
            print("Current crawl:", academic_year)

        while True:
            out = await s.get(f"{BASEURL}/validcode.asp?epoch={time.time()}")
            img = await out.read()
            with Path("ValidCode.png").open("wb") as f:
                f.write(img)

            code = parse_valid_code(img)
            out = await fetch(s, code)
            print("Validation Code:", code)
            if "Wrong Validation Code" in out:
                print("Wrong Validation Code")
            else:
                break

        out = await fetch(s, code)
        max_page = int(re.findall(r"Showing page \d+ of (\d+) pages", out)[-1])

        if max_page == 0:
            print("No data")
            return

        tasks.extend(map(lambda i: fetch(s, code, i), range(2, max_page + 1)))
        pages = list(await tqdm_async.gather(*tasks, desc="Fetching data", unit="page"))

    result = []
    for page in tqdm(pages, desc="Parsing data", unit="page"):
        html = BeautifulSoup(str(page), "html.parser")
        data = html.select("table tr[bgcolor]")

        for d in data:
            tags: list[str] = []
            for line_break in d.find_all("br"):
                line_break: Tag
                line_break.replace_with("\n")
            for tag in d.select_one("td:nth-child(25) font") or []:
                tags.append(tag.text)
                tag.extract()

            original_data = list(map(lambda x: x.text.strip(), d))
            (
                Change,
                ChangeDescription,
                MultipleCompulsory,
                Department,
                Number,
                Grade,
                Class,
                Name,
                Credit,
                YearSemester,
                CompulsoryElective,
                Restrict,
                Select,
                Selected,
                Remaining,
                Teacher,
                Room,
            ) = original_data[:17]
            ClassTime = original_data[17:24]
            Description = original_data[24]

            info_url_el = d.select_one("td:nth-child(8) small a[href]")
            result.append(
                {
                    "url": info_url_el.attrs["href"] if info_url_el else None,
                    "change": Change,
                    "changeDescription": ChangeDescription,
                    "multipleCompulsory": MultipleCompulsory,
                    "department": Department,
                    "number": Number,
                    "grade": Grade,
                    "class": Class,
                    "name": Name,
                    "credit": Credit,
                    "yearSemester": YearSemester,
                    "compulsoryElective": CompulsoryElective,
                    "restrict": Restrict,
                    "select": Select,
                    "selected": Selected,
                    "remaining": Remaining,
                    "teacher": Teacher,
                    "room": Room,
                    "classTime": ClassTime,
                    "description": Description,
                    "tags": tags,
                }
            )

    Path("out.json").write_text(
        json.dumps(result, ensure_ascii=False),
        encoding="utf-8",
    )


# # 異動
# Change: str
# # 異動-說明
# Description: str
# # 多門必修
# MultipleCompulsory: str
# # 系所別
# Department: str
# # 課號
# Number: str
# # 年級
# Grade: str
# # 班別
# Class: str
# # 科目名稱
# Name: str

# # 課目大綱
# Url: str

# # 學分
# Credit: str
# # 學年度
# YearSemester: str
# # 必選修
# CompulsoryElective: str
# # 限修
# Restrict: str
# # 點選
# Select: str
# # 選上
# Selected: str
# # 餘額
# Remaining: str
# # 授課教師
# Teacher: str
# # 教室
# Room: str


def start() -> None:
    asyncio.run(main())


if __name__ == "__main__":
    start()
