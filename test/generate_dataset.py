import asyncio
from pathlib import Path
import shutil
import time
import aiohttp
import io

from PIL import Image

from utils.parse_valid_code import parse_valid_code

BASEURL = "https://selcrs.nsysu.edu.tw/menu1"


async def fetch(s: aiohttp.ClientSession, code: str):
    async with s.post(
        f"{BASEURL}/dplycourse.asp?page={1}",
        data={
            "HIS": "",
            "IDNO": "",
            "ITEM": "",
            "D0": "1122",
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
        return await resp.text()


async def main():
    images = Path("images")

    if images.is_dir():
        shutil.rmtree(str(images))

    total = done_count = error_count = 0
    async with aiohttp.ClientSession() as s:
        while total < 4000:
            out = await s.get(
                f"{BASEURL}/validcode.asp?epoch={time.time()}",
            )

            img = await out.read()
            code = parse_valid_code(img)
            out = await fetch(s, code)
            total += 1
            if "Wrong Validation Code" in out:
                error_count += 1
                path = images / f"errors/{error_count}_{code}.png"
                path.parent.mkdir(parents=True, exist_ok=True)
                with path.open("wb") as f:
                    f.write(img)
            else:
                done_count += 1
                # Load the image
                image = Image.open(io.BytesIO(img))

                # Convert the image to grayscale
                image = image.convert("L")

                width, height = image.size
                segment_width = width // 4

                for i, digit in enumerate(code):
                    segment = image.crop(
                        (i * segment_width, 0, (i + 1) * segment_width, height)
                    )

                    digit_path = images / f"done/{digit}/"
                    digit_path.mkdir(parents=True, exist_ok=True)
                    segment_path = digit_path / f"{done_count}_{code}_{i}.png"
                    segment.save(segment_path)
            print(
                f"total: {total:04d}, done: {done_count:04d}, error: {error_count:04d}"
                " [{:.2f}%]".format(done_count / total * 100)
            )


asyncio.run(main())
