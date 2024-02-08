import asyncio
import hashlib
import os
import json
from pathlib import Path

from utils.get_academic_year import get_academic_year

API_ROOT_PATH = Path(__file__).parent.parent / "data"
API_ROOT_PATH.mkdir(parents=True, exist_ok=True)


async def main():
    academic_year = os.getenv("ACADEMIC_YEAR")

    try:
        d = await get_academic_year(academic_year)
    except ValueError as e:
        print(e)
        return

    print(json.dumps(d, indent=4))


def start() -> None:
    asyncio.run(main())


if __name__ == "__main__":
    start()
