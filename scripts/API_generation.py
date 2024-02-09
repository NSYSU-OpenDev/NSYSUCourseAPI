import asyncio
import json
import os
from pathlib import Path

from deepdiff import DeepDiff

from utils.get_academic_year import get_academic_year
from utils.struct import (
    AcademicYearPathVersionManager,
    RootPathVersionManager,
    recursion_generate_paths_info_file,
)
from utils.utils import json_minify_dump, paginate

API_ROOT_PATH = Path("data")
ROOT_VERSION_PATH = API_ROOT_PATH / "version.json"
API_ROOT_PATH.mkdir(parents=True, exist_ok=True)


async def main():
    academic_year = os.getenv("ACADEMIC_YEAR")

    try:
        data, academic_year = await get_academic_year(academic_year, max_page=2)
    except ValueError as e:
        print(e)
        return

    if not data:
        return

    academic_year_dir = API_ROOT_PATH / academic_year
    academic_year_dir.mkdir(parents=True, exist_ok=True)

    academic_year_version_file = academic_year_dir / "version.json"
    academic_year_version_manager = AcademicYearPathVersionManager(academic_year_version_file)
    old_latest_version = academic_year_version_manager.latest_version

    old_data = {}
    if old_latest_version:
        old_academic_year_file = academic_year_dir / old_latest_version / "all.json"
        if old_academic_year_file.is_file():
            old_data = json.loads(old_academic_year_file.read_text(encoding="utf-8"))

    root_version_manager = RootPathVersionManager(ROOT_VERSION_PATH)
    if root_version_manager.add_version(academic_year):
        root_version_manager.to_file(ROOT_VERSION_PATH)

    diff = DeepDiff(data, old_data, ignore_order=True, report_repetition=True)
    if academic_year_version_file.is_file() and not diff:
        return

    timestamp = academic_year_version_manager.add_version()
    if timestamp is None:
        return

    academic_year_version_manager.to_file(academic_year_version_file)

    new_academic_year_dir = academic_year_dir / timestamp
    new_academic_year_dir.mkdir(parents=True, exist_ok=True)
    (new_academic_year_dir / "all.json").write_text(json_minify_dump(data), encoding="utf-8")

    i = 0
    for i, page in enumerate(paginate(data, 10)):
        page_path = new_academic_year_dir / f"page_{i + 1}.json"
        page_path.write_text(json_minify_dump(page), encoding="utf-8")

    info_content = json_minify_dump({"page_size": i + 1, "updated": timestamp})
    (new_academic_year_dir / "info.json").write_text(info_content, encoding="utf-8")

    recursion_generate_paths_info_file(API_ROOT_PATH)


def start() -> None:
    asyncio.run(main())


if __name__ == "__main__":
    start()
