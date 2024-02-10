import asyncio
import csv
import json
import os
from pathlib import Path
import shutil

from deepdiff import DeepDiff

from utils.get_academic_year import get_academic_year
from utils.struct import (
    AcademicYearPathVersionManager,
    RootPathVersionManager,
    recursion_generate_paths_info_file,
)
from utils.utils import json_minify_dump, paginate

PER_PAGE_SIZE = 20
MAX_HISTORY_COUNT = 5

# Root path for API data
API_ROOT_PATH = Path("data")
ROOT_VERSION_PATH = API_ROOT_PATH / "version.json"


def trim_version(
    academic_year_version_manager: AcademicYearPathVersionManager,
    academic_year_dir: Path,
    academic_year_version_file: Path,
) -> None:
    """
    Trim the version history for a given academic year.

    Args:
        academic_year_version_manager (AcademicYearPathVersionManager):
            The version manager for the academic year.
        academic_year_dir (Path):
            The directory containing academic year data.
        academic_year_version_file (Path):
            The file path to save the trimmed version history.

    Returns:
        None
    """
    # Get the list of versions for the academic year
    academic_year_versions = list(academic_year_version_manager.versions.keys())

    # for year in academic_year_versions:
    #     # Construct the path to the directory of the current version
    #     current_dir = academic_year_dir / year
    #     if not current_dir.is_dir():
    #         # Remove the version from the version manager if the directory does not exist
    #         try:
    #             del academic_year_version_manager.versions[year]
    #         except KeyError:
    #             pass

    # Check if the number of versions exceeds the maximum allowed history count
    if len(academic_year_versions) > MAX_HISTORY_COUNT:
        # Iterate over the old versions to be removed
        for old in academic_year_versions[:-MAX_HISTORY_COUNT]:
            # Construct the path to the directory of the old version
            old_dir = academic_year_dir / old

            # Check if the directory exists and remove it if it does
            if old_dir.is_dir():
                shutil.rmtree(old_dir)

            # Remove the old version from the version manager
            try:
                del academic_year_version_manager.versions[old]
            except KeyError:
                pass

        # Save the trimmed version history to file
        academic_year_version_manager.to_file(academic_year_version_file)


async def main():
    # Retrieve academic year from environment variable
    academic_year = os.getenv("ACADEMIC_YEAR", "").strip()
    if not academic_year:
        academic_year = None

    # Retrieve max page from environment variable
    max_page = os.getenv("MAX_PAGE", "").strip()
    if not max_page:
        max_page = None
    else:
        max_page = int(max_page)

    try:
        # Get academic year data
        data, academic_year = await get_academic_year(academic_year, max_page=max_page)
    except ValueError as e:
        print(e)
        return

    if not data:
        return

    # Setup API root path if it doesn't exist
    API_ROOT_PATH.mkdir(parents=True, exist_ok=True)

    # Create directory for academic year data if it doesn't exist
    academic_year_dir = API_ROOT_PATH / academic_year
    academic_year_dir.mkdir(parents=True, exist_ok=True)

    # Initialize version manager for academic year
    academic_year_version_file = academic_year_dir / "version.json"
    academic_year_version_manager = AcademicYearPathVersionManager(academic_year_version_file)
    old_latest_version = academic_year_version_manager.latest_version

    # Load old data if available
    old_data = {}
    if old_latest_version:
        old_academic_year_file = academic_year_dir / old_latest_version / "all.json"
        if old_academic_year_file.is_file():
            old_data = json.loads(old_academic_year_file.read_text(encoding="utf-8"))

    # Initialize root version manager
    root_version_manager = RootPathVersionManager(ROOT_VERSION_PATH)
    if academic_year not in root_version_manager.versions:
        root_version_manager.add_version(academic_year)
        root_version_manager.to_file(ROOT_VERSION_PATH)

    # Trim the version history
    trim_version(academic_year_version_manager, academic_year_dir, academic_year_version_file)

    # Find differences between new and old data
    diff = DeepDiff(old_data, data, ignore_order=True, report_repetition=True)
    if academic_year_version_file.is_file() and not diff:
        return

    # Add new version of data
    timestamp = academic_year_version_manager.add_version()
    if timestamp is None:
        return

    academic_year_version_manager.to_file(academic_year_version_file)

    # Save new data to the corresponding directory
    new_academic_year_dir = academic_year_dir / timestamp
    new_academic_year_dir.mkdir(parents=True, exist_ok=True)
    (new_academic_year_dir / "all.json").write_text(json_minify_dump(data), encoding="utf-8")

    # Paginate and save data into separate files
    i = 0
    for i, page in enumerate(paginate(data, PER_PAGE_SIZE)):
        page_path = new_academic_year_dir / f"page_{i + 1}.json"
        page_path.write_text(json_minify_dump(page), encoding="utf-8")

    # Generate CSV files from JSON data
    for path in new_academic_year_dir.glob("**/*.json"):
        data = json.loads(path.read_text(encoding="utf-8"))
        csv_file = path.parent / f"{path.name.removesuffix('.json')}.csv"
        if csv_file.is_file():
            continue

        if isinstance(data, list) and len(data) > 0:
            with csv_file.open("w", encoding="utf-8") as f:
                writer = csv.DictWriter(f, data[0].keys())
                writer.writeheader()
                writer.writerows(data)

    # Generate info file for the current academic year version
    info_content = json_minify_dump({"page_size": i + 1, "updated": timestamp})
    (new_academic_year_dir / "info.json").write_text(info_content, encoding="utf-8")

    # Generate info file for the current academic year version
    (new_academic_year_dir / "diff.txt").write_text(diff.pretty(), encoding="utf-8")

    # Trim the version history
    trim_version(academic_year_version_manager, academic_year_dir, academic_year_version_file)

    # Update paths info file
    recursion_generate_paths_info_file(API_ROOT_PATH)


def start() -> None:
    asyncio.run(main())


if __name__ == "__main__":
    start()
