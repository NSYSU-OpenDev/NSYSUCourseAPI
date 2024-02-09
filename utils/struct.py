from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
from typing import Callable, Optional, Union
from utils.parse_info import parse_academic_year_code

from utils.utils import generate_iso_time, json_minify_dump, to_datetime, to_timestamp


#################################
#           path.json           #
#################################
def generate_path_info_struct(root_path: Path, path: Path) -> dict:
    """
    Generate path information structure.

    Args:
        root_path (Path): The relative path to the main path.
        path (Path): The path to generate information for.

    Returns:
        dict: A dictionary containing information about the path. It includes the following keys:

            - 'name' (str): The name of the file or directory.
            - 'path' (str): The relative path to the file or directory.
            - 'raw_url' (str): The raw URL of the file or directory, if available.
            - 'static_url' (str): The static URL of the file or directory, if available.
            - 'type' (str): The type of the path, either 'dir' for directory or 'file' for file.
            - 'sha256' (str, optional): The SHA-256 hash of the file content, only if the path is a file.
            - 'size' (int, optional): The size of the file in bytes, only if the path is a file.
    """
    file_path = path.relative_to(root_path).as_posix()
    root = {
        "name": path.name,
        "path": file_path,
        "raw_url": os.getenv("RAW_BASE_URL", "") + file_path,
        "static_url": os.getenv("STATIC_BASE_URL", "") + file_path,
        "type": "dir",
    }

    if path.is_file():
        return {
            **root,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "size": path.stat().st_size,
            "type": "file",
        }

    return root


def generate_paths_info_struct(
    root_path: Path,
    path: Path,
    *,
    filter: Callable[[Path], bool] = lambda _: True,
) -> list[dict]:
    """
    Generate path information structures for all paths inside a directory.

    Args:
        root_path (Path): The relative path to the main directory.
        path (Path): The directory path to generate information for.
        filter (Callable[[Path], bool], optional): A function to filter paths. Defaults to lambda _: True.

    Returns:
        list[dict]: A list of dictionaries, each containing information about a path within the directory.
    """
    return [generate_path_info_struct(root_path, p) for p in path.iterdir() if filter(p)]


def recursion_generate_paths_info_file(root_path: Path, directory_path: Optional[Path] = None) -> None:
    """
    Recursively generates path information files for directories and their contents.

    Args:
        root_path (Path): The relative path to the main directory.
        directory_path (Optional[Path]): The directory path to start generating path information files from.
            If not provided, it defaults to the root_path.

    Raises:
        ValueError: If the provided path is not a directory.
    """
    if directory_path is None:
        directory_path = root_path

    if not directory_path.is_dir():
        raise ValueError(f"Path {directory_path.as_posix()!r} is not a directory")

    def start_generation(path: Path) -> None:
        paths_info_struct = generate_paths_info_struct(
            root_path,
            path,
            # Exclude the path.json file from the generated paths info (remove yourself)
            filter=lambda x: x.name not in ["path.json", ".git"],
        )
        (path / "path.json").write_text(json_minify_dump(paths_info_struct), encoding="utf-8")

    start_generation(directory_path)
    for p in directory_path.glob("**/*"):
        if p.is_dir():
            start_generation(p)


################################
#         version.json         #
################################
class _BaseVersionManger:
    """
    Base class for managing versions stored in a JSON file.

    Attributes:
        _versions (dict[str, str]): Dictionary containing version numbers and their corresponding data.
        _latest_version (str): Latest version number.
    """

    def __init__(self, data: Union[dict, Path, None] = None) -> None:
        """
        Initializes the BaseVersionManger with provided data.

        Args:
            data (Union[dict, Path, None], optional): Data to initialize the version manager.
                It can be either a dictionary, a Path object representing a JSON file,
                or None if no initial data is provided. Defaults to None.
        """
        self._versions: dict[str, str] = {}
        self._latest_version: str = ""

        if data is not None:
            self.update(data)

    def update(self, data: Union[dict, Path]) -> None:
        """
        Update version information with new data.

        Args:
            data (Union[dict, Path]): Data to update from. It can be either a dictionary or a Path object.
                If it's a dictionary, it's directly used as the new data.
                If it's a Path object, it's assumed to be a JSON file containing the new data.

        Raises:
            ValueError: If the data type is invalid.
        """
        if isinstance(data, dict):
            self._update_from_dict(data)
        elif isinstance(data, Path):
            self._update_from_json_file(data)
        else:
            raise ValueError(f"Invalid data type: {type(data)}")

    def _update_from_dict(self, data: dict) -> None:
        """
        Update version information with data from a dictionary.

        Args:
            data (dict): Dictionary containing version data.
        """
        self._latest_version: str = data["latest"]
        self._versions: dict[str, str] = data["history"]

    def _update_from_json_file(self, file_path: Path) -> bool:
        """
        Update version information with data from a JSON file.

        Args:
            file_path (Path): Path to the JSON file containing version data.

        Returns:
            bool: True if the update from the JSON file was successful, False otherwise.
        """
        if file_path.is_file():
            try:
                data = json.loads(file_path.read_text(encoding="utf-8"))
                self._update_from_dict(data)
                return True
            except json.JSONDecodeError:
                pass
        return False

    @property
    def versions(self) -> dict[str, str]:
        """
        Get the dictionary containing version numbers and their corresponding data.

        Returns:
            dict[str, str]: Dictionary containing version numbers and their corresponding data.
        """
        return self._versions

    @property
    def latest_version(self) -> str:
        """
        Get the latest version number.

        Returns:
            str: Latest version number.
        """
        return self._latest_version

    def set_latest_version(self, latest_version: str) -> None:
        """
        Set the latest version number.

        Args:
            latest_version (str): Latest version number to set.
        """
        self._latest_version = latest_version

    def is_latest_version(self, version: str) -> bool:
        """
        Check if a version is the latest version.

        Args:
            version (str): Version number to check.

        Returns:
            bool: True if the version is the latest version, False otherwise.
        """
        return version == self._latest_version

    def is_version_exists(self, version: str) -> bool:
        """
        Check if a version exists.

        Args:
            version (str): Version number to check.

        Returns:
            bool: True if the version exists, False otherwise.
        """
        return version in self._versions

    def to_dict(self) -> dict:
        """
        Convert the version information to a dictionary.

        Returns:
            dict: Dictionary containing version information.
        """
        return {
            "latest": self._latest_version,
            "history": self._versions,
        }

    def to_file(self, file_path: Path, **kwargs) -> None:
        """
        Write the version information to a JSON file.

        Args:
            file_path (Path): Path to the JSON file.
            **kwargs: Additional keyword arguments passed to json_minify_dump.
        """
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text(json_minify_dump(self.to_dict(), **kwargs), encoding="utf-8")


class RootPathVersionManager(_BaseVersionManger):
    """Version manager for root paths"""

    def add_version(self, academic_year: str, *, new_version: bool = False) -> bool:
        """
        Add a new version for the given update.

        Args:
            academic_year (str): Academic year code for the new version.

        Returns:
            bool: True if the latest version is updated, False otherwise.
            new_version (bool, optional): If True, forces the update of the latest version
                even if the provided academic year is not greater than the current latest version.
                Defaults to False.

        Raises:
            ValueError: If the academic year code is invalid. (from parse_academic_year_code)
        """
        self._versions[academic_year] = parse_academic_year_code(academic_year)

        if (
            new_version
            # if _laster_version is default ("")
            or not self._latest_version
            # if update is greater than _laster_version
            or academic_year > self._latest_version
        ):
            self._latest_version = academic_year
            return True
        return False


class AcademicYearPathVersionManager(_BaseVersionManger):
    """Version manager for academic year paths"""

    def add_version(
        self,
        update: Optional[datetime] = None,
        *,
        new_version: bool = False,
    ) -> Optional[str]:
        """
        Add a new version for the given academic year.

        Args:
            update (Optional[datetime], optional): Update datetime for the new version.
                If not provided, the current datetime is used. Defaults to None.
            new_version (bool, optional): Flag indicating if the update represents a new version.
                Defaults to False.

        Returns:
            Optional[str]: Update timestamp if the latest version is updated, None otherwise.
        """
        if update is None:
            update = datetime.now()

        update_date = to_timestamp(update)
        self._versions[update_date] = generate_iso_time(update)

        if (
            new_version
            # if _laster_version is default ("")
            or not self._latest_version
            # if update is greater than _laster_version
            or update > to_datetime(self._latest_version)
        ):
            self._latest_version = update_date
            return update_date


#################################
#           path.json           #
#################################
