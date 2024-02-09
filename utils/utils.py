from datetime import datetime
import json
from typing import TYPE_CHECKING, Any, Iterator, Optional, TypeVar, overload, Literal

if TYPE_CHECKING:
    from _typeshed import SupportsWrite

_T = TypeVar("_T")


def is_integer(text: str) -> bool:
    """
    Check if the text conforms to int.

    Args:
        text (str): The string to check.

    Returns:
        bool: Whether the text conforms to int.
    """
    try:
        int(text)
        return True
    except ValueError:
        return False


@overload
def json_minify_dump(
    obj: Any,
    fp: Literal[None] = None,
    *,
    minify: bool = ...,
    **kwargs,
) -> str: ...


@overload
def json_minify_dump(
    obj: Any,
    fp: "SupportsWrite[str]",
    *,
    minify: bool = ...,
    **kwargs,
) -> None: ...


def json_minify_dump(
    obj: Any,
    fp: Optional["SupportsWrite[str]"] = None,
    *,
    minify: bool = True,
    **kwargs,
) -> Optional[str]:
    """
    Dump an object to JSON format, with optional minification.

    Args:
        obj (Any): The object to dump.
        fp (Optional[SupportsWrite[str]]): Write stream. Defaults to None.
        minify (Optional[bool]): Whether to minify the JSON output. Defaults to True.
        **kwargs: Additional keyword arguments to pass to json.dump() or json.dumps().

    Returns:
        Optional[str]: If `fp` is None, returns the JSON string representation. Otherwise, returns None.
    """
    separators = (",", ":") if minify else None

    if fp is None:
        return json.dumps(obj, ensure_ascii=False, separators=separators, **kwargs)
    else:
        json.dump(obj, fp, ensure_ascii=False, separators=separators, **kwargs)
        return None


def generate_iso_time(time: datetime) -> str:
    """
    Generate an ISO 8601 formatted string representation of a datetime object.

    Args:
        time (datetime): The datetime object to format.

    Returns:
        str: The ISO 8601 formatted string representation of the datetime object.
    """
    return time.strftime("%Y-%m-%dT%H:%M:%SZ")


def to_datetime(text: str) -> datetime:
    """
    Convert a literal timestamp string to a datetime object.

    Args:
        text (str): The literal timestamp string to convert.

    Returns:
        datetime: The datetime object representing the timestamp.

    Raises:
        ValueError: If the input string does not match the expected format.
    """
    return datetime.strptime(text, "%Y%m%d_%H%M%S")


def to_timestamp(time: datetime) -> str:
    """
    Convert a datetime object to a literal timestamp string.

    Args:
        time (datetime): The datetime object to convert.

    Returns:
        str: The literal timestamp string representation of the datetime.
    """
    return time.strftime("%Y%m%d_%H%M%S")


def paginate(data: list[_T], page_size: int) -> Iterator[list[_T]]:
    for i in range(0, len(data), page_size):
        yield data[i : i + page_size]
