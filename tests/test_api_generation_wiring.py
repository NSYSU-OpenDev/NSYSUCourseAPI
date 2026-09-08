# tests/test_api_generation_wiring.py
from pathlib import Path
import re

SOURCE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "API_generation.py"
SOURCE = SOURCE_PATH.read_text(encoding="utf-8")


def _main_body() -> str:
    """The text of main() alone, so ordering assertions cannot accidentally
    match module-level code."""
    start = SOURCE.index("async def main()")
    end = SOURCE.index("\ndef start(", start)
    return SOURCE[start:end]


def test_module_still_compiles():
    """This module is read as text rather than imported: importing it reaches
    utils.parse_valid_code and therefore torch, which the test suite should
    not need. Compiling keeps a syntax error from slipping past the
    source-text assertions below."""
    compile(SOURCE, str(SOURCE_PATH), "exec")


def test_report_is_written_before_the_deepdiff_early_return():
    """A persistent shortfall produces identical data every run, so DeepDiff
    finds no change and main() returns early. Writing the report after that
    point would mean the worst case never reports."""
    body = _main_body()
    write_at = body.index("INTEGRITY_REPORT_PATH.write_text")
    early_return_at = body.index("if academic_year_version_file.is_file() and not diff:")
    assert write_at < early_return_at


def test_actual_total_is_captured_before_the_csv_loop_rebinds_data():
    """The CSV generation loop rebinds `data` to per-file JSON, so the course
    count must be taken before it."""
    body = _main_body()
    capture_at = body.index("actual_total = len(data)")
    csv_loop_at = body.index("for path in new_academic_year_dir.glob")
    assert capture_at < csv_loop_at


def test_integrity_report_path_is_outside_the_data_directory():
    # The Deploy step runs `git add -A` inside data/; the run artifact must
    # not be committed to gh-pages.
    match = re.search(r'INTEGRITY_REPORT_PATH = Path\("([^"]+)"\)', SOURCE)
    assert match, "INTEGRITY_REPORT_PATH must be defined as a Path literal"
    assert not match.group(1).startswith("data/")
    assert Path(match.group(1)).parent == Path(".")
