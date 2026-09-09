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
    point would mean the worst case never reports.

    Every write must precede the early return, not just the first one: main()
    also writes an abort ledger from the `except ValueError` handler, and
    matching that one alone would let the main write drift past the return."""
    body = _main_body()
    writes = [m.start() for m in re.finditer(r"INTEGRITY_REPORT_PATH\.write_text", body)]
    assert writes, "main() must write the integrity ledger"
    early_return_at = body.index("if academic_year_version_file.is_file() and not diff:")
    assert max(writes) < early_return_at


def test_a_crawl_that_produced_nothing_still_writes_a_ledger():
    """get_academic_year raises ValueError when it cannot determine the
    academic year or the page count. Returning without a ledger means
    report_integrity.py finds no file and also exits 0 — a green checkmark
    over a silently frozen API."""
    body = _main_body()
    handler_at = body.index("except ValueError as e:")
    handler = body[handler_at : body.index("\n    # Written before", handler_at)]
    assert "INTEGRITY_REPORT_PATH.write_text" in handler
    assert "build_integrity_report" in handler
    # The ledger write must not itself be able to fail the run.
    assert "except Exception" in handler


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
