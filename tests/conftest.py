from pathlib import Path

FIXTURES = Path(__file__).parent / "fixtures"


def fixture_text(name: str) -> str:
    """Read a captured upstream fixture as text.

    Deliberately lenient, and NOT the same as the crawler: the crawler
    decodes strictly and treats an undecodable page as missing ("") so it
    is rescanned and, failing that, reported in lost_pages. Fixtures are
    known-good captures, so replacing a stray byte here keeps a fixture
    defect from masquerading as a parser bug.
    """
    return (FIXTURES / name).read_bytes().decode("utf-8", errors="replace")
