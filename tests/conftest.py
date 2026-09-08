from pathlib import Path

FIXTURES = Path(__file__).parent / "fixtures"


def fixture_text(name: str) -> str:
    """Read a captured upstream fixture as text.

    Upstream serves UTF-8 but occasionally emits stray bytes, so decode
    the same way the crawler does.
    """
    return (FIXTURES / name).read_bytes().decode("utf-8", errors="replace")
