"""Guards for the academic-year term labels published in version.json.

The 4th digit of an academic year code is the term, and upstream's dropdown
lists all four: 0 暑碩, 1 上, 2 下, 3 暑期. Codes ending in 0 are real and go back
a long way (1060 down to 0880), so the term must not be dropped.

The map was correct; the indexing was not. It was written to be indexed by the
digit directly, but the lookup subtracted one, so every label came out shifted.
Because labels were stored rather than recomputed, the shift was frozen into
published data for eight of ten academic years.
"""
import json

import pytest

from tests.conftest import FIXTURES
from utils.parse_info import parse_academic_year_code
from utils.struct import RootPathVersionManager

# Every academic year option the upstream dropdown offered on 2026-09-09,
# captured so the mapping is checked against real data rather than assumptions.
UPSTREAM_OPTIONS = json.loads(
    (FIXTURES / "academic_year_options.json").read_text(encoding="utf-8")
)


@pytest.mark.parametrize(
    ("code", "label"),
    [
        # Every term upstream emits, including 暑碩, which a previous attempt at
        # this fix deleted outright.
        ("1150", "115暑碩"),
        ("1151", "115上"),
        ("1152", "115下"),
        ("1153", "115暑期"),
        ("1060", "106暑碩"),
        ("0880", "088暑碩"),
        ("1141", "114上"),
        ("1142", "114下"),
        ("1143", "114暑期"),
    ],
)
def test_term_labels_match_upstream(code, label):
    assert parse_academic_year_code(code) == label


@pytest.mark.parametrize("code", ["1154", "115", "11511", "115X", ""])
def test_invalid_codes_are_rejected(code):
    with pytest.raises(ValueError):
        parse_academic_year_code(code)


def test_stored_labels_are_recomputed_on_load():
    """Loading repairs history written while the indexing was wrong, rather than
    preserving it. These are the values that were actually published."""
    manager = RootPathVersionManager(
        {
            "latest": "1151",
            "history": {
                "1121": "112上",
                "1122": "112下",
                "1123": "112下",
                "1131": "113暑碩",
                "1142": "114上",
                "1151": "115暑碩",
            },
        }
    )

    assert manager.versions == {
        "1121": "112上",
        "1122": "112下",
        "1123": "112暑期",
        "1131": "113上",
        "1142": "114下",
        "1151": "115上",
    }


def test_a_summer_masters_code_survives_relabelling():
    """Regression guard: 暑碩 was briefly removed from the map, which would have
    mislabelled every code ending in 0 and made the validator reject them."""
    manager = RootPathVersionManager(
        {"latest": "1151", "history": {"1060": "106暑碩", "1151": "115上"}}
    )

    assert manager.versions["1060"] == "106暑碩"


def test_an_unparseable_code_keeps_its_stored_label():
    """Relabelling runs before any course data is published, so it must never
    raise: a cosmetic label is not worth stopping a publish over."""
    manager = RootPathVersionManager(
        {"latest": "1151", "history": {"1151": "115暑碩", "garbage": "whatever"}}
    )

    assert manager.versions["1151"] == "115上"
    assert manager.versions["garbage"] == "whatever"


def test_adding_a_new_year_uses_the_corrected_indexing():
    manager = RootPathVersionManager({"latest": "1151", "history": {"1151": "115上"}})
    manager.add_version("1152")

    assert manager.versions["1152"] == "115下"


@pytest.mark.parametrize(("code", "upstream_label"), sorted(UPSTREAM_OPTIONS.items()))
def test_every_upstream_option_gets_the_label_upstream_shows(code, upstream_label):
    """Check the mapping against all 113 real options rather than a hand-picked
    few. A previous fix was reasoned about instead of verified and dropped the
    暑碩 term, which this would have caught immediately."""
    assert parse_academic_year_code(code) == upstream_label


def test_the_fixture_still_covers_all_four_terms():
    """If upstream ever adds a term, the parametrised test above only proves the
    mapping matches what was captured. This pins the coverage itself."""
    assert {code[3] for code in UPSTREAM_OPTIONS} == {"0", "1", "2", "3"}
