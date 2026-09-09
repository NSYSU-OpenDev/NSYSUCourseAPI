"""Guards for the academic-year term labels published in version.json.

The 4th digit of an academic year code is the term. Upstream's own dropdown is
the authority: 1151 is 115上, 1142 is 114下, 1143 is 114暑期. A "暑碩" entry was
once prepended to ACADEMIC_YEAR_MAP, shifting every label by one, and because
labels were stored rather than recomputed the mistake persisted in published
data for eight of ten academic years.
"""
import pytest

from utils.parse_info import parse_academic_year_code
from utils.struct import RootPathVersionManager


@pytest.mark.parametrize(
    ("code", "label"),
    [
        ("1151", "115上"),
        ("1152", "115下"),
        ("1153", "115暑期"),
        ("1141", "114上"),
        ("1142", "114下"),
        ("1143", "114暑期"),
        ("1121", "112上"),
        ("1122", "112下"),
    ],
)
def test_term_labels_match_upstream(code, label):
    assert parse_academic_year_code(code) == label


@pytest.mark.parametrize("code", ["1150", "115", "11511", "115X", ""])
def test_invalid_codes_are_rejected(code):
    """0 is not a term upstream ever emits, and it previously indexed backwards
    off the end of the map, silently producing the last term's label."""
    with pytest.raises(ValueError):
        parse_academic_year_code(code)


def test_stored_labels_are_recomputed_on_load():
    """Loading repairs history written by the earlier, incorrect map rather than
    preserving it. These are the real values that were published."""
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


def test_an_unparseable_code_keeps_its_stored_label():
    """Relabelling runs before any course data is published, so it must never
    raise: a cosmetic label is not worth stopping a publish over."""
    manager = RootPathVersionManager(
        {"latest": "1151", "history": {"1151": "115暑碩", "garbage": "whatever"}}
    )

    assert manager.versions["1151"] == "115上"
    assert manager.versions["garbage"] == "whatever"


def test_adding_a_new_year_uses_the_corrected_map():
    manager = RootPathVersionManager({"latest": "1151", "history": {"1151": "115上"}})
    manager.add_version("1152")

    assert manager.versions["1152"] == "115下"
