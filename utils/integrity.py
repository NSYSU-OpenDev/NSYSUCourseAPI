from dataclasses import asdict, dataclass, field
from datetime import datetime
import hashlib
import json
from typing import Optional

from utils.utils import generate_iso_time


@dataclass(frozen=True)
class SchemaDrift:
    """An upstream value outside the set this parser knows about."""

    field: str
    value: str
    course_id: str
    department: str


@dataclass(frozen=True)
class ParseFailure:
    """A row that could not be parsed into a course at all."""

    page: int
    reason: str


class ParseCollector:
    """Accumulates parse anomalies without interrupting the parse."""

    def __init__(self) -> None:
        self.drifts: list[SchemaDrift] = []
        self.failures: list[ParseFailure] = []

    def add_drift(self, field_name: str, value: str, course_id: str, department: str) -> None:
        self.drifts.append(SchemaDrift(field_name, value, course_id, department))

    def add_failure(self, page: int, reason: str) -> None:
        self.failures.append(ParseFailure(page, reason))


@dataclass
class CrawlReport:
    """What the crawl itself observed, independent of publishing."""

    total_pages: int
    expected_total: Optional[int]
    lost_pages: list[int] = field(default_factory=list)
    rescan_rounds: int = 0
    collector: ParseCollector = field(default_factory=ParseCollector)


def compute_signature(
    *,
    lost_pages: list[int],
    missing: int,
    drifts: list[SchemaDrift],
    failures: list[ParseFailure],
) -> str:
    """Fingerprint of the anomaly state.

    Deliberately excludes timestamps: the workflow runs hourly, and a
    signature that changed every run would comment on the tracking issue
    every hour.
    """
    payload = {
        "lost_pages": sorted(lost_pages),
        "missing": missing,
        "drifts": sorted((d.field, d.value, d.course_id) for d in drifts),
        "failures": sorted((f.page, f.reason) for f in failures),
    }
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@dataclass
class IntegrityReport:
    academic_year: str
    checked_at: str
    expected_total: Optional[int]
    actual_total: int
    missing: int
    complete: bool
    total_pages: int
    rescan_rounds: int
    lost_pages: list[int]
    schema_drift: list[SchemaDrift]
    parse_failures: list[ParseFailure]
    signature: str

    def to_dict(self) -> dict:
        return asdict(self)


def build_integrity_report(
    academic_year: str,
    actual_total: int,
    crawl_report: CrawlReport,
    checked_at: datetime,
) -> IntegrityReport:
    """Combine crawl observations and the published count into a ledger."""
    expected = crawl_report.expected_total

    # Courses can be added between the first and last request of a crawl.
    # A count above the declared total means the catalogue grew, not that
    # data was lost.
    missing = max(0, expected - actual_total) if expected is not None else 0

    drifts = crawl_report.collector.drifts
    failures = crawl_report.collector.failures
    complete = missing == 0 and not crawl_report.lost_pages and not drifts and not failures

    return IntegrityReport(
        academic_year=academic_year,
        checked_at=generate_iso_time(checked_at),
        expected_total=expected,
        actual_total=actual_total,
        missing=missing,
        complete=complete,
        total_pages=crawl_report.total_pages,
        rescan_rounds=crawl_report.rescan_rounds,
        lost_pages=sorted(crawl_report.lost_pages),
        schema_drift=list(drifts),
        parse_failures=list(failures),
        signature=compute_signature(
            lost_pages=crawl_report.lost_pages,
            missing=missing,
            drifts=drifts,
            failures=failures,
        ),
    )
