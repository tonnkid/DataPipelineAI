"""
ReportingAgent — Execution reports, data lineage, quality trends.
Generates comprehensive reports for pipeline executions.
"""
import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class ExecutionReport:
    pipeline_id: str
    start_time: float
    end_time: float
    duration_seconds: float
    status: str
    ingestion_summary: dict = field(default_factory=dict)
    quality_summary: dict = field(default_factory=dict)
    loading_summary: dict = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {"pipeline_id": self.pipeline_id, "duration": self.duration_seconds,
                "status": self.status, "ingestion": self.ingestion_summary,
                "quality": self.quality_summary, "loading": self.loading_summary}


@dataclass
class LineageRecord:
    record_id: str
    source_id: str
    target_id: str
    transformations: list[str] = field(default_factory=list)
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        return {"record_id": self.record_id, "source": self.source_id,
                "target": self.target_id, "transformations": self.transformations}


class ReportingAgent:
    """Generates comprehensive reports for pipeline executions."""

    def __init__(self, config: dict[str, Any] | None = None):
        self.config = config or {}
        self.reports: list[ExecutionReport] = []
        self.lineage: list[LineageRecord] = []

    async def generate_report(self, pipeline_id: str, ingestion_results: list,
                               quality_report, load_results: list) -> dict:
        """Generate execution report."""
        start_time = time.time()
        logger.info(f"Generating report for pipeline {pipeline_id}")

        # Summarize ingestion
        ingestion_summary = {
            "total_records": sum(r.records_ingested for r in ingestion_results),
            "total_bytes": sum(r.bytes_processed for r in ingestion_results),
            "sources": len(ingestion_results),
            "errors": sum(len(r.errors) for r in ingestion_results)
        }

        # Summarize quality
        quality_summary = {}
        if quality_report:
            quality_summary = {
                "score": quality_report.quality_score,
                "passed": quality_report.records_passed,
                "failed": quality_report.records_failed,
                "status": quality_report.status
            }

        # Summarize loading
        loading_summary = {
            "total_records": sum(r.records_loaded for r in load_results),
            "total_bytes": sum(r.bytes_written for r in load_results),
            "targets": len(load_results),
            "errors": sum(len(r.errors) for r in load_results)
        }

        report = ExecutionReport(
            pipeline_id=pipeline_id,
            start_time=start_time,
            end_time=time.time(),
            duration_seconds=time.time() - start_time,
            status="completed",
            ingestion_summary=ingestion_summary,
            quality_summary=quality_summary,
            loading_summary=loading_summary
        )

        self.reports.append(report)
        logger.info(f"Report generated: {pipeline_id}")

        return report.to_dict()

    def add_lineage(self, record_id: str, source_id: str, target_id: str, transformations: list[str]):
        """Add lineage record."""
        lineage = LineageRecord(record_id=record_id, source_id=source_id,
                                target_id=target_id, transformations=transformations)
        self.lineage.append(lineage)

    async def health_check(self) -> dict:
        return {"status": "healthy", "reports_generated": len(self.reports)}
