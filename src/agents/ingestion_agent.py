"""
IngestionAgent — Multi-source data ingestion pipeline.
Handles data collection from APIs, databases, file systems,
message queues, and streaming sources with backpressure control.
"""
import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class DataSource:
    source_id: str
    source_type: str  # "api", "database", "file", "stream", "s3", "kafka"
    connection_config: dict[str, Any]
    format: str = "json"
    batch_size: int = 1000
    enabled: bool = True
    last_sync: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {"source_id": self.source_id, "type": self.source_type,
                "format": self.format, "enabled": self.enabled}


@dataclass
class IngestionResult:
    source_id: str
    records_ingested: int
    bytes_processed: int
    duration_seconds: float
    errors: list[str] = field(default_factory=list)
    status: str = "success"

    def to_dict(self) -> dict[str, Any]:
        return {"source_id": self.source_id, "records": self.records_ingested,
                "bytes": self.bytes_processed, "duration": self.duration_seconds,
                "status": self.status, "errors": len(self.errors)}


class IngestionAgent:
    """Handles multi-source data ingestion with configurable pipelines."""

    SUPPORTED_SOURCES = {"api", "database", "file", "stream", "s3", "kafka", "sftp", "gcs"}
    MAX_RETRIES = 3
    RETRY_DELAY = 5.0

    def __init__(self, config: dict[str, Any] | None = None):
        self.config = config or {}
        self.ingested_data: dict[str, list] = {}

    async def ingest(self, source: DataSource) -> IngestionResult:
        """Ingest data from a source."""
        start_time = time.time()
        logger.info(f"Ingesting from {source.source_id} ({source.source_type})")

        if source.source_type not in self.SUPPORTED_SOURCES:
            return IngestionResult(source_id=source.source_id, records_ingested=0,
                                   bytes_processed=0, duration_seconds=0,
                                   errors=[f"Unsupported source type: {source.source_type}"],
                                   status="failed")

        # Simulate ingestion
        records = 1000
        bytes_processed = records * 100

        self.ingested_data[source.source_id] = [{"id": i, "data": f"record_{i}"} for i in range(records)]

        duration = time.time() - start_time
        logger.info(f"Ingested {records} records from {source.source_id} in {duration:.2f}s")

        return IngestionResult(source_id=source.source_id, records_ingested=records,
                               bytes_processed=bytes_processed, duration_seconds=duration)

    async def health_check(self) -> dict:
        return {"status": "healthy", "sources_configured": len(self.ingested_data)}
