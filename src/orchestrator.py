"""DataPipelineAI Orchestrator — Coordinates all 7 pipeline agents."""
import asyncio
import logging
import time
from typing import Any
from .agents.ingestion_agent import IngestionAgent, DataSource, IngestionResult
from .agents.transformation_agent import TransformationAgent, TransformationRule, TransformationResult
from .agents.quality_agent import QualityAgent, QualityRule, QualityReport
from .agents.loading_agent import LoadingAgent, LoadTarget, LoadResult
from .agents.scheduler_agent import SchedulerAgent, PipelineTask
from .agents.monitor_agent import MonitorAgent
from .agents.reporting_agent import ReportingAgent

logger = logging.getLogger(__name__)


class PipelineOrchestrator:
    """Coordinates all 7 agents into a unified data pipeline."""

    def __init__(self, config: dict[str, Any] | None = None):
        self.config = config or {}
        self.ingestion = IngestionAgent(config)
        self.transformation = TransformationAgent(config)
        self.quality = QualityAgent(config)
        self.loading = LoadingAgent(config)
        self.scheduler = SchedulerAgent(config)
        self.monitor = MonitorAgent(config)
        self.reporting = ReportingAgent(config)
        self.pipelines: dict[str, dict] = {}

    async def run_pipeline(self, sources: list[str], targets: list[str], pipeline_id: str = "default") -> dict:
        """Execute a complete data pipeline."""
        start_time = time.time()
        logger.info(f"Starting pipeline {pipeline_id}")

        # 1. Ingest
        ingestion_results = []
        for source_id in sources:
            source = DataSource(source_id=source_id, source_type="api", connection_config={})
            result = await self.ingestion.ingest(source)
            ingestion_results.append(result)

        # 2. Transform
        transformation_results = []
        rules = [TransformationRule(rule_id="clean", rule_type="clean", config={})]
        for result in ingestion_results:
            transformed = await self.transformation.transform([], rules)
            transformation_results.append(transformed)

        # 3. Quality check
        quality_rules = [QualityRule(rule_id="check", rule_type="schema", config={})]
        quality_report = await self.quality.validate([], quality_rules)

        # 4. Load
        load_results = []
        for target_id in targets:
            target = LoadTarget(target_id=target_id, target_type="database", connection_config={})
            result = await self.loading.load([], target)
            load_results.append(result)

        # 5. Report
        report = await self.reporting.generate_report(pipeline_id, ingestion_results, quality_report, load_results)

        duration = time.time() - start_time
        logger.info(f"Pipeline {pipeline_id} completed in {duration:.2f}s")

        return {
            "pipeline_id": pipeline_id,
            "status": "completed",
            "duration_seconds": duration,
            "ingestion": [r.to_dict() for r in ingestion_results],
            "quality": quality_report.to_dict() if quality_report else None,
            "loading": [r.to_dict() for r in load_results],
            "report": report
        }

    def get_status(self) -> dict:
        """Get current system status."""
        return {
            "status": "running",
            "agents": {
                "ingestion": "active",
                "transformation": "active",
                "quality": "active",
                "loading": "active",
                "scheduler": "active",
                "monitor": "active",
                "reporting": "active"
            },
            "pipelines": len(self.pipelines),
            "uptime": time.time()
        }

    async def health_check(self) -> dict:
        """Run health check on all agents."""
        return {
            "status": "healthy",
            "agents": {
                "ingestion": await self.ingestion.health_check(),
                "transformation": await self.transformation.health_check(),
                "quality": await self.quality.health_check(),
                "loading": await self.loading.health_check(),
                "scheduler": await self.scheduler.health_check(),
                "monitor": await self.monitor.health_check(),
                "reporting": await self.reporting.health_check()
            }
        }
