"""DataPipelineAI Tests."""
import asyncio
import pytest
from src.orchestrator import PipelineOrchestrator


@pytest.mark.asyncio
async def test_pipeline_run():
    """Test pipeline execution."""
    orchestrator = PipelineOrchestrator()
    result = await orchestrator.run_pipeline(
        sources=["api_source"],
        targets=["postgres"],
        pipeline_id="test"
    )
    assert result["status"] == "completed"
    assert result["pipeline_id"] == "test"


@pytest.mark.asyncio
async def test_health_check():
    """Test health check."""
    orchestrator = PipelineOrchestrator()
    health = await orchestrator.health_check()
    assert health["status"] == "healthy"
