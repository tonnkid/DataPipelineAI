"""
SchedulerAgent — DAG-based scheduling, dependency resolution, retry logic.
Manages pipeline execution order and dependency resolution.
"""
import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any
from collections import defaultdict

logger = logging.getLogger(__name__)


@dataclass
class PipelineTask:
    task_id: str
    task_type: str  # "ingest", "transform", "quality", "load"
    dependencies: list[str] = field(default_factory=list)
    config: dict[str, Any] = field(default_factory=dict)
    max_retries: int = 3
    retry_delay: float = 5.0
    status: str = "pending"
    attempts: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {"task_id": self.task_id, "type": self.task_type,
                "dependencies": self.dependencies, "status": self.status,
                "attempts": self.attempts}


class SchedulerAgent:
    """DAG-based scheduler with dependency resolution and retry logic."""

    def __init__(self, config: dict[str, Any] | None = None):
        self.config = config or {}
        self.tasks: dict[str, PipelineTask] = {}
        self.execution_order: list[str] = []

    def add_task(self, task: PipelineTask):
        """Add a task to the scheduler."""
        self.tasks[task.task_id] = task
        logger.info(f"Added task: {task.task_id} ({task.task_type})")

    def resolve_dependencies(self) -> list[str]:
        """Topological sort to resolve execution order."""
        graph = defaultdict(list)
        in_degree = defaultdict(int)

        for task_id, task in self.tasks.items():
            in_degree[task_id] = len(task.dependencies)
            for dep in task.dependencies:
                graph[dep].append(task_id)

        queue = [task_id for task_id, degree in in_degree.items() if degree == 0]
        order = []

        while queue:
            task_id = queue.pop(0)
            order.append(task_id)
            for neighbor in graph[task_id]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        self.execution_order = order
        logger.info(f"Execution order: {order}")
        return order

    async def execute_task(self, task: PipelineTask) -> bool:
        """Execute a task with retry logic."""
        for attempt in range(task.max_retries):
            task.attempts = attempt + 1
            logger.info(f"Executing {task.task_id} (attempt {attempt + 1}/{task.max_retries})")

            # Simulate execution
            await asyncio.sleep(0.1)
            task.status = "completed"
            return True

        task.status = "failed"
        logger.error(f"Task {task.task_id} failed after {task.max_retries} attempts")
        return False

    async def health_check(self) -> dict:
        return {"status": "healthy", "tasks_pending": len([t for t in self.tasks.values() if t.status == "pending"])}
