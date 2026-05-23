"""
MonitorAgent — Health monitoring, SLA tracking, metric collection.
Monitors pipeline health and tracks SLA compliance.
"""
import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class SLARule:
    rule_id: str
    metric: str  # "duration", "throughput", "error_rate", "freshness"
    threshold: float
    operator: str  # "lt", "gt", "eq", "lte", "gte"
    enabled: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {"rule_id": self.rule_id, "metric": self.metric,
                "threshold": self.threshold, "operator": self.operator}


@dataclass
class MetricPoint:
    metric_name: str
    value: float
    timestamp: float
    tags: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {"name": self.metric_name, "value": self.value,
                "timestamp": self.timestamp, "tags": self.tags}


class MonitorAgent:
    """Monitors pipeline health and tracks SLA compliance."""

    def __init__(self, config: dict[str, Any] | None = None):
        self.config = config or {}
        self.metrics: list[MetricPoint] = []
        self.sla_rules: list[SLARule] = []
        self.violations: list[dict] = []

    def add_sla_rule(self, rule: SLARule):
        """Add an SLA rule."""
        self.sla_rules.append(rule)
        logger.info(f"Added SLA rule: {rule.rule_id}")

    def record_metric(self, name: str, value: float, tags: dict[str, str] | None = None):
        """Record a metric point."""
        metric = MetricPoint(metric_name=name, value=value, timestamp=time.time(), tags=tags or {})
        self.metrics.append(metric)
        logger.debug(f"Recorded metric: {name}={value}")

    def check_sla(self) -> list[dict]:
        """Check all SLA rules against current metrics."""
        violations = []
        for rule in self.sla_rules:
            if not rule.enabled:
                continue

            # Get latest metric
            relevant = [m for m in self.metrics if m.metric_name == rule.metric]
            if not relevant:
                continue

            latest = relevant[-1]
            violated = False

            if rule.operator == "gt" and latest.value > rule.threshold:
                violated = True
            elif rule.operator == "lt" and latest.value < rule.threshold:
                violated = True
            elif rule.operator == "gte" and latest.value >= rule.threshold:
                violated = True
            elif rule.operator == "lte" and latest.value <= rule.threshold:
                violated = True
            elif rule.operator == "eq" and latest.value == rule.threshold:
                violated = True

            if violated:
                violations.append({"rule": rule.rule_id, "metric": rule.metric,
                                   "value": latest.value, "threshold": rule.threshold})
                logger.warning(f"SLA violation: {rule.rule_id}")

        self.violations.extend(violations)
        return violations

    async def health_check(self) -> dict:
        return {"status": "healthy", "metrics_count": len(self.metrics), "sla_rules": len(self.sla_rules)}
