"""DataPipelineAI CLI — Command-line interface for pipeline management."""
import argparse
import asyncio
import json
import logging
from .orchestrator import PipelineOrchestrator


def setup_logging(level: str = "INFO"):
    logging.basicConfig(level=getattr(logging, level.upper()),
                        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")


def parse_args():
    parser = argparse.ArgumentParser(description="DataPipelineAI — Multi-Agent Data Pipeline")
    sub = parser.add_subparsers(dest="command")

    run_p = sub.add_parser("run", help="Run a pipeline")
    run_p.add_argument("--sources", nargs="+", required=True, help="Source IDs to ingest from")
    run_p.add_argument("--targets", nargs="+", required=True, help="Target IDs to load to")
    run_p.add_argument("--pipeline-id", default="default", help="Pipeline identifier")

    sub.add_parser("status", help="Show system status")
    sub.add_parser("health", help="Run health check")

    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser.parse_args()


def main():
    args = parse_args()
    setup_logging(args.log_level)
    orchestrator = PipelineOrchestrator()

    if args.command == "run":
        result = asyncio.run(orchestrator.run_pipeline(args.sources, args.targets, args.pipeline_id))
        print(json.dumps(result, indent=2))
    elif args.command == "status":
        status = orchestrator.get_status()
        print(json.dumps(status, indent=2))
    elif args.command == "health":
        health = asyncio.run(orchestrator.health_check())
        print(json.dumps(health, indent=2))
    else:
        print("DataPipelineAI — Multi-Agent Data Pipeline")
        print("Commands: run, status, health")
