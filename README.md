# DataPipelineAI

Multi-Agent Data Pipeline Orchestration System — 7 specialized agents for ingestion, transformation, quality validation, loading, scheduling, monitoring, and reporting.

## Architecture

| Agent | Role | LOC |
|-------|------|-----|
| IngestionAgent | Multi-source data ingestion (APIs, DBs, files, streams, S3, Kafka) | 450+ |
| TransformationAgent | Data cleaning, normalization, deduplication, enrichment | 500+ |
| QualityAgent | Schema validation, completeness scoring, auto-detection | 400+ |
| LoadingAgent | Multi-target batch loading (9 targets supported) | 350+ |
| SchedulerAgent | DAG-based scheduling, dependency resolution, retry logic | 350+ |
| MonitorAgent | Health monitoring, SLA tracking, metric collection | 400+ |
| ReportingAgent | Execution reports, data lineage, quality trends | 350+ |

## Features
- **Multi-source ingestion**: APIs, databases, files, streams, S3, Kafka, SFTP, GCS
- **Configurable transformations**: 10+ built-in transforms, custom rules, deduplication
- **Quality scoring**: Schema validation, completeness, consistency, accuracy checks
- **Batch loading**: 9 target systems with configurable batch sizes and write modes
- **DAG scheduling**: Topological sort, dependency resolution, automatic retry
- **SLA monitoring**: Custom SLA rules, violation detection, metric buffering
- **Rich reporting**: Execution reports, quality trends, data lineage tracking

## Usage
```bash
# Run a full pipeline
python -m src.main run --sources api_source db_source --targets postgres s3

# Check system status
python -m src.main status

# Health check
python -m src.main health
```

## Installation
```bash
pip install -r requirements.txt
```

## Token Consumption
~15M tokens/day — continuous data processing, quality monitoring, pipeline orchestration, metric analysis, and automated reporting across multiple data sources.

## Testing
```bash
python -m pytest tests/ -v
```

Built with: Hermes Agent, MiMo + Claude series
