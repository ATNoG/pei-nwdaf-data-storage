# pei-nwdaf-data-storage

> Project for PEI evaluation 25/26

## Overview

Data storage service providing dual-database persistence layer for the NWDAF platform. Consumes processed network data from Kafka and stores it in both time-series (InfluxDB) and analytical (ClickHouse) databases, while exposing REST APIs for data retrieval.

## Technologies

- **Python** 3.13
- **FastAPI** - REST API framework
- **InfluxDB** - Time-series database for metrics
- **ClickHouse** - Columnar analytical database
- **Apache Kafka** (confluent_kafka) - Message streaming consumer
- **Docker & Docker Compose** - Containerization
- **uvicorn** - ASGI server
- **pytest** - Testing framework

## Architecture

**Dual Database Strategy**:
- **InfluxDB**: Optimized for time-series queries, network metrics storage, fast writes
- **ClickHouse**: Optimized for analytical queries, aggregations, OLAP workloads

## Key Features

- **Kafka consumer**: Subscribes to `network.data.processed` topic
- **Dual writes**: Simultaneously stores data in InfluxDB and ClickHouse
- **REST API endpoints**: Query historical network data
  - Cell metadata retrieval
  - Time-range queries
  - Latency data access
  - Batch pagination support
- **Data models**: Network performance metrics (RSRP, SINR, RSRQ, latency, CQI, datarate)
- **Integration**: Provides data to ML service for training and processor for metadata

## Quick Start

```bash
docker-compose up -d
```

## Database Connections

- **InfluxDB**: Port 8086
- **ClickHouse**: Port 8123 (HTTP), Port 9000 (TCP)

## API Endpoints (Examples)

- `/cells` - List all cells
- `/cells/{cell_id}` - Get cell metadata
- `/latency` - Query latency data
- `/metrics` - Retrieve network metrics

## Data Flow

```
Kafka (network.data.processed) →
Storage Service →
├── InfluxDB (time-series metrics)
└── ClickHouse (analytical queries)
```

## Use Cases

- Historical network performance analysis
- Training data for ML models
- Cell metadata for processor windowing
- Time-range queries for analytics
- Real-time metrics dashboards

## Environment Variables

Configure via `.env`:
- Database connection strings
- Kafka broker URLs
- Service ports
- Retention policies

## Testing

```bash
pytest tests/
```
