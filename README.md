# Data Storage Service

Consumes network data and decisions from Kafka and stores them in InfluxDB and ClickHouse, exposing REST APIs for querying.

## How It Works

1. Subscribes to Kafka topics: `network.data.ingested`, `network.data.processed`, `network.decisions`
2. Routes each message to the appropriate database:
   - `network.data.ingested` → **InfluxDB** (raw time-series metrics)
   - `network.data.processed` → **ClickHouse** (aggregated analytics)
   - `network.decisions` → **ClickHouse** (compressed decision data)
3. Exposes REST API for querying stored data

## Databases

| Database | Port | Used For |
|---|---|---|
| InfluxDB | `8086` | Raw time-series metrics |
| ClickHouse | `8123` (HTTP), `9000` (TCP) | Processed analytics + decisions |

## API

Base path: `/api/v1`

| Method | Endpoint | Database | Description |
|---|---|---|---|
| `GET` | `/cell` | InfluxDB | List all cell IDs |
| `GET` | `/raw` | InfluxDB | Query raw ingested metrics |
| `GET` | `/raw/fields` | InfluxDB | List available metric fields |
| `GET` | `/processed` | ClickHouse | Query processed/aggregated data |
| `GET` | `/processed/example` | ClickHouse | Example response schema |
| `GET` | `/decisions` | ClickHouse | Query decision data |

### `/decisions` Query Parameters

| Parameter | Required | Description |
|---|---|---|
| `start_time` | yes | Unix timestamp (seconds) |
| `end_time` | yes | Unix timestamp (seconds) |
| `cell_id` | no | Filter by cell (omit for all cells) |
| `offset` | no | Pagination offset (default: 0) |
| `limit` | no | Max records (default: 100, max: 1000) |

**Response** — returns compressed decision records:
```json
[
  {
    "cell_id": 1,
    "id": 1,
    "timestamp": "2026-03-20T21:06:33.482000",
    "compression_method": "gzip",
    "compressed_data": "<base64-encoded gzip>"
  }
]
```

To decompress: base64-decode `compressed_data`, then gzip-decompress.

## Configuration

| Variable | Default | Description |
|---|---|---|
| `KAFKA_HOST` | `kafka` | Kafka broker hostname |
| `KAFKA_PORT` | `9092` | Kafka broker port |
| `INFLUX_URL` | `http://influxdb:8086` | InfluxDB URL |
| `INFLUX_TOKEN` | — | InfluxDB auth token |
| `INFLUX_ORG` | — | InfluxDB organization |
| `INFLUX_BUCKET` | — | InfluxDB bucket |
| `CLICKHOUSE_HOST` | `clickhouse` | ClickHouse hostname |
| `CLICKHOUSE_PORT` | — | ClickHouse HTTP port |
| `CLICKHOUSE_USER` | — | ClickHouse user |
| `CLICKHOUSE_PASSWORD` | — | ClickHouse password |

## Running

```bash
docker compose up --build
```
