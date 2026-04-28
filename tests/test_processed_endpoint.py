from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

SAMPLE_START_TIME = int(datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc).timestamp())
SAMPLE_END_TIME = int(datetime(2024, 1, 1, 13, 0, 0, tzinfo=timezone.utc).timestamp())


@pytest.fixture
def mock_clickhouse_service():
    service_mock = MagicMock()
    with patch("src.services.databases.ClickHouse.get_service", return_value=service_mock):
        yield service_mock


@pytest.fixture
def test_client(mock_clickhouse_service):
    from fastapi import FastAPI
    from src.routers.v1 import v1_router

    app = FastAPI()
    app.include_router(v1_router, prefix="/api/v1", tags=["v1"])
    return TestClient(app)


@pytest.fixture
def sample_row():
    return {
        "window_start": datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        "window_end": datetime(2024, 1, 1, 12, 1, 0, tzinfo=timezone.utc),
        "window_duration_seconds": 60,
        "sample_count": 10,
        "snssai_sst": "1",
        "snssai_sd": "000001",
        "dnn": "internet",
        "event": "PERF_DATA",
        "ue_tags": {},
        "thrputUl_mbps_mean": 11.5,
        "pdb_ms_mean": 25.0,
    }


REQUIRED_PARAMS = {
    "start_time": SAMPLE_START_TIME,
    "end_time": SAMPLE_END_TIME,
    "snssai_sst": "1",
    "dnn": "internet",
}


class TestProcessedEndpoint:
    def test_success(self, test_client, mock_clickhouse_service, sample_row):
        mock_clickhouse_service.query_processed.return_value = [sample_row]

        response = test_client.get("/api/v1/processed", params=REQUIRED_PARAMS)

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["snssai_sst"] == "1"
        assert data[0]["dnn"] == "internet"
        assert data[0]["thrputUl_mbps_mean"] == 11.5

    def test_empty_result(self, test_client, mock_clickhouse_service):
        mock_clickhouse_service.query_processed.return_value = []

        response = test_client.get("/api/v1/processed", params=REQUIRED_PARAMS)

        assert response.status_code == 200
        assert response.json() == []

    def test_missing_snssai_sst(self, test_client, mock_clickhouse_service):
        params = {k: v for k, v in REQUIRED_PARAMS.items() if k != "snssai_sst"}
        response = test_client.get("/api/v1/processed", params=params)
        assert response.status_code == 200

    def test_missing_dnn(self, test_client, mock_clickhouse_service):
        params = {k: v for k, v in REQUIRED_PARAMS.items() if k != "dnn"}
        response = test_client.get("/api/v1/processed", params=params)
        assert response.status_code == 200

    def test_missing_start_time(self, test_client, mock_clickhouse_service):
        params = {k: v for k, v in REQUIRED_PARAMS.items() if k != "start_time"}
        response = test_client.get("/api/v1/processed", params=params)
        assert response.status_code == 422

    def test_missing_end_time(self, test_client, mock_clickhouse_service):
        params = {k: v for k, v in REQUIRED_PARAMS.items() if k != "end_time"}
        response = test_client.get("/api/v1/processed", params=params)
        assert response.status_code == 422

    def test_optional_snssai_sd(self, test_client, mock_clickhouse_service, sample_row):
        mock_clickhouse_service.query_processed.return_value = [sample_row]

        response = test_client.get(
            "/api/v1/processed",
            params={**REQUIRED_PARAMS, "snssai_sd": "000001"},
        )

        assert response.status_code == 200
        call_kwargs = mock_clickhouse_service.query_processed.call_args[1]
        assert call_kwargs["snssai_sd"] == "000001"

    def test_optional_event_filter(self, test_client, mock_clickhouse_service, sample_row):
        mock_clickhouse_service.query_processed.return_value = [sample_row]

        response = test_client.get(
            "/api/v1/processed",
            params={**REQUIRED_PARAMS, "event": "PERF_DATA"},
        )

        assert response.status_code == 200
        call_kwargs = mock_clickhouse_service.query_processed.call_args[1]
        assert call_kwargs["event"] == "PERF_DATA"

    def test_optional_window_duration(self, test_client, mock_clickhouse_service, sample_row):
        mock_clickhouse_service.query_processed.return_value = [sample_row]

        response = test_client.get(
            "/api/v1/processed",
            params={**REQUIRED_PARAMS, "window_duration_seconds": 60},
        )

        assert response.status_code == 200
        call_kwargs = mock_clickhouse_service.query_processed.call_args[1]
        assert call_kwargs["window_duration_seconds"] == 60

    def test_pagination_passed_to_service(self, test_client, mock_clickhouse_service, sample_row):
        mock_clickhouse_service.query_processed.return_value = [sample_row]

        response = test_client.get(
            "/api/v1/processed",
            params={**REQUIRED_PARAMS, "offset": 50, "limit": 25},
        )

        assert response.status_code == 200
        call_kwargs = mock_clickhouse_service.query_processed.call_args[1]
        assert call_kwargs["offset"] == 50
        assert call_kwargs["limit"] == 25

    def test_default_pagination(self, test_client, mock_clickhouse_service, sample_row):
        mock_clickhouse_service.query_processed.return_value = [sample_row]

        test_client.get("/api/v1/processed", params=REQUIRED_PARAMS)

        call_kwargs = mock_clickhouse_service.query_processed.call_args[1]
        assert call_kwargs["offset"] == 0
        assert call_kwargs["limit"] == 100

    def test_invalid_limit_too_large(self, test_client, mock_clickhouse_service):
        response = test_client.get(
            "/api/v1/processed",
            params={**REQUIRED_PARAMS, "limit": 5000},
        )
        assert response.status_code == 422

    def test_invalid_negative_offset(self, test_client, mock_clickhouse_service):
        response = test_client.get(
            "/api/v1/processed",
            params={**REQUIRED_PARAMS, "offset": -10},
        )
        assert response.status_code == 422

    def test_invalid_start_time_format(self, test_client, mock_clickhouse_service):
        params = {**REQUIRED_PARAMS, "start_time": "not-a-timestamp"}
        response = test_client.get("/api/v1/processed", params=params)
        assert response.status_code == 422

    def test_service_error_returns_500(self, test_client, mock_clickhouse_service):
        mock_clickhouse_service.query_processed.side_effect = Exception("DB error")

        response = test_client.get("/api/v1/processed", params=REQUIRED_PARAMS)

        assert response.status_code == 500
        assert "DB error" in response.json()["detail"]

    def test_multiple_results(self, test_client, mock_clickhouse_service, sample_row):
        mock_clickhouse_service.query_processed.return_value = [sample_row] * 5

        response = test_client.get("/api/v1/processed", params=REQUIRED_PARAMS)

        assert response.status_code == 200
        assert len(response.json()) == 5

    def test_ue_tags_in_response(self, test_client, mock_clickhouse_service):
        row = {
            "window_start": datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc),
            "window_end": datetime(2024, 1, 1, 12, 1, tzinfo=timezone.utc),
            "window_duration_seconds": 60,
            "sample_count": 5,
            "snssai_sst": "1",
            "snssai_sd": "000001",
            "dnn": "internet",
            "event": "PERF_DATA",
            "ue_tags": {"ueIpv4Addr": "10.0.0.1"},
        }
        mock_clickhouse_service.query_processed.return_value = [row]

        response = test_client.get("/api/v1/processed", params=REQUIRED_PARAMS)

        assert response.status_code == 200
        assert response.json()[0]["ue_tags"] == {"ueIpv4Addr": "10.0.0.1"}


class TestFieldsEndpoint:
    def test_fields_success(self, test_client, mock_clickhouse_service):
        mock_clickhouse_service.get_metric_event_map.return_value = {
            "thrputUl_mbps_mean": ["PERF_DATA"],
            "pdb_ms_mean": ["PERF_DATA"],
            "speed_mean": ["UE_MOBILITY"],
        }

        response = test_client.get("/api/v1/processed/fields")

        assert response.status_code == 200
        data = response.json()
        assert data["thrputUl_mbps_mean"] == ["PERF_DATA"]
        assert data["speed_mean"] == ["UE_MOBILITY"]

    def test_fields_empty(self, test_client, mock_clickhouse_service):
        mock_clickhouse_service.get_metric_event_map.return_value = {}

        response = test_client.get("/api/v1/processed/fields")

        assert response.status_code == 200
        assert response.json() == {}

    def test_fields_service_error(self, test_client, mock_clickhouse_service):
        mock_clickhouse_service.get_metric_event_map.side_effect = Exception("view missing")

        response = test_client.get("/api/v1/processed/fields")

        assert response.status_code == 500
        assert "view missing" in response.json()["detail"]
