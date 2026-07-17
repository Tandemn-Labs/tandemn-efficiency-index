import json
from datetime import UTC, datetime
from urllib import parse

from tandemn_efficiency_index.prometheus.client import PrometheusClient


class FakeTransport:
    def __init__(self, response: dict[str, object]) -> None:
        self.response = response
        self.requests: list[tuple[str, bytes, float]] = []

    def post(self, url: str, data: bytes, timeout_seconds: float) -> bytes:
        self.requests.append((url, data, timeout_seconds))
        return json.dumps(self.response).encode()


def test_queries_and_parses_prometheus_range_matrix() -> None:
    transport = FakeTransport(
        {
            "status": "success",
            "data": {
                "resultType": "matrix",
                "result": [
                    {
                        "metric": {
                            "__name__": "DCGM_FI_DEV_GPU_UTIL",
                            "namespace": "inference",
                            "pod": "qwen-worker-abc12",
                        },
                        "values": [
                            [1784232000, "81.5"],
                            [1784232005, "NaN"],
                            [1784232007, "+Inf"],
                            [1784232010, "82"],
                        ],
                    }
                ],
            },
        }
    )
    client = PrometheusClient("http://prometheus.monitoring.svc:9090/", transport=transport)
    start = datetime.fromtimestamp(1784232000, UTC)
    end = datetime.fromtimestamp(1784232010, UTC)

    result = client.query_range("DCGM_FI_DEV_GPU_UTIL", start, end, 10)

    assert result[0].labels["pod"] == "qwen-worker-abc12"
    assert [sample.value for sample in result[0].samples] == [81.5, 82.0]
    url, data, timeout = transport.requests[0]
    assert url == "http://prometheus.monitoring.svc:9090/api/v1/query_range"
    assert parse.parse_qs(data.decode())["step"] == ["10"]
    assert timeout == 10.0
