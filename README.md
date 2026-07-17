# Tandemn Efficiency Index

Tandemn Efficiency Index (TEI) is a Kubernetes-native observability and benchmarking foundation for
GPU inference workloads. It discovers how a model-serving job is configured, attributes GPU and
inference telemetry to the worker Pods that produced it, and maintains a rolling per-job record that
can be inspected through a local dashboard and JSON API.

The project is designed to move beyond a cluster-wide utilization chart. Its data model connects:

- The deployed model and serving-engine configuration.
- Parallelism, batching, KV-cache, scheduling, and placement settings.
- Kubernetes worker identity and runtime role.
- Physical GPU metrics from NVIDIA DCGM.
- Worker-scoped vLLM latency, throughput, queue, token, and KV-cache signals.

This joined record is the input required for future TEI recommendations such as identifying
underutilized hardware, memory pressure, poor workload placement, inefficient parallelism, and
prefill/decode imbalance.

## Current project status

TEI is currently an MVP intended for Tandemn developers, infrastructure teams, and early design
partners validating NVIDIA GPU inference workloads on Kubernetes. The collection path, normalized
models, rolling observer, API, dashboard, and deterministic smoke-test environment are implemented.

The repository is not yet a finished production distribution. It does not currently include:

- A Helm chart or production Kubernetes manifests.
- Durable telemetry persistence across TEI Pod restarts.
- The hosted Tandemn account, API-key, upload, or report workflow.
- Koi simulation, efficiency scoring, or automated recommendations.
- Cost-per-token, SLO-margin, canonical chain assignment, or resource-map pricing.
- Authentication or authorization for the local dashboard server.

The current dashboard reports observed evidence and directional benchmark bands. It does not claim
to calculate a final Tandemn Efficiency Index score.

## Who TEI is for today

The current implementation is useful for teams that meet most of the following conditions:

- Run LLM inference on NVIDIA GPUs in Kubernetes.
- Deploy models with NVIDIA Dynamo or Ray Serve.
- Operate Prometheus and NVIDIA dcgm-exporter in the cluster.
- Use vLLM when worker-level inference telemetry is required.
- Can provide the Kubernetes namespace and resource name for each workload being observed.
- Want to validate workload attribution and the quality of collected telemetry before running
  higher-level optimization analysis.

TEI is not currently a general-purpose Kubernetes observability replacement. It is narrowly focused
on collecting the workload and performance context needed to explain and improve model-serving
efficiency.

## Supported frameworks and versions

### Workload discovery

| Runtime | Kubernetes resource | API versions | Parsed serving backends |
| --- | --- | --- | --- |
| NVIDIA Dynamo | `DynamoGraphDeployment` | `nvidia.com/v1beta1`, `nvidia.com/v1alpha1` | vLLM, SGLang, TensorRT-LLM |
| Ray Serve | `RayService` | `ray.io/v1`, `ray.io/v1alpha1` | vLLM, SGLang |

Both detectors normalize their resource into the same `Workload` shape. TEI currently requires an
exact namespace and resource name for every DynamoGraphDeployment or RayService it should observe.

### Telemetry

| Source | Current support |
| --- | --- |
| NVIDIA DCGM | Per-GPU utilization, memory, power, thermal, clocks, engine activity, PCIe, NVLink, and XID series |
| vLLM | Per-worker TTFT, TPOT, throughput, live batch, queue depth, KV-cache, token lengths, prefill/decode iteration rates, and pressure signals |
| Kubernetes | Worker Pod UID, namespace, name, node, container names, runtime instance, runtime role, and first/last observation times |

DCGM metrics are supported for both Dynamo and Ray workloads. vLLM inference queries only run for
workloads whose normalized backend is `vllm`.

## How it works

```text
DynamoGraphDeployment ──┐
                        ├──> normalized Workload ──┐
RayService ─────────────┘                          │
                                                  ├──> per-job ClusterRecord
Kubernetes worker Pods ──> worker attribution ────┤         │
                                                  │         v
Prometheus/DCGM ─────────> per-GPU time series ───┤   JSON snapshot API
Prometheus/vLLM ─────────> per-worker time series ┘         │
                                                            v
                                                     local dashboard
```

At startup, the runtime-specific detectors parse the configured workloads. The observer then:

1. Finds the currently running worker Pods using labels produced by Dynamo or Ray.
2. Polls Prometheus on a ten-second cadence.
3. Joins DCGM GPU series to worker Pods using dcgm-exporter's Kubernetes ownership labels.
4. Queries vLLM inference metrics once per current worker Pod.
5. Appends unseen samples to the owning job without rebuilding the cluster record.
6. Retains rolling time series for 24 hours and removes expired samples from the front.
7. Serves bounded, downsampled snapshots to the local dashboard.

If a worker stops, its historical data remains in its job record but TEI stops querying new
worker-scoped metrics for it. GPUs without a recognized owner remain visible as unattributed
telemetry instead of disappearing from cluster-level capacity accounting.

## Data model

The central record is job-first:

```text
ClusterRecord
├── jobs[workload_id]
│   ├── workload
│   │   ├── model and backend
│   │   ├── normalized components
│   │   ├── parallelism and engine settings
│   │   └── Kubernetes Pod selectors
│   ├── workers[pod_uid]
│   └── telemetry
│       └── series[series_id]
│           ├── metric name
│           ├── workload, Pod, node, GPU, and runtime scope
│           └── rolling timestamp/value samples
└── unattributed_telemetry
```

Each metric series has a stable identity derived from its metric name, scope, and remaining
Prometheus labels. Overlapping Prometheus range queries are safe because samples at or before the
latest stored timestamp are ignored.

## Prometheus requirements

TEI expects an existing Prometheus HTTP endpoint. Prometheus Operator is supported, but TEI only
depends on the standard Prometheus HTTP API and does not require a specific Operator installation.

The default collection interval is ten seconds. The Prometheus scrape interval should also be ten
seconds to avoid repeated gaps or unnecessary interpolation.

### DCGM Pod attribution

dcgm-exporter must run with Kubernetes PodResources attribution enabled:

```text
DCGM_EXPORTER_KUBERNETES=true
```

The exporter identifies the Pod that owns each allocated GPU. When Prometheus adds its own scrape
target `pod` label, the GPU owner's label must be preserved as `exported_pod`. TEI treats:

- `pod` as the dcgm-exporter Pod.
- `exported_pod` as the workload Pod that owns the GPU.
- `exported_namespace` as the optional namespace of that owner.

Without `exported_pod`, physical GPU telemetry is still retained, but it may remain unattributed.
The dashboard and API expose attribution reasons so this configuration problem is visible.

### vLLM metrics

TEI scopes every vLLM query to one worker with `pod="<worker-pod-name>"`. This prevents multiple
data-parallel replicas from being combined into one deployment-wide value. Rate and histogram
queries use the same one-minute and five-minute windows as the validated Tandemn collector.

Empty results and non-finite Prometheus values are ignored. A failed or unavailable metric is
recorded as missing without cancelling the other queries in the collection tick. Topology- and
configuration-dependent metrics such as NVLink are therefore optional.

## Development setup

Requirements:

- Python 3.12 or newer.
- [`uv`](https://docs.astral.sh/uv/).
- Access to a Kubernetes cluster for live integration testing.
- A reachable Prometheus endpoint for live telemetry.

Install the project and development dependencies:

```shell
uv sync --group dev
```

Run validation:

```shell
uv run ruff format --check src tests smoketest
uv run ruff check src tests smoketest
uv run mypy src smoketest
uv run pytest
uv run pytest smoketest
```

Integration tests must use local containers or test clusters, never real cloud accounts.

## Running the local dashboard with mock data

The repository includes a removable smoke-test environment representing a customer running Qwen,
GLM, and DeepSeek Dynamo jobs. It contains 23 minutes and 17 seconds of deterministic DCGM history,
complete, partial, missing, and unattributed telemetry cases.

```shell
uv run python -m smoketest.run_dashboard
```

Open `http://127.0.0.1:8000` and stop the server with `Ctrl+C`.

The complete smoke-test environment lives under `smoketest/`. Production code does not import it,
so it can be removed as one directory before a production release.

## Running against a cluster

Construct exact workload targets, detect their normalized configuration, and start the observer:

```python
from tandemn_efficiency_index.models.workload import WorkloadRuntime
from tandemn_efficiency_index.observability import ObservabilityServer
from tandemn_efficiency_index.observer import ClusterObserver
from tandemn_efficiency_index.workload_detection import (
    ClusterWorkloadDetector,
    WorkloadTarget,
)

targets = [
    WorkloadTarget(
        runtime=WorkloadRuntime.DYNAMO,
        namespace="inference",
        name="qwen-production",
    ),
]

workloads = ClusterWorkloadDetector.from_in_cluster().detect(targets)
observer = ClusterObserver.from_in_cluster(
    prometheus_url="http://prometheus-operated.monitoring.svc:9090",
    workloads=workloads,
)

ObservabilityServer(
    observer,
    host="0.0.0.0",
    port=8000,
).serve_forever()
```

The in-cluster service account needs permission to read the configured DynamoGraphDeployments or
RayServices and list their selected worker Pods.

## Snapshot API

The dashboard server exposes:

```text
GET /api/v1/snapshot
```

Query parameters:

- `window_seconds`: window ending at the record's latest update. Use `0` for all retained data.
- `max_points`: maximum serialized points per series.

The response contains:

- Cluster, worker, GPU, metric, and series counts.
- Normalized workload configuration and component settings.
- Attributed worker identities.
- Job-scoped GPU and inference time series.
- Per-GPU metric coverage.
- Unattributed telemetry and attribution failure reasons.
- Metrics unavailable during the most recent collection tick.

## Repository structure

```text
src/tandemn_efficiency_index/
├── dynamo/workload_detection.py   # DynamoGraphDeployment normalization
├── ray/workload_detection.py      # RayService normalization
├── models/                         # Shared workload, telemetry, and cluster records
├── prometheus/                     # Prometheus client, DCGM, and vLLM collectors
├── pod_attribution.py              # Runtime worker discovery
├── observer.py                     # Rolling collection orchestration
├── observability.py                # Snapshot API and local HTTP server
└── ui/                              # Local dashboard assets

tests/                               # Unit and contract tests
smoketest/                           # Removable dashboard-development fixture
OBSERVABILITY.md                     # Detailed dashboard and API behavior
```

## Privacy and data egress

The current implementation stores telemetry only in the TEI process and serves it locally. It does
not upload cluster data, call Tandemn-hosted services, or invoke an LLM. Those workflows will require
explicit product and security design before they are added.

## Near-term roadmap

The next major product layers are expected to include:

1. Helm packaging, RBAC, service discovery, and production configuration.
2. Durable local storage for the rolling observation window.
3. A lightweight local analysis report derived from the normalized workload and telemetry record.
4. An authenticated Tandemn-hosted upload and asynchronous analysis workflow.
5. Koi-backed simulation and recommendations covering hardware fit, cost, placement,
   parallelization, batching, and KV-cache behavior.

See [OBSERVABILITY.md](OBSERVABILITY.md) for the current dashboard, API, coverage, and benchmark
contracts.
