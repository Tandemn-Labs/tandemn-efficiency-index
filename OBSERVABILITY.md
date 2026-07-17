# TEI observability dashboard

The TEI observability layer presents the rolling `ClusterRecord` collected from Kubernetes and
Prometheus. It is intentionally part of the Python package: the collector, snapshot API, and static
dashboard can run in one Pod without a separate frontend service.

```text
Kubernetes CRDs ──> workload detection ─┐
Kubernetes Pods ──> pod attribution ────┼──> ClusterObserver ──> rolling ClusterRecord
Prometheus/DCGM + vLLM ──> metric collection ──┘                       │
                                                                         v
Browser <── static UI + /api/v1/snapshot <── ObservabilityServer + Runtime
```

## Start the dashboard

Create the observer from the exact Dynamo or Ray workloads being benchmarked, then give it to the
observability server:

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
        name="qwen-benchmark",
    ),
]
workloads = ClusterWorkloadDetector.from_in_cluster().detect(targets)
observer = ClusterObserver.from_in_cluster(
    prometheus_url="http://prometheus.monitoring.svc:9090",
    workloads=workloads,
)

ObservabilityServer(observer, host="0.0.0.0", port=8000).serve_forever()
```

The runtime collects immediately and then follows the observer's sample cadence. The browser
refreshes the snapshot every ten seconds and supports 15-minute, 1-hour, 6-hour, and 24-hour views.

## API

`GET /api/v1/snapshot` returns the cluster summary, normalized workloads, attributed workers,
telemetry series, per-workload metric coverage, query-wide missing metrics, and unattributed series
grouped by attribution failure reason.

Query parameters:

- `window_seconds`: sample window ending at `updated_at`; use `0` for the whole retained window.
- `max_points`: maximum points returned per series. The first and last values plus bucket minima and
  maxima are retained so brief spikes and dips survive downsampling.

The default response is one hour with at most 180 points per series. This keeps the live payload
bounded even when the in-memory observer retains 24 hours of ten-second samples.

## Prometheus attribution contract

TEI polls Prometheus range queries every ten seconds. DCGM series are joined to worker Pods through
the `exported_pod` label produced when dcgm-exporter Kubernetes PodResources attribution is enabled.
The scrape's `pod` label identifies the exporter Pod and must not be treated as the GPU owner. Set
`DCGM_EXPORTER_KUBERNETES=true` and preserve the owner as `exported_pod` during Prometheus relabeling.

vLLM latency, throughput, queue, KV-cache, token-length, and prefill/decode metrics are queried once
per currently running worker with `pod="<worker-pod-name>"`. Historical Pods remain in the cluster
record for reporting but are not queried after they disappear. Empty, `NaN`, and infinite samples
are ignored. Config-gated DCGM metrics such as NVLink remain listed as missing when Prometheus does
not expose them, without preventing other metrics from being collected in the same tick.

## UI mapping

The interface uses Koi's typography, grayscale surfaces, 14px bordered frames, pill controls,
compact two-line rows, and hoverable SVG time series. It is a focused observability canvas rather
than a product navigation shell.

The main performance grid contains only signals that explain workload efficiency and GPU pressure:

- GPU utilization.
- SM active time and occupancy.
- Tensor pipe activity.
- Framebuffer usage.
- Average GPU power.

The operational-health rail keeps maximum GPU temperature, aggregate PCIe traffic, XID state, and
displayed-signal availability visible without allocating full charts to them. The worker frame maps
GPU count, utilization, framebuffer pressure, and freshness back to each attributed Pod. A telemetry
coverage table exposes every configured DCGM metric, including framebuffer reservation, clocks,
graphics and DRAM activity, PCIe, and NVLink. It reports expected versus reporting GPUs, series and
sample counts, value ranges, freshness, and complete/partial/missing state.

Expandable diagnostic frames expose normalized component configuration, GPU and MIG scope,
Prometheus labels, source Pod identity, attribution method, and unattributed series.

## Benchmark profile

Chart indicators use a hardcoded `generic LLM inference` profile. They are directional heuristics,
not a TEI score or a substitute for model- and GPU-specific known-good runs.

| Signal | Good | Watch | Needs attention |
| --- | --- | --- | --- |
| GPU utilization | `>= 80%` | `50–80%` | `< 50%` |
| SM active | `>= 80%` | `50–80%` | `< 50%` |
| Tensor activity | `>= 50%` | `20–50%` | `< 20%` |
| GPU memory pressure | `60–90%` | `40–60%` or `90–95%` | `< 40%` or `> 95%` |

SM occupancy is labeled `Context` because occupancy depends on kernel resources and bottleneck type;
higher occupancy does not always mean better performance. GPU power is also contextual until TEI
collects the device power limit and GPU model. Temperature uses `< 80°C` as good, `80–90°C` as
watch, and `>= 90°C` as needs attention. Any non-zero XID value needs attention.

The SM-active boundaries follow NVIDIA DCGM guidance. The other bands are TEI defaults chosen for
the first LLM inference profile and should be calibrated from benchmark history. The dashboard
draws healthy regions inside percentage plots and updates the status badge as the user hovers each
historical sample.

## Layout contract

The frame constraints are deliberate so changing job or metric volume does not change the visual
hierarchy:

- Canvas: centered, fluid width with a 1560px maximum and 48px desktop side gutters.
- Run context: one 142px-minimum frame for identity, four execution facts, and an optional workload
  selector.
- Performance: a two-column grid with an 18px gutter; each chart frame is at least 365px high and
  reserves 220px for the plot.
- Operational health: one 136px-minimum horizontal frame with four equal signal cells.
- Telemetry coverage: a scrollable table covering every configured metric and per-GPU completeness.
- Workers: one 280px-minimum frame; its row viewport is capped at 340px and scrolls when Pod count
  grows.
- Diagnostics: expandable workload configuration, GPU scope, and unattributed telemetry frames.
- Vertical rhythm: 46px between major sections and 18px between related frames.

The interface does not calculate a TEI score yet. It presents raw and aggregated evidence without
implying a benchmark formula that is not present in the current data model.
