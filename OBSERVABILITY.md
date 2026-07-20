# TEI observability dashboard

The TEI observability layer generates a `ClusterRecord` report from separately managed Kubernetes
state and Prometheus time series. It is intentionally part of the Python package: the collector,
snapshot API, and static dashboard can run in one Pod without a separate frontend service.

```text
Kubernetes CRDs ──> workload detection ─┐
Kubernetes Pods ──> job-key attribution ┼──> PostgreSQL ObservationState
                                        │                 │
Prometheus time series ─────────────────┴──── range query ┤
                                                          v
                                                generated ClusterRecord
                                                          │
                                                snapshot API/browser
```

## Start the dashboard

Build the dependencies and install the Helm chart. The default chart includes Prometheus:

```shell
helm dependency build ./helm/tei
helm install tei ./helm/tei \
  --namespace tandemn-system \
  --create-namespace \
  --set image.repository=example.com/tandemn-efficiency-index \
  --set image.tag=0.2.2
```

The bundled Prometheus server retains 24 hours on a 20 GiB persistent volume and uses a ten-second
scrape interval. To reuse an existing server, set `prometheus.enabled=false` and `prometheus.url`.
Set `dcgmExporter.enabled=false` for an existing DCGM exporter, or `postgresql.enabled=false` plus
`database.existingSecret.name` for an external PostgreSQL DSN.

The runtime lists visible CRDs and workload instances immediately, caches supported CRD APIs, and
reconciles workloads and Pods every ten seconds. New workloads are added without a restart. Removed
workloads and historical Pod assignments remain in PostgreSQL for their observation. The browser
supports 15-minute, 1-hour, 6-hour, and 24-hour views by querying Prometheus for that selected range.

Cluster discovery uses the TEI ServiceAccount created by the chart. It requires read access to CRD
definitions plus cluster-wide `get` and `list` access to DynamoGraphDeployments,
DynamoGraphDeploymentRequests, RayServices, and Pods. Set `discovery.mode=namespaces` and populate
`discovery.namespaces` to bind workload and Pod access only in selected namespaces.

For DGDR-generated Dynamo jobs, each API workload contains a `declared_intent` object with
normalized workload assumptions (`input_sequence_length`, `output_sequence_length`, request rate,
and concurrency), latency SLOs, and requested hardware. Ray jobs use the same object for explicit
per-component replica, queue, concurrency, and autoscaling controls; RayService does not provide
DGDR-style TTFT or ITL targets, so its `slo` mapping remains empty.

## API

`GET /api/v1/snapshot` returns the cluster summary, normalized workloads, attributed workers,
telemetry series, per-workload metric coverage, query-wide missing metrics, and unattributed series
grouped by attribution failure reason.

Query parameters:

- `window_seconds`: sample window ending at `updated_at`; use `0` for the whole retained window;
  non-zero values cannot exceed 86,400 seconds.
- `max_points`: maximum points returned per series. The first and last values plus bucket minima and
  maxima are retained so brief spikes and dips survive downsampling. The accepted range is 2–2,000.

The default response is one hour with at most 180 points per series. PostgreSQL contains no
Prometheus samples; downsampling only bounds the generated response payload.

`GET /api/v1/status` reports lifecycle, collection, Prometheus, and storage state. `/healthz` checks
that the API lifecycle controller is healthy; an intentionally stopped collector remains healthy so
the CLI can start it again. `/readyz` additionally requires successful Kubernetes discovery,
Prometheus reachability, and writable PostgreSQL. Configure `auth.bearerTokenSecret` to require a
bearer token for `/api/*`; probe routes remain unauthenticated.

The service exposes idempotent, authenticated collection controls:

```text
POST /api/v1/observation/start
POST /api/v1/observation/stop
POST /api/v1/observation/restart
```

Stopping collection leaves the API, dashboard, observer state, and PostgreSQL connection available.
It stops periodic Kubernetes reconciliation but does not stop the separately deployed Prometheus or
dcgm-exporter. Snapshot requests can therefore still query retained Prometheus data with the last
known workload and Pod assignments. The lifecycle state is process-local in the MVP; restarting the
TEI Pod automatically starts collection and restores its active observation from PostgreSQL.

## Prometheus attribution contract

TEI runs Prometheus range queries when a report is requested. DCGM series are joined to worker Pods
through the `exported_pod` label produced when dcgm-exporter Kubernetes PodResources attribution is
enabled. The scrape's `pod` label identifies the exporter Pod and must not be treated as the GPU
owner. Set `DCGM_EXPORTER_KUBERNETES=true` and preserve the owner as `exported_pod` during
Prometheus relabeling.

TEI runs one bounded raw query for DCGM and the vLLM, SGLang, Dynamo, and Ray Serve families, then
eight PromQL normalization queries for p99 TTFT/TPOT, request and token throughput, queue/load, and
KV-cache usage. Returned series are attributed using DCGM's `exported_pod` ownership labels, direct
`(namespace, pod)` identity, or a Pod UID when available. The Pod assignment contributes the DGD
name for Dynamo or generated RayCluster name for Ray as `runtime_job_key`; every attributed metric
scope therefore links Prometheus back to the normalized workload. Series without a recognized Pod
identity remain visible as unattributed telemetry. Empty, `NaN`, and infinite samples are ignored.

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
coverage table exposes the required DCGM profile and reports expected versus reporting GPUs, series
and sample counts, freshness, and complete/partial/missing state. Optional raw DCGM signals such as
framebuffer reservation, SM occupancy, or NVLink remain visible when the exporter/GPU supports
them. Normalized inference coverage is reported separately.

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
