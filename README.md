# Tandemn Efficiency Index

Tandemn Efficiency Index (TEI) observes NVIDIA GPU inference workloads running in Kubernetes. It
discovers NVIDIA Dynamo and Ray Serve workloads, joins their Kubernetes configuration with
Prometheus and NVIDIA DCGM telemetry, and exposes a local dashboard and JSON API.

TEI is currently an MVP for evaluating telemetry collection and workload attribution. It does not
calculate a final efficiency score or send cluster data to Tandemn.

## Distribution status

The repository contains a Helm chart, but no versioned chart or container image has been published
yet. Install TEI from a source checkout and publish its container image to a registry your cluster
can pull from.

## Requirements

- Kubernetes with NVIDIA GPU nodes, drivers, and the NVIDIA device plugin.
- NVIDIA Dynamo or KubeRay installed, with at least one `DynamoGraphDeployment` or `RayService`.
- vLLM metrics, or SGLang started with `--enable-metrics`.
- `docker` with Buildx, Helm, and `kubectl`.
- A container registry accessible from the cluster.
- Permission to create cluster-scoped RBAC resources.
- A default StorageClass with at least 28 GiB available for the bundled services.

The chart installs Prometheus, NVIDIA dcgm-exporter, and PostgreSQL by default. It does not install
GPU drivers, the NVIDIA device plugin, Dynamo, KubeRay, or inference workloads.

## Install

### 1. Build and publish the TEI image

Replace `registry.example.com` with a registry your cluster can access:

```shell
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  --tag registry.example.com/tandemn-efficiency-index:0.2.0 \
  --push .
```

If the registry is private, create an image pull Secret in `tei-system` and pass its name through
`imagePullSecrets` when installing the chart.

### 2. Install the Helm chart

```shell
helm dependency build ./helm/tei

helm upgrade --install tei ./helm/tei \
  --namespace tei-system \
  --create-namespace \
  --set image.repository=registry.example.com/tandemn-efficiency-index \
  --set image.tag=0.2.0
```

For a private registry, add:

```shell
--set 'imagePullSecrets[0].name=YOUR_SECRET_NAME'
```

The default installation creates:

- One TEI collector and dashboard Pod.
- Prometheus with a 20 GiB persistent volume and 24-hour retention.
- NVIDIA dcgm-exporter as a DaemonSet.
- PostgreSQL with an 8 GiB persistent volume.
- Read-only RBAC for supported workload CRDs and Pods.

### 3. Verify the installation

```shell
kubectl --namespace tei-system get pods
kubectl --namespace tei-system rollout status deployment/tei-tei --timeout=5m
```

If TEI is not ready, inspect its status and logs:

```shell
kubectl --namespace tei-system port-forward service/tei-tei 8000:8000
curl http://127.0.0.1:8000/readyz
kubectl --namespace tei-system logs deployment/tei-tei
```

## Open the dashboard

TEI uses a private `ClusterIP` Service. Keep this command running:

```shell
kubectl --namespace tei-system port-forward service/tei-tei 8000:8000
```

Open <http://127.0.0.1:8000>. TEI automatically refreshes discovered workloads and Pods every ten
seconds. The first useful charts appear after Prometheus has collected telemetry.

## Common configuration

| Need | Helm values |
| --- | --- |
| Limit discovery to namespaces | `discovery.mode=namespaces`, `discovery.namespaces={inference,research}` |
| Use existing Prometheus | `prometheus.enabled=false`, `prometheus.url=...` |
| Disable bundled dcgm-exporter | `dcgmExporter.enabled=false` |
| Use existing PostgreSQL | `postgresql.enabled=false`, `database.existingSecret.name=...` |
| Protect the API with a bearer token | `auth.bearerTokenSecret.name=...` |

An external Prometheus server must already scrape DCGM and the relevant Dynamo or Ray Pods. An
external PostgreSQL Secret must contain its connection string in a `dsn` key unless
`database.existingSecret.key` is changed.

All available settings are documented in [`helm/tei/values.yaml`](helm/tei/values.yaml).

## Uninstall

```shell
helm uninstall tei --namespace tei-system
```

Helm may leave the Prometheus and PostgreSQL persistent volume claims behind. Review them before
deleting any retained data:

```shell
kubectl --namespace tei-system get persistentvolumeclaims
```

See [`OBSERVABILITY.md`](OBSERVABILITY.md) for the telemetry, attribution, dashboard, and API
contracts.
