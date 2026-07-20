# Tandemn Efficiency Index

Tandemn Efficiency Index (TEI) observes NVIDIA GPU inference workloads in Kubernetes. It discovers
NVIDIA Dynamo and Ray Serve workloads, joins their configuration with Prometheus and NVIDIA DCGM
telemetry, and exposes a dashboard and JSON API.

TEI is currently an observability MVP. It does not calculate a final efficiency score or send
cluster data to Tandemn.

## Requirements

- A Kubernetes cluster with NVIDIA GPU nodes, drivers, and the NVIDIA device plugin.
- NVIDIA Dynamo or KubeRay with a running `DynamoGraphDeployment` or `RayService`.
- vLLM metrics, or SGLang started with `--enable-metrics`.
- Helm and `kubectl`.
- Permission to create cluster-scoped read-only RBAC resources.

TEI does not install GPU drivers, the NVIDIA device plugin, Dynamo, KubeRay, or inference workloads.

## Install an evaluation stack

The default chart installs TEI with dedicated Prometheus, NVIDIA dcgm-exporter, and PostgreSQL
services. It requires a default StorageClass and provisions 28 GiB of persistent storage.

```shell
helm upgrade --install tei \
  oci://ghcr.io/tandemn-labs/charts/tei \
  --version 0.2.0 \
  --namespace tei-system \
  --create-namespace \
  --set prometheus.enabled=true
```

Wait for TEI to become ready:

```shell
kubectl --namespace tei-system rollout status deployment/tei-tei --timeout=5m
```

## Open the dashboard

Keep this port-forward running:

```shell
kubectl --namespace tei-system port-forward service/tei-tei 8000:8000
```

Open <http://127.0.0.1:8000>. The first useful charts appear after Prometheus has collected
telemetry.

Check readiness or logs when the collector does not start:

```shell
curl http://127.0.0.1:8000/readyz
kubectl --namespace tei-system logs deployment/tei-tei
```

## Production configuration

Production installations should use managed Prometheus and PostgreSQL services, an existing
Kubernetes-aware dcgm-exporter, namespace-scoped workload access, API authentication, and TLS
Ingress.

Download the chart and copy its production profile:

```shell
helm pull oci://ghcr.io/tandemn-labs/charts/tei \
  --version 0.2.0 \
  --untar

cp tei/values-production.yaml tei-production.yaml
```

Edit `tei-production.yaml` and replace every example namespace, URL, Secret name, Ingress class,
hostname, and TLS Secret. The referenced PostgreSQL Secret must contain its connection string under
`dsn`; the API token Secret must contain the bearer token under `token`.

Install the configured release:

```shell
helm upgrade --install tei \
  oci://ghcr.io/tandemn-labs/charts/tei \
  --version 0.2.0 \
  --namespace tei-system \
  --create-namespace \
  --values tei-production.yaml
```

The external Prometheus server must already scrape DCGM and the relevant Dynamo or Ray Pods. Review
all chart settings with:

```shell
helm show values oci://ghcr.io/tandemn-labs/charts/tei --version 0.2.0
```

## Private packages

If the GHCR packages are private, authenticate Helm before downloading the chart:

```shell
helm registry login ghcr.io
```

The cluster also needs a GHCR image pull Secret in `tei-system`. Pass it to Helm with:

```shell
--set 'imagePullSecrets[0].name=YOUR_SECRET_NAME'
```

Public packages require neither step.

## Upgrade

Review the release notes and reuse the same values file with the new version:

```shell
helm upgrade tei \
  oci://ghcr.io/tandemn-labs/charts/tei \
  --version NEW_VERSION \
  --namespace tei-system \
  --values tei-production.yaml
```

## Uninstall

```shell
helm uninstall tei --namespace tei-system
```

Helm may retain Prometheus and PostgreSQL persistent volume claims. Review them before deleting any
stored data:

```shell
kubectl --namespace tei-system get persistentvolumeclaims
```

For source development, clone the repository, run `helm dependency build ./helm/tei`, and install
the local chart path. See [`OBSERVABILITY.md`](OBSERVABILITY.md) for telemetry and API contracts.
