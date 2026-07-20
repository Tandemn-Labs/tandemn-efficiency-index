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

Installation has two parts: Helm deploys the TEI services into Kubernetes, and a GitHub Release
provides the TUI binary that runs on your computer.

## 1. Install TEI in the cluster

The default chart installs TEI with dedicated Prometheus, NVIDIA dcgm-exporter, and PostgreSQL
services. It requires a default StorageClass and provisions 28 GiB of persistent storage.

```shell
helm upgrade --install tei \
  oci://ghcr.io/tandemn-labs/charts/tei \
  --namespace tandemn-system \
  --create-namespace
```

Wait for TEI to become ready:

```shell
kubectl --namespace tandemn-system rollout status deployment/tei-tei --timeout=5m
```

## 2. Install the local TUI

On macOS or Linux, download and run the installer:

```shell
curl -fsSLO https://github.com/Tandemn-Labs/tandemn-efficiency-index/releases/latest/download/install.sh
sh install.sh
```

On Windows, run:

```powershell
Invoke-WebRequest https://github.com/Tandemn-Labs/tandemn-efficiency-index/releases/latest/download/install.ps1 -OutFile install.ps1
.\install.ps1
```

The installer detects the computer platform, verifies the downloaded TUI checksum, and installs it
in `$HOME/.local/bin`. Ensure that directory is on your `PATH`.

Run the TUI from a computer with `kubectl` access to the cluster:

```shell
tei --kube
```

The TUI opens a temporary Kubernetes port-forward to the TEI service in `tandemn-system`.

## Open the dashboard

Keep this port-forward running:

```shell
kubectl --namespace tandemn-system port-forward service/tei-tei 8000:8000
```

Open <http://127.0.0.1:8000>. The first useful charts appear after Prometheus has collected
telemetry.

Check readiness or logs when the collector does not start:

```shell
curl http://127.0.0.1:8000/readyz
kubectl --namespace tandemn-system logs deployment/tei-tei
```

## Production configuration

Production installations should use managed Prometheus and PostgreSQL services, an existing
Kubernetes-aware dcgm-exporter, namespace-scoped workload access, API authentication, and TLS
Ingress.

Download the chart and copy its production profile:

```shell
helm pull oci://ghcr.io/tandemn-labs/charts/tei \
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
  --namespace tandemn-system \
  --create-namespace \
  --values tei-production.yaml
```

The external Prometheus server must already scrape DCGM and the relevant Dynamo or Ray Pods. Review
all chart settings with:

```shell
helm show values oci://ghcr.io/tandemn-labs/charts/tei
```

Published releases are public. Customers do not need a GHCR login or image pull Secret.

## Upgrade

Review the release notes and reuse the same values file with the new version:

```shell
helm upgrade tei \
  oci://ghcr.io/tandemn-labs/charts/tei \
  --namespace tandemn-system \
  --values tei-production.yaml
```

## Uninstall

```shell
helm uninstall tei --namespace tandemn-system
```

Helm may retain Prometheus and PostgreSQL persistent volume claims. Review them before deleting any
stored data:

```shell
kubectl --namespace tandemn-system get persistentvolumeclaims
```

For source development, clone the repository, run `helm dependency build ./helm/tei`, and install
the local chart path. See [`OBSERVABILITY.md`](OBSERVABILITY.md) for telemetry and API contracts.
