# TEI Helm chart

This chart installs the Tandemn Efficiency Index collector, API, and dashboard. Prometheus, NVIDIA
dcgm-exporter, and PostgreSQL are bundled unless they are explicitly disabled.

Install a self-contained evaluation release:

```shell
helm upgrade --install tei \
  oci://ghcr.io/tandemn-labs/charts/tei \
  --version 0.2.2 \
  --namespace tandemn-system \
  --create-namespace
```

For production, pull and unpack the chart, copy `values-production.yaml`, and replace its namespaces,
service URLs, Secret names, Ingress class, hostname, and TLS Secret:

```shell
helm pull oci://ghcr.io/tandemn-labs/charts/tei --version 0.2.2 --untar
cp tei/values-production.yaml tei-production.yaml
```

Install with the edited values file:

```shell
helm upgrade --install tei \
  oci://ghcr.io/tandemn-labs/charts/tei \
  --version 0.2.2 \
  --namespace tandemn-system \
  --create-namespace \
  --values tei-production.yaml
```

TEI requires read access to supported Dynamo or Ray workload CRDs and Pods. It does not install GPU
drivers, the NVIDIA device plugin, Dynamo, KubeRay, or inference workloads.
