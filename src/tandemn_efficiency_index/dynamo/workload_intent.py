"""Normalize DynamoGraphDeploymentRequest workload and SLO intent."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from tandemn_efficiency_index.models.workload import WorkloadIntent

DYNAMO_DGDR_PLURAL = "dynamographdeploymentrequests"
DYNAMO_DGDR_API_VERSIONS = ("v1beta1", "v1alpha1")
DGDR_NAME_LABEL = "dgdr.nvidia.com/name"
DGDR_NAMESPACE_LABEL = "dgdr.nvidia.com/namespace"

WORKLOAD_FIELDS = {
    "isl": "input_sequence_length",
    "osl": "output_sequence_length",
    "requestRate": "request_rate",
    "request_rate": "request_rate",
    "concurrency": "concurrency",
}
SLO_FIELDS = {
    "ttft": "ttft_ms",
    "itl": "itl_ms",
    "e2eLatency": "e2e_latency_ms",
    "e2e_latency": "e2e_latency_ms",
}
HARDWARE_FIELDS = {
    "gpuSku": "gpu_sku",
    "gpu_sku": "gpu_sku",
    "vramMb": "vram_mb",
    "vram_mb": "vram_mb",
    "totalGpus": "total_gpus",
    "total_gpus": "total_gpus",
    "numGpusPerNode": "gpus_per_node",
    "num_gpus_per_node": "gpus_per_node",
    "interconnect": "interconnect",
    "rdma": "rdma",
}


def parse_dgdr_intent(resource: Mapping[str, Any]) -> WorkloadIntent:
    """Parse one DGDR into the shared declared-intent shape."""
    if resource.get("kind") not in (None, "DynamoGraphDeploymentRequest"):
        raise ValueError("Resource is not a DynamoGraphDeploymentRequest")

    metadata = _mapping(resource.get("metadata"))
    spec = _mapping(resource.get("spec"))
    namespace = _required_text(metadata, "namespace")
    name = _required_text(metadata, "name")
    api_version = str(resource.get("apiVersion") or "nvidia.com/unknown")

    if api_version.endswith("/v1alpha1"):
        workload, slo, hardware = _legacy_intent(spec)
    else:
        workload = _normalized(_mapping(spec.get("workload")), WORKLOAD_FIELDS)
        slo = _normalized(_mapping(spec.get("sla")), SLO_FIELDS)
        hardware = _normalized(_mapping(spec.get("hardware")), HARDWARE_FIELDS)

    return WorkloadIntent(
        source_kind="DynamoGraphDeploymentRequest",
        source_namespace=namespace,
        source_name=name,
        source_uid=_text(metadata.get("uid")),
        api_version=api_version,
        model_id=_text(spec.get("model")),
        backend=_text(spec.get("backend")),
        workload=workload,
        slo=slo,
        hardware=hardware,
    )


def dgdr_reference(resource: Mapping[str, Any]) -> tuple[str, str] | None:
    """Return the exact DGDR namespace and name labels on a generated DGD."""
    metadata = _mapping(resource.get("metadata"))
    labels = _mapping(metadata.get("labels"))
    namespace = _text(labels.get(DGDR_NAMESPACE_LABEL))
    name = _text(labels.get(DGDR_NAME_LABEL))
    if namespace is None or name is None:
        return None
    return namespace, name


def _legacy_intent(
    spec: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    profiling = _mapping(spec.get("profilingConfig"))
    config = _mapping(profiling.get("config"))
    legacy_sla = _mapping(config.get("sla"))
    workload = _normalized(_mapping(config.get("workload")), WORKLOAD_FIELDS)
    workload.update(_normalized(legacy_sla, WORKLOAD_FIELDS))
    slo = _normalized(legacy_sla, SLO_FIELDS)
    hardware = _normalized(_mapping(config.get("hardware")), HARDWARE_FIELDS)
    return workload, slo, hardware


def _normalized(values: Mapping[str, Any], fields: Mapping[str, str]) -> dict[str, Any]:
    return {
        normalized_name: values[source_name]
        for source_name, normalized_name in fields.items()
        if source_name in values
    }


def _required_text(values: Mapping[str, Any], key: str) -> str:
    value = _text(values.get(key))
    if value is None:
        raise ValueError(f"Required field is missing: {key}")
    return value


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _text(value: Any) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None
