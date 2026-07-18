"""Fetch named DynamoGraphDeployments and extract workload configuration."""

# Output shape from DynamoWorkloadDetector.detect():
# {
#     "dynamo:<namespace>/<dgd-name>": {
#         "runtime": "dynamo",
#         "namespace": "<namespace>",
#         "name": "<dgd-name>",
#         "uid": "<kubernetes-uid-or-null>",
#         "api_version": "nvidia.com/<version>",
#         "model_id": "<model-name>",
#         "backend": "<vllm|sglang|trtllm>",
#         "disaggregated": <bool>,
#         "total_gpus": <int>,
#         "declared_intent": <normalized-dgdr-intent-or-null>,
#         "pod_selectors": [
#             {
#                 "runtime_instance": "<dgd-name>",
#                 "runtime_state": "active",
#                 "match_labels": {
#                     "nvidia.com/dynamo-graph-deployment-name": "<dgd-name>",
#                     "nvidia.com/dynamo-component-type": "worker",
#                 },
#                 "role_label": "nvidia.com/dynamo-sub-component-type",
#             }
#         ],
#         "components": [
#             {
#                 "name": "<component-name>",
#                 "component_type": "<frontend|worker|prefill|decode|router>",
#                 "replicas": <int>,
#                 "image": "<container-image-or-null>",
#                 "gpus_per_replica": <int>,
#                 "total_gpus": <int>,
#                 "placement": {<kubernetes-placement-fields>},
#                 "x": {<normalized-koi-x-fields>},
#             }
#         ],
#     }
# }

from __future__ import annotations

import logging
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from tandemn_efficiency_index.models.workload import (
    Workload,
    WorkloadComponent,
    WorkloadPodSelector,
    WorkloadRuntime,
)

LOGGER = logging.getLogger(__name__)

DYNAMO_API_GROUP = "nvidia.com"
DGD_PLURAL = "dynamographdeployments"
DYNAMO_API_VERSIONS = ("v1beta1", "v1alpha1")

BACKEND_MARKERS = {
    "vllm": ("vllm",),
    "sglang": ("sglang",),
    "trtllm": ("trtllm", "tensorrt_llm", "tensorrt-llm"),
}

FLAG_TO_X = {
    "model": "model_id",
    "model-id": "model_id",
    "served-model-name": "model_id",
    "tensor-parallel-size": "tp",
    "pipeline-parallel-size": "pp",
    "data-parallel-size": "dp",
    "expert-parallel-size": "ep",
    "block-size": "block_size",
    "max-num-seqs": "max_num_seq",
    "max-num-batched-tokens": "max_num_batched_tokens",
    "gpu-memory-utilization": "gpu_mem_util",
    "max-model-len": "max_model_len",
    "swap-space": "swap_space_gb",
    "kv-cache-dtype": "kvcache_dtype",
    "dtype": "weight_dtype",
    "quantization": "weight_quantization_method",
    "enable-prefix-caching": "prefix_cache_enabled",
    "enable-chunked-prefill": "chunked_prefill_enable",
    "chunk-size": "chunk_size",
    "kv-transfer-config": "kv_transfer_method",
    "router-mode": "router_policy",
    "scheduling-policy": "scheduling_policy",
}

INTEGER_X = frozenset(
    {
        "tp",
        "pp",
        "dp",
        "ep",
        "block_size",
        "max_num_seq",
        "max_num_batched_tokens",
        "max_model_len",
        "chunk_size",
    }
)
FLOAT_X = frozenset({"gpu_mem_util", "swap_space_gb"})
BOOLEAN_X = frozenset({"prefix_cache_enabled", "chunked_prefill_enable"})


class CustomObjectsApi(Protocol):
    """Kubernetes API operation required by the detector."""

    def get_namespaced_custom_object(
        self, group: str, version: str, namespace: str, plural: str, name: str
    ) -> dict[str, Any]: ...


@dataclass(frozen=True)
class DynamoWorkloadTarget:
    """User-provided identity of one DynamoGraphDeployment."""

    namespace: str
    name: str

    def __post_init__(self) -> None:
        if not self.namespace.strip() or not self.name.strip():
            raise ValueError("DGD namespace and name are required")


class DynamoDiscoveryError(RuntimeError):
    """Raised when a requested DGD cannot be read."""


class DynamoWorkloadDetector:
    """Fetch exact DGD targets and parse their workload configuration."""

    def __init__(
        self,
        custom_objects_api: CustomObjectsApi,
        api_versions: Sequence[str] = DYNAMO_API_VERSIONS,
    ) -> None:
        self._api = custom_objects_api
        self._api_versions = tuple(api_versions)

    @classmethod
    def from_in_cluster(cls) -> DynamoWorkloadDetector:
        """Create a detector using the TEI Pod service account."""
        from kubernetes import client, config

        config.load_incluster_config()
        return cls(client.CustomObjectsApi())

    def detect(self, targets: Collection[DynamoWorkloadTarget]) -> dict[str, Workload]:
        """Fetch user-specified DGDs and return normalized workloads by ID."""
        if not targets:
            raise ValueError("At least one DGD target is required")

        workloads: dict[str, Workload] = {}
        for target in sorted(set(targets), key=lambda item: (item.namespace, item.name)):
            workload = self.parse(self._get(target))
            workloads[workload.workload_id] = workload

        LOGGER.info("Loaded %d Dynamo workloads", len(workloads))
        return workloads

    def parse(self, resource: Mapping[str, Any]) -> Workload:
        """Parse one Kubernetes DGD object into a workload record."""
        if resource.get("kind") not in (None, "DynamoGraphDeployment"):
            raise ValueError("Resource is not a DynamoGraphDeployment")

        metadata = _mapping(resource.get("metadata"))
        spec = _mapping(resource.get("spec"))
        namespace = _required_text(metadata, "namespace")
        name = _required_text(metadata, "name")
        raw_components = _components(spec, namespace, name)
        backend = _backend(spec, raw_components, namespace, name)

        components = [
            _parse_component(component_name, component, backend)
            for component_name, component in raw_components
        ]
        model_ids = {
            str(component.x["model_id"]) for component in components if component.x.get("model_id")
        }
        if len(model_ids) != 1:
            raise ValueError(
                f"DGD {namespace}/{name} must declare exactly one model; found {len(model_ids)}"
            )

        component_types = {component.component_type for component in components}
        return Workload(
            runtime=WorkloadRuntime.DYNAMO,
            namespace=namespace,
            name=name,
            uid=_text(metadata.get("uid")),
            api_version=str(resource.get("apiVersion") or f"{DYNAMO_API_GROUP}/unknown"),
            model_id=model_ids.pop(),
            backend=backend,
            disaggregated="prefill" in component_types and "decode" in component_types,
            total_gpus=sum(component.total_gpus for component in components),
            components=components,
            pod_selectors=[
                WorkloadPodSelector(
                    runtime_instance=name,
                    runtime_state="active",
                    match_labels={
                        "nvidia.com/dynamo-graph-deployment-name": name,
                    },
                    role_label="nvidia.com/dynamo-sub-component-type",
                )
            ],
            source_generation=_optional_integer(metadata.get("generation")),
            source_resource_version=_text(metadata.get("resourceVersion")),
        )

    def _get(self, target: DynamoWorkloadTarget) -> dict[str, Any]:
        not_found: Exception | None = None
        for version in self._api_versions:
            try:
                return self._api.get_namespaced_custom_object(
                    DYNAMO_API_GROUP,
                    version,
                    target.namespace,
                    DGD_PLURAL,
                    target.name,
                )
            except Exception as exc:
                if getattr(exc, "status", None) != 404:
                    message = f"Unable to read DGD {target.namespace}/{target.name}: {exc}"
                    raise DynamoDiscoveryError(message) from exc
                not_found = exc

        message = f"DGD {target.namespace}/{target.name} was not found"
        raise DynamoDiscoveryError(message) from not_found


def _components(
    spec: Mapping[str, Any], namespace: str, name: str
) -> list[tuple[str, Mapping[str, Any]]]:
    components = spec.get("components")
    if isinstance(components, list):
        parsed: list[tuple[str, Mapping[str, Any]]] = []
        for component in components:
            component_mapping = _mapping(component)
            component_name = _required_text(component_mapping, "name")
            parsed.append((component_name, component_mapping))
        return parsed
    if isinstance(components, Mapping):
        return [
            (str(component_name), _mapping(component))
            for component_name, component in components.items()
        ]

    services = spec.get("services")
    if isinstance(services, Mapping):
        return [
            (str(component_name), _mapping(component))
            for component_name, component in services.items()
        ]

    raise ValueError(f"DGD {namespace}/{name} has no components")


def _backend(
    spec: Mapping[str, Any],
    components: Sequence[tuple[str, Mapping[str, Any]]],
    namespace: str,
    name: str,
) -> str:
    configured = _text(spec.get("backendFramework")) or _text(spec.get("backend"))
    if configured:
        return configured.lower()

    found: set[str] = set()
    for component_name, component in components:
        container = _main_container(component)
        values = [component_name, _text(container.get("image")) or ""]
        values.extend(_strings(container.get("command")))
        values.extend(_strings(container.get("args")))
        component_text = " ".join(values).lower()
        for backend, markers in BACKEND_MARKERS.items():
            if any(marker in component_text for marker in markers):
                found.add(backend)

    if len(found) != 1:
        raise ValueError(f"DGD {namespace}/{name} must declare one recognizable backend")
    return found.pop()


def _parse_component(name: str, component: Mapping[str, Any], backend: str) -> WorkloadComponent:
    component_type = (
        _text(component.get("subComponentType"))
        or _text(component.get("componentType"))
        or _text(component.get("type"))
    )
    component_type = component_type.lower() if component_type else name.lower()
    replicas = _integer(component.get("replicas"), default=1)
    container = _main_container(component)
    image = _text(container.get("image"))
    x = _parse_args(_strings(container.get("command")) + _strings(container.get("args")))
    x["engine_name"] = backend

    model_ref = _mapping(component.get("modelRef"))
    model_id = _text(model_ref.get("name")) or _text(model_ref.get("modelName"))
    if model_id:
        x["model_id"] = model_id

    resources = _mapping(container.get("resources")) or _mapping(component.get("resources"))
    gpus_per_replica = _gpus(resources)
    return WorkloadComponent(
        name=name,
        component_type=component_type,
        replicas=replicas,
        image=image,
        gpus_per_replica=gpus_per_replica,
        total_gpus=replicas * gpus_per_replica,
        placement=_placement(component),
        x=x,
    )


def _main_container(component: Mapping[str, Any]) -> Mapping[str, Any]:
    pod_template = _mapping(component.get("podTemplate"))
    containers = _mapping(pod_template.get("spec")).get("containers")
    if isinstance(containers, list) and containers:
        return _mapping(
            next(
                (
                    container
                    for container in containers
                    if isinstance(container, Mapping) and container.get("name") == "main"
                ),
                containers[0],
            )
        )
    return _mapping(_mapping(component.get("extraPodSpec")).get("mainContainer"))


def _parse_args(tokens: Sequence[str]) -> dict[str, Any]:
    parsed: dict[str, Any] = {}
    index = 0
    while index < len(tokens):
        token = tokens[index]
        index += 1
        if not token.startswith("--"):
            continue

        flag = token[2:]
        value: Any = True
        if "=" in flag:
            flag, value = flag.split("=", 1)
        elif index < len(tokens) and not tokens[index].startswith("--"):
            value = tokens[index]
            index += 1

        field_name = FLAG_TO_X.get(flag)
        if field_name:
            parsed[field_name] = _coerce(field_name, value)
    return parsed


def _coerce(field_name: str, value: Any) -> Any:
    try:
        if field_name in INTEGER_X:
            return int(value)
        if field_name in FLOAT_X:
            return float(value)
    except (TypeError, ValueError):
        return value
    if field_name in BOOLEAN_X:
        return str(value).lower() in {"1", "true", "yes", "on"}
    return value


def _gpus(resources: Mapping[str, Any]) -> int:
    for values in (_mapping(resources.get("limits")), _mapping(resources.get("requests"))):
        for key in ("nvidia.com/gpu", "gpu"):
            if key in values:
                return _integer(values[key], default=0)
    return 0


def _placement(component: Mapping[str, Any]) -> dict[str, Any]:
    pod_spec = _mapping(_mapping(component.get("podTemplate")).get("spec"))
    if not pod_spec:
        pod_spec = _mapping(component.get("extraPodSpec"))
    fields = ("nodeSelector", "affinity", "tolerations", "topologySpreadConstraints")
    return {field_name: pod_spec[field_name] for field_name in fields if field_name in pod_spec}


def _required_text(values: Mapping[str, Any], *keys: str) -> str:
    for key in keys:
        value = _text(values.get(key))
        if value:
            return value
    raise ValueError(f"Required field is missing: {' or '.join(keys)}")


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _strings(value: Any) -> list[str]:
    return [str(item) for item in value] if isinstance(value, list) else []


def _text(value: Any) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None


def _integer(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _optional_integer(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
