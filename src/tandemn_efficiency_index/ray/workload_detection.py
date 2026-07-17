"""Fetch named RayServices and extract Ray Serve LLM configuration."""

# Output shape from RayWorkloadDetector.detect():
# {
#     "ray:<namespace>/<rayservice-name>": {
#         "runtime": "ray",
#         "namespace": "<namespace>",
#         "name": "<rayservice-name>",
#         "uid": "<kubernetes-uid-or-null>",
#         "api_version": "ray.io/<version>",
#         "model_id": "<model-name>",
#         "backend": "<vllm|sglang>",
#         "disaggregated": <bool>,
#         "total_gpus": <number>,
#         "pod_selectors": [
#             {
#                 "runtime_instance": "<active-or-pending-raycluster-name>",
#                 "runtime_state": "<active|pending>",
#                 "match_labels": {
#                     "ray.io/cluster": "<raycluster-name>",
#                     "ray.io/node-type": "worker",
#                 },
#                 "role_label": "ray.io/group",
#             }
#         ],
#         "components": [
#             {
#                 "name": "<serve-application-name>",
#                 "component_type": "<llm|prefill|decode>",
#                 "replicas": <int>,
#                 "image": "<ray-worker-image-or-null>",
#                 "gpus_per_replica": <number>,
#                 "total_gpus": <number>,
#                 "placement": {<ray-placement-fields>},
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

import yaml

from tandemn_efficiency_index.models.workload import (
    Workload,
    WorkloadComponent,
    WorkloadPodSelector,
    WorkloadRuntime,
)

LOGGER = logging.getLogger(__name__)

RAY_API_GROUP = "ray.io"
RAY_SERVICE_PLURAL = "rayservices"
RAY_API_VERSIONS = ("v1", "v1alpha1")

ENGINE_TO_X = {
    "tensor_parallel_size": "tp",
    "tp_size": "tp",
    "pipeline_parallel_size": "pp",
    "pp_size": "pp",
    "data_parallel_size": "dp",
    "dp_size": "dp",
    "expert_parallel_size": "ep",
    "enable_expert_parallel": "expert_parallel_enabled",
    "block_size": "block_size",
    "max_num_seqs": "max_num_seq",
    "max_num_batched_tokens": "max_num_batched_tokens",
    "gpu_memory_utilization": "gpu_mem_util",
    "mem_fraction_static": "gpu_mem_util",
    "max_model_len": "max_model_len",
    "swap_space": "swap_space_gb",
    "kv_cache_dtype": "kvcache_dtype",
    "dtype": "weight_dtype",
    "quantization": "weight_quantization_method",
    "enable_prefix_caching": "prefix_cache_enabled",
    "enable_chunked_prefill": "chunked_prefill_enable",
}


class CustomObjectsApi(Protocol):
    """Kubernetes API operation required by the detector."""

    def get_namespaced_custom_object(
        self, group: str, version: str, namespace: str, plural: str, name: str
    ) -> dict[str, Any]: ...


@dataclass(frozen=True)
class RayWorkloadTarget:
    """User-provided identity of one RayService."""

    namespace: str
    name: str

    def __post_init__(self) -> None:
        if not self.namespace.strip() or not self.name.strip():
            raise ValueError("RayService namespace and name are required")


class RayDiscoveryError(RuntimeError):
    """Raised when a requested RayService cannot be read."""


class RayWorkloadDetector:
    """Fetch exact RayService targets and parse their LLM configuration."""

    def __init__(
        self,
        custom_objects_api: CustomObjectsApi,
        api_versions: Sequence[str] = RAY_API_VERSIONS,
    ) -> None:
        self._api = custom_objects_api
        self._api_versions = tuple(api_versions)

    @classmethod
    def from_in_cluster(cls) -> RayWorkloadDetector:
        """Create a detector using the TEI Pod service account."""
        from kubernetes import client, config

        config.load_incluster_config()
        return cls(client.CustomObjectsApi())

    def detect(self, targets: Collection[RayWorkloadTarget]) -> dict[str, Workload]:
        """Fetch user-specified RayServices and return normalized workloads by ID."""
        if not targets:
            raise ValueError("At least one RayService target is required")

        workloads: dict[str, Workload] = {}
        for target in sorted(set(targets), key=lambda item: (item.namespace, item.name)):
            workload = self.parse(self._get(target))
            workloads[workload.workload_id] = workload

        LOGGER.info("Loaded %d Ray workloads", len(workloads))
        return workloads

    def parse(self, resource: Mapping[str, Any]) -> Workload:
        """Parse one Kubernetes RayService object into a workload record."""
        if resource.get("kind") not in (None, "RayService"):
            raise ValueError("Resource is not a RayService")

        metadata = _mapping(resource.get("metadata"))
        spec = _mapping(resource.get("spec"))
        namespace = _required_text(metadata, "namespace")
        name = _required_text(metadata, "name")
        applications = _applications(spec, namespace, name)
        configured_components = _configured_components(applications, namespace, name)
        components = [
            _parse_component(llm_config, spec, component_type, component_name)
            for llm_config, component_type, component_name in configured_components
        ]
        model_ids = {str(component.x["model_id"]) for component in components}
        if len(model_ids) != 1:
            raise ValueError(
                f"RayService {namespace}/{name} must declare exactly one model; "
                f"found {len(model_ids)}"
            )
        backends = {str(component.x["engine_name"]) for component in components}
        if len(backends) != 1:
            raise ValueError(f"RayService {namespace}/{name} mixes multiple LLM backends")
        component_types = {component.component_type for component in components}

        return Workload(
            runtime=WorkloadRuntime.RAY,
            namespace=namespace,
            name=name,
            uid=_text(metadata.get("uid")),
            api_version=str(resource.get("apiVersion") or f"{RAY_API_GROUP}/unknown"),
            model_id=model_ids.pop(),
            backend=backends.pop(),
            disaggregated={"prefill", "decode"}.issubset(component_types),
            total_gpus=_clean_number(sum(component.total_gpus for component in components)),
            components=components,
            pod_selectors=_pod_selectors(_mapping(resource.get("status"))),
        )

    def _get(self, target: RayWorkloadTarget) -> dict[str, Any]:
        not_found: Exception | None = None
        for version in self._api_versions:
            try:
                return self._api.get_namespaced_custom_object(
                    RAY_API_GROUP,
                    version,
                    target.namespace,
                    RAY_SERVICE_PLURAL,
                    target.name,
                )
            except Exception as exc:
                if getattr(exc, "status", None) != 404:
                    message = f"Unable to read RayService {target.namespace}/{target.name}: {exc}"
                    raise RayDiscoveryError(message) from exc
                not_found = exc

        message = f"RayService {target.namespace}/{target.name} was not found"
        raise RayDiscoveryError(message) from not_found


def _applications(spec: Mapping[str, Any], namespace: str, name: str) -> list[Mapping[str, Any]]:
    raw_config = spec.get("serveConfigV2")
    try:
        config = yaml.safe_load(raw_config) if isinstance(raw_config, str) else raw_config
    except yaml.YAMLError as exc:
        raise ValueError(f"RayService {namespace}/{name} has invalid serveConfigV2") from exc

    applications = _mapping(config).get("applications")
    if not isinstance(applications, list) or not applications:
        raise ValueError(f"RayService {namespace}/{name} has no Serve applications")
    return [_mapping(application) for application in applications]


def _pod_selectors(status: Mapping[str, Any]) -> list[WorkloadPodSelector]:
    selectors: list[WorkloadPodSelector] = []
    seen_clusters: set[str] = set()
    service_states = (
        ("activeServiceStatus", "active"),
        ("pendingServiceStatus", "pending"),
    )
    for field_name, runtime_state in service_states:
        service_status = _mapping(status.get(field_name))
        cluster_name = _text(service_status.get("rayClusterName"))
        if not cluster_name or cluster_name in seen_clusters:
            continue
        seen_clusters.add(cluster_name)
        selectors.append(
            WorkloadPodSelector(
                runtime_instance=cluster_name,
                runtime_state=runtime_state,
                match_labels={
                    "ray.io/cluster": cluster_name,
                    "ray.io/node-type": "worker",
                },
                role_label="ray.io/group",
            )
        )
    return selectors


def _configured_components(
    applications: Sequence[Mapping[str, Any]], namespace: str, name: str
) -> list[tuple[Mapping[str, Any], str, str]]:
    found: list[tuple[Mapping[str, Any], str, str]] = []
    for application in applications:
        args = _mapping(application.get("args"))
        application_name = _text(application.get("name")) or "ray-serve-llm"

        prefill = args.get("prefill_config")
        decode = args.get("decode_config")
        if prefill is not None or decode is not None:
            if not isinstance(prefill, Mapping) or not isinstance(decode, Mapping):
                raise ValueError("Ray PD disaggregation requires inline prefill and decode configs")
            found.extend(
                (
                    (prefill, "prefill", f"{application_name}-prefill"),
                    (decode, "decode", f"{application_name}-decode"),
                )
            )
            continue

        raw_configs = args.get("llm_configs")
        if raw_configs is None and "llm_config" in args:
            raw_configs = [args["llm_config"]]
        if not isinstance(raw_configs, list):
            continue
        for index, config in enumerate(raw_configs):
            if not isinstance(config, Mapping):
                raise ValueError("External Ray LLM config files are not supported")
            component_name = application_name
            if len(raw_configs) > 1:
                component_name = f"{application_name}-{index + 1}"
            found.append((config, "llm", component_name))

    if not found:
        raise ValueError(
            f"RayService {namespace}/{name} must declare an inline Ray Serve LLM config"
        )
    return found


def _parse_component(
    llm_config: Mapping[str, Any],
    spec: Mapping[str, Any],
    component_type: str,
    component_name: str,
) -> WorkloadComponent:
    model = _mapping(llm_config.get("model_loading_config"))
    model_id = _required_text(model, "model_id")
    engine = _engine_name(llm_config)
    engine_kwargs = _mapping(llm_config.get("engine_kwargs"))
    deployment = _mapping(llm_config.get("deployment_config"))
    autoscaling = _mapping(deployment.get("autoscaling_config"))
    replicas = _replicas(deployment, autoscaling)
    gpus_per_replica = _gpus_per_replica(engine_kwargs, llm_config)

    x: dict[str, Any] = {"model_id": model_id, "engine_name": engine}
    model_source = model.get("model_source")
    if model_source is not None:
        x["model_source"] = model_source
    tokenizer_source = model.get("tokenizer_source")
    if tokenizer_source is not None:
        x["tokenizer_source"] = tokenizer_source
    for source_name, field_name in ENGINE_TO_X.items():
        if source_name in engine_kwargs:
            x[field_name] = engine_kwargs[source_name]
    kv_transfer = _mapping(engine_kwargs.get("kv_transfer_config"))
    if kv_transfer:
        x["kv_transfer_method"] = kv_transfer.get("kv_connector")
        if "kv_role" in kv_transfer:
            x["kv_transfer_role"] = kv_transfer["kv_role"]
    _copy_deployment_x(x, deployment, autoscaling)

    return WorkloadComponent(
        name=component_name,
        component_type=component_type,
        replicas=replicas,
        image=_worker_image(spec),
        gpus_per_replica=gpus_per_replica,
        total_gpus=_clean_number(replicas * gpus_per_replica),
        placement=_placement(llm_config, deployment),
        x=x,
    )


def _copy_deployment_x(
    x: dict[str, Any], deployment: Mapping[str, Any], autoscaling: Mapping[str, Any]
) -> None:
    fields = (
        (deployment, "max_ongoing_requests", "max_ongoing_requests"),
        (deployment, "max_queued_requests", "max_queued_requests"),
        (deployment, "request_router_config", "router_policy"),
        (autoscaling, "min_replicas", "min_replicas"),
        (autoscaling, "max_replicas", "max_replicas"),
        (autoscaling, "target_ongoing_requests", "target_ongoing_requests"),
    )
    for source, source_name, field_name in fields:
        if source_name in source:
            x[field_name] = source[source_name]


def _replicas(deployment: Mapping[str, Any], autoscaling: Mapping[str, Any]) -> int:
    configured = deployment.get("num_replicas")
    if isinstance(configured, int) and not isinstance(configured, bool):
        return configured
    for field_name in ("initial_replicas", "min_replicas"):
        value = autoscaling.get(field_name)
        if isinstance(value, int) and not isinstance(value, bool):
            return value
    return 1


def _gpus_per_replica(
    engine_kwargs: Mapping[str, Any], llm_config: Mapping[str, Any]
) -> int | float:
    placement = _mapping(llm_config.get("placement_group_config"))
    bundles = placement.get("bundles")
    if isinstance(bundles, list) and bundles:
        return _clean_number(sum(_number(_mapping(bundle).get("GPU"), 0.0) for bundle in bundles))

    tp = _number(engine_kwargs.get("tensor_parallel_size", engine_kwargs.get("tp_size")), 1.0)
    pp = _number(engine_kwargs.get("pipeline_parallel_size", engine_kwargs.get("pp_size")), 1.0)
    dp = _number(engine_kwargs.get("data_parallel_size", engine_kwargs.get("dp_size")), 1.0)
    bundle = _mapping(placement.get("bundle_per_worker"))
    gpu_per_worker = _number(bundle.get("GPU"), 1.0)
    return _clean_number(tp * pp * dp * gpu_per_worker)


def _placement(llm_config: Mapping[str, Any], deployment: Mapping[str, Any]) -> dict[str, Any]:
    placement: dict[str, Any] = {}
    for field_name in ("accelerator_type", "accelerator_config", "placement_group_config"):
        if field_name in llm_config:
            placement[field_name] = llm_config[field_name]
    ray_actor_options = deployment.get("ray_actor_options")
    if isinstance(ray_actor_options, Mapping):
        placement["ray_actor_options"] = dict(ray_actor_options)
    return placement


def _worker_image(spec: Mapping[str, Any]) -> str | None:
    cluster = _mapping(spec.get("rayClusterConfig"))
    worker_groups = cluster.get("workerGroupSpecs")
    if not isinstance(worker_groups, list):
        return None

    images: set[str] = set()
    for group in worker_groups:
        pod_spec = _mapping(_mapping(_mapping(group).get("template")).get("spec"))
        containers = pod_spec.get("containers")
        if not isinstance(containers, list):
            continue
        ray_workers = [
            container for container in containers if _mapping(container).get("name") == "ray-worker"
        ]
        for container in ray_workers or containers:
            image = _text(_mapping(container).get("image"))
            if image:
                images.add(image)
    return images.pop() if len(images) == 1 else None


def _engine_name(llm_config: Mapping[str, Any]) -> str:
    server_cls = (_text(llm_config.get("server_cls")) or "").lower()
    if "sglang" in server_cls:
        return "sglang"
    return (_text(llm_config.get("llm_engine")) or "vllm").lower()


def _required_text(values: Mapping[str, Any], *keys: str) -> str:
    for key in keys:
        value = _text(values.get(key))
        if value:
            return value
    raise ValueError(f"Required field is missing: {' or '.join(keys)}")


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _text(value: Any) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None


def _number(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _clean_number(value: float) -> int | float:
    return int(value) if value.is_integer() else value
