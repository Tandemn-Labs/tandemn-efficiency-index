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
#         "declared_intent": <normalized-ray-serve-intent>,
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
    WorkloadIntent,
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
        if not model_ids:
            raise ValueError(f"RayService {namespace}/{name} does not declare a model")
        backends = {str(component.x["engine_name"]) for component in components}
        if not backends:
            raise ValueError(f"RayService {namespace}/{name} does not declare an LLM backend")
        component_types = {component.component_type for component in components}
        api_version = str(resource.get("apiVersion") or f"{RAY_API_GROUP}/unknown")

        return Workload(
            runtime=WorkloadRuntime.RAY,
            namespace=namespace,
            name=name,
            uid=_text(metadata.get("uid")),
            api_version=api_version,
            model_id=", ".join(sorted(model_ids)),
            backend=", ".join(sorted(backends)),
            disaggregated={"prefill", "decode"}.issubset(component_types),
            total_gpus=_clean_number(sum(component.total_gpus for component in components)),
            components=components,
            pod_selectors=_pod_selectors(_mapping(resource.get("status"))),
            declared_intent=_declared_intent(metadata, api_version, components),
            source_generation=_optional_integer(metadata.get("generation")),
            source_resource_version=_text(metadata.get("resourceVersion")),
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
        for node_type in ("head", "worker"):
            selectors.append(
                WorkloadPodSelector(
                    runtime_instance=cluster_name,
                    runtime_state=runtime_state,
                    match_labels={
                        "ray.io/cluster": cluster_name,
                        "ray.io/node-type": node_type,
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
            found.extend(_generic_components(application))
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
            f"RayService {namespace}/{name} must expose model and backend configuration"
        )
    return found


def _generic_components(
    application: Mapping[str, Any],
) -> list[tuple[Mapping[str, Any], str, str]]:
    """Normalize explicit model-serving settings from a custom Ray Serve wrapper."""
    application_args = _mapping(application.get("args"))
    deployments = application.get("deployments")
    candidates = deployments if isinstance(deployments, list) and deployments else [{}]
    application_name = _text(application.get("name")) or "ray-serve"
    import_path = _text(application.get("import_path")) or ""
    found: list[tuple[Mapping[str, Any], str, str]] = []

    for raw_deployment in candidates:
        deployment = _mapping(raw_deployment)
        user_config = _mapping(deployment.get("user_config"))
        configured = {**application_args, **user_config}
        model_id = _first_text(
            configured,
            "model_id",
            "model",
            "model_name",
            "model_path",
        )
        backend = _configured_backend(configured, import_path, deployment)
        if model_id is None or backend is None:
            continue

        engine_kwargs = _mapping(configured.get("engine_kwargs"))
        if not engine_kwargs:
            engine_kwargs = _mapping(configured.get(f"{backend}_engine_kwargs"))
        component_name = _text(deployment.get("name")) or application_name
        llm_config: dict[str, Any] = {
            "model_loading_config": {"model_id": model_id},
            "llm_engine": backend,
            "engine_kwargs": dict(engine_kwargs),
            "deployment_config": dict(deployment),
        }
        found.append((llm_config, "llm", component_name))
    return found


def _configured_backend(
    configured: Mapping[str, Any],
    import_path: str,
    deployment: Mapping[str, Any],
) -> str | None:
    explicit = _first_text(configured, "backend", "engine", "engine_name", "llm_engine")
    if explicit:
        normalized = explicit.lower()
        if normalized in {"vllm", "sglang"}:
            return normalized
    marker_text = " ".join(
        (
            import_path,
            _text(deployment.get("name")) or "",
            _text(configured.get("server_cls")) or "",
        )
    ).lower()
    matches = {backend for backend in ("vllm", "sglang") if backend in marker_text}
    return matches.pop() if len(matches) == 1 else None


def _first_text(values: Mapping[str, Any], *keys: str) -> str | None:
    return next((_text(values.get(key)) for key in keys if _text(values.get(key))), None)


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
    gpus_per_replica = _gpus_per_replica(engine_kwargs, llm_config, deployment)

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
        (autoscaling, "initial_replicas", "initial_replicas"),
        (autoscaling, "target_ongoing_requests", "target_ongoing_requests"),
        (autoscaling, "upscale_delay_s", "upscale_delay_s"),
        (autoscaling, "downscale_delay_s", "downscale_delay_s"),
        (autoscaling, "upscaling_factor", "upscaling_factor"),
        (autoscaling, "downscaling_factor", "downscaling_factor"),
        (autoscaling, "metrics_interval_s", "metrics_interval_s"),
        (autoscaling, "look_back_period_s", "look_back_period_s"),
    )
    for source, source_name, field_name in fields:
        if source_name in source:
            x[field_name] = source[source_name]


def _declared_intent(
    metadata: Mapping[str, Any],
    api_version: str,
    components: Sequence[WorkloadComponent],
) -> WorkloadIntent:
    component_intents: list[dict[str, Any]] = []
    autoscaling_fields = (
        "min_replicas",
        "max_replicas",
        "initial_replicas",
        "target_ongoing_requests",
        "upscale_delay_s",
        "downscale_delay_s",
        "upscaling_factor",
        "downscaling_factor",
        "metrics_interval_s",
        "look_back_period_s",
    )
    for component in components:
        intent: dict[str, Any] = {
            "name": component.name,
            "component_type": component.component_type,
            "replicas": component.replicas,
        }
        for field_name in ("max_ongoing_requests", "max_queued_requests"):
            if field_name in component.x:
                intent[field_name] = component.x[field_name]
        autoscaling = {
            field_name: component.x[field_name]
            for field_name in autoscaling_fields
            if field_name in component.x
        }
        if autoscaling:
            intent["autoscaling"] = autoscaling
        component_intents.append(intent)

    return WorkloadIntent(
        source_kind="RayService",
        source_namespace=_required_text(metadata, "namespace"),
        source_name=_required_text(metadata, "name"),
        source_uid=_text(metadata.get("uid")),
        api_version=api_version,
        model_id=_single_component_value(components, "model_id"),
        backend=_single_component_value(components, "engine_name"),
        components=component_intents,
    )


def _single_component_value(components: Sequence[WorkloadComponent], field_name: str) -> str | None:
    values = {str(component.x[field_name]) for component in components if field_name in component.x}
    return values.pop() if len(values) == 1 else None


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
    engine_kwargs: Mapping[str, Any],
    llm_config: Mapping[str, Any],
    deployment: Mapping[str, Any],
) -> int | float:
    placement = _mapping(llm_config.get("placement_group_config"))
    bundles = placement.get("bundles")
    if isinstance(bundles, list) and bundles:
        return _clean_number(sum(_number(_mapping(bundle).get("GPU"), 0.0) for bundle in bundles))

    ray_actor_options = _mapping(deployment.get("ray_actor_options"))
    if "num_gpus" in ray_actor_options:
        return _clean_number(_number(ray_actor_options.get("num_gpus"), 0.0))

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


def _optional_integer(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _clean_number(value: float) -> int | float:
    return int(value) if value.is_integer() else value
