"""Discover supported model-serving workloads through Kubernetes CRDs."""

from __future__ import annotations

import logging
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Protocol

from tandemn_efficiency_index.dynamo.workload_detection import (
    DGD_PLURAL,
    DYNAMO_API_GROUP,
    DYNAMO_API_VERSIONS,
    DynamoWorkloadDetector,
)
from tandemn_efficiency_index.dynamo.workload_intent import (
    DYNAMO_DGDR_API_VERSIONS,
    DYNAMO_DGDR_PLURAL,
    dgdr_reference,
    parse_dgdr_intent,
)
from tandemn_efficiency_index.models.workload import Workload, WorkloadIntent
from tandemn_efficiency_index.ray.workload_detection import (
    RAY_API_GROUP,
    RAY_API_VERSIONS,
    RAY_SERVICE_PLURAL,
    RayWorkloadDetector,
)

LOGGER = logging.getLogger(__name__)


class ApiextensionsApi(Protocol):
    """Kubernetes CRD operation required by workload discovery."""

    def list_custom_resource_definition(self) -> Any: ...


class CustomObjectsApi(Protocol):
    """Kubernetes custom-object operations required by workload discovery."""

    def get_namespaced_custom_object(
        self,
        group: str,
        version: str,
        namespace: str,
        plural: str,
        name: str,
    ) -> dict[str, Any]: ...

    def list_cluster_custom_object(
        self,
        group: str,
        version: str,
        plural: str,
    ) -> dict[str, Any]: ...

    def list_namespaced_custom_object(
        self,
        group: str,
        version: str,
        namespace: str,
        plural: str,
    ) -> dict[str, Any]: ...


@dataclass(frozen=True)
class SupportedCustomResource:
    """Kubernetes identity and parser for one supported workload resource."""

    group: str
    kind: str
    plural: str
    preferred_versions: tuple[str, ...]


WORKLOAD_RESOURCES = (
    SupportedCustomResource(
        group=DYNAMO_API_GROUP,
        kind="DynamoGraphDeployment",
        plural=DGD_PLURAL,
        preferred_versions=DYNAMO_API_VERSIONS,
    ),
    SupportedCustomResource(
        group=RAY_API_GROUP,
        kind="RayService",
        plural=RAY_SERVICE_PLURAL,
        preferred_versions=RAY_API_VERSIONS,
    ),
)
DGDR_RESOURCE = SupportedCustomResource(
    group=DYNAMO_API_GROUP,
    kind="DynamoGraphDeploymentRequest",
    plural=DYNAMO_DGDR_PLURAL,
    preferred_versions=DYNAMO_DGDR_API_VERSIONS,
)
SUPPORTED_RESOURCES = (*WORKLOAD_RESOURCES, DGDR_RESOURCE)


class WorkloadDiscoveryError(RuntimeError):
    """Raised when Kubernetes workload discovery cannot complete."""


class KubernetesWorkloadDiscovery:
    """List supported CRDs and normalize all visible workload instances."""

    def __init__(
        self,
        apiextensions_api: ApiextensionsApi,
        custom_objects_api: CustomObjectsApi,
        namespaces: Sequence[str] = (),
        crd_refresh_interval: timedelta = timedelta(minutes=5),
    ) -> None:
        self._apiextensions_api = apiextensions_api
        self._custom_objects_api = custom_objects_api
        self._namespaces = tuple(
            sorted({namespace.strip() for namespace in namespaces if namespace.strip()})
        )
        self._dynamo = DynamoWorkloadDetector(custom_objects_api)
        self._ray = RayWorkloadDetector(custom_objects_api)
        self._crd_refresh_seconds = crd_refresh_interval.total_seconds()
        self._cached_resources: dict[tuple[str, str], DiscoveredCrd] | None = None
        self._last_crd_refresh: float | None = None

    @classmethod
    def from_in_cluster(
        cls,
        namespaces: Sequence[str] = (),
    ) -> KubernetesWorkloadDiscovery:
        """Create discovery using the TEI Pod service account."""
        from kubernetes import client, config

        config.load_incluster_config()
        return cls(
            client.ApiextensionsV1Api(),
            client.CustomObjectsApi(),
            namespaces,
        )

    def discover(self) -> dict[str, Workload]:
        """Return all supported workloads visible to the TEI service account."""
        available = self._available_resources()
        dynamo_intents = self._discover_dynamo_intents(available)

        workloads: dict[str, Workload] = {}
        for supported in WORKLOAD_RESOURCES:
            discovered = available.get((supported.group, supported.kind))
            if discovered is None:
                LOGGER.info(
                    "Kubernetes CRD %s.%s is not installed", supported.plural, supported.group
                )
                continue
            version = _select_version(discovered.served_versions, supported)
            if version is None:
                LOGGER.warning(
                    "Kubernetes CRD %s.%s has no supported served version",
                    supported.plural,
                    supported.group,
                )
                continue
            for resource in self._list_instances(supported, version):
                try:
                    workload = self._parse(supported, resource)
                    if supported.kind == "DynamoGraphDeployment":
                        reference = dgdr_reference(resource)
                        if reference is not None:
                            workload.declared_intent = dynamo_intents.get(reference)
                except (TypeError, ValueError) as exc:
                    metadata = _mapping(resource.get("metadata"))
                    LOGGER.warning(
                        "Skipping invalid %s %s/%s: %s",
                        supported.kind,
                        metadata.get("namespace", "<unknown>"),
                        metadata.get("name", "<unknown>"),
                        exc,
                    )
                    continue
                workloads[workload.workload_id] = workload

        LOGGER.info("Discovered %d Kubernetes workloads", len(workloads))
        return workloads

    def _available_resources(self) -> dict[tuple[str, str], DiscoveredCrd]:
        now = time.monotonic()
        if (
            self._cached_resources is not None
            and self._last_crd_refresh is not None
            and now - self._last_crd_refresh < self._crd_refresh_seconds
        ):
            return self._cached_resources
        crds = self._list_crds()
        self._cached_resources = {
            (resource.group, resource.kind): resource
            for resource in (_supported_crd(crd) for crd in crds)
            if resource is not None
        }
        self._last_crd_refresh = now
        return self._cached_resources

    def _discover_dynamo_intents(
        self,
        available: Mapping[tuple[str, str], DiscoveredCrd],
    ) -> dict[tuple[str, str], WorkloadIntent]:
        discovered = available.get((DGDR_RESOURCE.group, DGDR_RESOURCE.kind))
        if discovered is None:
            LOGGER.info(
                "Kubernetes CRD %s.%s is not installed",
                DGDR_RESOURCE.plural,
                DGDR_RESOURCE.group,
            )
            return {}
        version = _select_version(discovered.served_versions, DGDR_RESOURCE)
        if version is None:
            LOGGER.warning(
                "Kubernetes CRD %s.%s has no supported served version",
                DGDR_RESOURCE.plural,
                DGDR_RESOURCE.group,
            )
            return {}

        intents: dict[tuple[str, str], WorkloadIntent] = {}
        for resource in self._list_instances(DGDR_RESOURCE, version):
            try:
                intent = parse_dgdr_intent(resource)
            except (TypeError, ValueError) as exc:
                metadata = _mapping(resource.get("metadata"))
                LOGGER.warning(
                    "Skipping invalid DynamoGraphDeploymentRequest %s/%s: %s",
                    metadata.get("namespace", "<unknown>"),
                    metadata.get("name", "<unknown>"),
                    exc,
                )
                continue
            intents[(intent.source_namespace, intent.source_name)] = intent
        return intents

    def _list_crds(self) -> list[Any]:
        try:
            response = self._apiextensions_api.list_custom_resource_definition()
        except Exception as exc:
            raise WorkloadDiscoveryError(f"Unable to list Kubernetes CRDs: {exc}") from exc
        return list(getattr(response, "items", []))

    def _list_instances(
        self,
        resource: SupportedCustomResource,
        version: str,
    ) -> list[Mapping[str, Any]]:
        try:
            if not self._namespaces:
                response = self._custom_objects_api.list_cluster_custom_object(
                    resource.group,
                    version,
                    resource.plural,
                )
                return _items(response)

            instances: list[Mapping[str, Any]] = []
            for namespace in self._namespaces:
                response = self._custom_objects_api.list_namespaced_custom_object(
                    resource.group,
                    version,
                    namespace,
                    resource.plural,
                )
                instances.extend(_items(response))
            return instances
        except Exception as exc:
            scope = "cluster" if not self._namespaces else ",".join(self._namespaces)
            message = f"Unable to list {resource.kind} resources in scope {scope}: {exc}"
            raise WorkloadDiscoveryError(message) from exc

    def _parse(
        self,
        resource_type: SupportedCustomResource,
        resource: Mapping[str, Any],
    ) -> Workload:
        if resource_type.kind == "DynamoGraphDeployment":
            return self._dynamo.parse(resource)
        return self._ray.parse(resource)


@dataclass(frozen=True)
class DiscoveredCrd:
    """Relevant identity extracted from a Kubernetes CRD definition."""

    group: str
    kind: str
    plural: str
    served_versions: tuple[str, ...]


def _supported_crd(crd: Any) -> DiscoveredCrd | None:
    spec = getattr(crd, "spec", None)
    names = getattr(spec, "names", None)
    group = str(getattr(spec, "group", ""))
    kind = str(getattr(names, "kind", ""))
    plural = str(getattr(names, "plural", ""))
    supported = next(
        (
            resource
            for resource in SUPPORTED_RESOURCES
            if resource.group == group and resource.kind == kind and resource.plural == plural
        ),
        None,
    )
    if supported is None:
        return None
    versions = tuple(
        str(version.name)
        for version in getattr(spec, "versions", [])
        if getattr(version, "served", False)
    )
    return DiscoveredCrd(group, kind, plural, versions)


def _select_version(
    served_versions: Sequence[str],
    resource: SupportedCustomResource,
) -> str | None:
    return next(
        (version for version in resource.preferred_versions if version in served_versions),
        None,
    )


def _items(response: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    items = response.get("items")
    if not isinstance(items, list):
        raise ValueError("Kubernetes custom-object list response has no items list")
    return [_mapping(item) for item in items]


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}
