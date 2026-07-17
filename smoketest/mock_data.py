"""Build realistic Dynamo and DCGM data for dashboard smoke testing."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from tandemn_efficiency_index.models.cluster_snapshot import (
    ClusterRecord,
    JobRecord,
    WorkloadPod,
)
from tandemn_efficiency_index.models.telemetry import MetricSample, MetricScope, MetricSeries
from tandemn_efficiency_index.models.workload import (
    Workload,
    WorkloadComponent,
    WorkloadPodSelector,
    WorkloadRuntime,
)
from tandemn_efficiency_index.observability import cluster_record_to_dict
from tandemn_efficiency_index.prometheus.dcgm import DCGM_METRICS

RUN_DURATION = timedelta(minutes=23, seconds=17)
SAMPLE_INTERVAL_SECONDS = 10
MOCK_ENDED_AT = datetime(2026, 7, 16, 12, 23, 17, tzinfo=UTC)
GPU_MEMORY_MIB = 81_559.0


@dataclass(frozen=True)
class ComponentSpec:
    """Configuration needed to create one mock Dynamo component."""

    role: str
    replicas: int
    gpus_per_replica: int
    max_num_seq: int
    max_num_batched_tokens: int


@dataclass(frozen=True)
class JobSpec:
    """Configuration and performance profile for one mock model job."""

    name: str
    model_id: str
    max_model_len: int
    base_gpu_util: float
    memory_fraction: float
    components: tuple[ComponentSpec, ...]


JOB_SPECS = (
    JobSpec(
        name="qwen-chat",
        model_id="Qwen/Qwen3-32B",
        max_model_len=32_768,
        base_gpu_util=67.0,
        memory_fraction=0.72,
        components=(
            ComponentSpec("prefill", 1, 1, 32, 16_384),
            ComponentSpec("decode", 1, 1, 192, 8_192),
        ),
    ),
    JobSpec(
        name="glm-chat",
        model_id="zai-org/GLM-4.5-Air",
        max_model_len=32_768,
        base_gpu_util=52.0,
        memory_fraction=0.64,
        components=(
            ComponentSpec("prefill", 1, 1, 24, 12_288),
            ComponentSpec("decode", 1, 1, 128, 8_192),
        ),
    ),
    JobSpec(
        name="deepseek-chat",
        model_id="deepseek-ai/DeepSeek-V3-0324",
        max_model_len=65_536,
        base_gpu_util=79.0,
        memory_fraction=0.86,
        components=(
            ComponentSpec("prefill", 1, 2, 16, 16_384),
            ComponentSpec("decode", 1, 2, 96, 8_192),
        ),
    ),
)


def build_mock_cluster_record(ended_at: datetime = MOCK_ENDED_AT) -> ClusterRecord:
    """Build the complete rolling record for the three-job customer scenario."""
    started_at = ended_at - RUN_DURATION
    sample_times = _sample_times(started_at, ended_at)
    jobs: dict[str, JobRecord] = {}

    for job_index, spec in enumerate(JOB_SPECS):
        job = _build_job(spec, job_index, started_at, ended_at, sample_times)
        jobs[job.workload_id] = job

    record = ClusterRecord(
        started_at=started_at,
        updated_at=ended_at,
        window_start=started_at,
        sample_interval_seconds=SAMPLE_INTERVAL_SECONDS,
        jobs=jobs,
    )
    _add_unattributed_series(record, sample_times)
    return record


def build_mock_snapshot(ended_at: datetime = MOCK_ENDED_AT) -> dict[str, Any]:
    """Return the exact JSON-ready payload consumed by the dashboard API."""
    return cluster_record_to_dict(
        build_mock_cluster_record(ended_at),
        window_seconds=None,
        max_points=180,
    )


def _build_job(
    spec: JobSpec,
    job_index: int,
    started_at: datetime,
    ended_at: datetime,
    sample_times: list[datetime],
) -> JobRecord:
    workload = _build_workload(spec, job_index)
    job = JobRecord(workload=workload)
    gpu_number = 0

    for component in spec.components:
        for replica_index in range(component.replicas):
            pod = _build_worker(
                workload,
                component,
                replica_index,
                job_index,
                started_at,
                ended_at,
            )
            job.workers[pod.uid] = pod
            for local_gpu_index in range(component.gpus_per_replica):
                _add_gpu_series(
                    job,
                    spec,
                    pod,
                    local_gpu_index,
                    gpu_number,
                    sample_times,
                )
                gpu_number += 1

    job.telemetry.last_sample_at = sample_times[-1]
    return job


def _build_workload(spec: JobSpec, job_index: int) -> Workload:
    components = [
        WorkloadComponent(
            name=f"{spec.name}-{component.role}",
            component_type=component.role,
            replicas=component.replicas,
            image="nvcr.io/nvidia/ai-dynamo/vllm-runtime:0.4.1",
            gpus_per_replica=component.gpus_per_replica,
            total_gpus=component.replicas * component.gpus_per_replica,
            placement={
                "nodeSelector": {
                    "nvidia.com/gpu.product": "NVIDIA-H100-80GB-HBM3",
                }
            },
            x=_workload_characteristics(spec, component),
        )
        for component in spec.components
    ]
    selectors = [
        WorkloadPodSelector(
            runtime_instance=spec.name,
            runtime_state="active",
            match_labels={
                "nvidia.com/dynamo-graph-deployment-name": spec.name,
                "nvidia.com/dynamo-component-type": "worker",
            },
            role_label="nvidia.com/dynamo-sub-component-type",
        )
        for component in spec.components
    ]
    return Workload(
        runtime=WorkloadRuntime.DYNAMO,
        namespace="chatbot-production",
        name=spec.name,
        uid=f"dgd-{job_index + 1:02d}-{spec.name}",
        api_version="nvidia.com/v1beta1",
        model_id=spec.model_id,
        backend="vllm",
        disaggregated=True,
        total_gpus=sum(component.total_gpus for component in components),
        components=components,
        pod_selectors=selectors,
    )


def _workload_characteristics(
    spec: JobSpec,
    component: ComponentSpec,
) -> dict[str, Any]:
    return {
        "model_id": spec.model_id,
        "engine_name": "vllm",
        "tp": component.gpus_per_replica,
        "pp": 1,
        "dp": component.replicas,
        "ep": 2 if spec.name == "deepseek-chat" else 1,
        "block_size": 16,
        "max_num_seq": component.max_num_seq,
        "max_num_batched_tokens": component.max_num_batched_tokens,
        "gpu_mem_util": 0.9,
        "max_model_len": spec.max_model_len,
        "kvcache_dtype": "fp8",
        "weight_dtype": "bfloat16",
        "weight_quantization_method": None,
        "prefix_cache_enabled": True,
        "chunked_prefill_enable": component.role == "prefill",
        "chunk_size": 512 if component.role == "prefill" else None,
        "router_policy": "round-robin",
        "scheduling_policy": "fcfs",
    }


def _build_worker(
    workload: Workload,
    component: ComponentSpec,
    replica_index: int,
    job_index: int,
    started_at: datetime,
    ended_at: datetime,
) -> WorkloadPod:
    pod_name = f"{workload.name}-{component.role}-{replica_index}"
    return WorkloadPod(
        workload_id=workload.workload_id,
        namespace=workload.namespace,
        name=pod_name,
        uid=f"pod-{pod_name}",
        node_name=f"gpu-node-{job_index * 2 + replica_index + 1:02d}",
        container_names=["main"],
        runtime_instance=workload.name,
        runtime_state="active",
        runtime_role=component.role,
        first_seen_at=started_at,
        last_seen_at=ended_at,
    )


def _add_gpu_series(
    job: JobRecord,
    spec: JobSpec,
    pod: WorkloadPod,
    local_gpu_index: int,
    gpu_number: int,
    sample_times: list[datetime],
) -> None:
    gpu_uuid = f"GPU-{spec.name.upper()}-{gpu_number:02d}"
    scope = MetricScope(
        workload_id=job.workload_id,
        pod_uid=pod.uid,
        container_name="main",
        node_name=pod.node_name,
        gpu_uuid=gpu_uuid,
        gpu_index=str(local_gpu_index),
        local_rank=str(local_gpu_index),
        gpu_instance_id=str(local_gpu_index) if spec.name == "deepseek-chat" else None,
        runtime_instance=pod.runtime_instance,
        runtime_role=pod.runtime_role,
        pod_namespace=pod.namespace,
        pod_name=pod.name,
        attribution_method="namespace_pod",
    )
    labels = {
        "cluster": "customer-chatbot-prod",
        "job": "dcgm-exporter",
    }
    for metric_name in DCGM_METRICS:
        if _skip_metric(spec, gpu_number, metric_name):
            continue
        series = MetricSeries.create(metric_name, scope, labels)
        series.append(
            MetricSample(
                timestamp=timestamp,
                value=_metric_value(
                    metric_name,
                    spec,
                    pod.runtime_role or "worker",
                    gpu_number,
                    sample_index,
                ),
            )
            for sample_index, timestamp in enumerate(sample_times)
        )
        job.telemetry.series[series.series_id] = series


def _skip_metric(spec: JobSpec, gpu_number: int, metric_name: str) -> bool:
    if spec.name != "deepseek-chat":
        return False
    if metric_name == "DCGM_FI_DEV_MEM_CLOCK":
        return True
    return metric_name == "DCGM_FI_DEV_MEM_COPY_UTIL" and gpu_number == 3


def _add_unattributed_series(record: ClusterRecord, sample_times: list[datetime]) -> None:
    scope = MetricScope(
        workload_id=None,
        pod_uid="deleted-pod-uid",
        container_name="main",
        node_name="gpu-node-idle-01",
        gpu_uuid="GPU-UNATTRIBUTED-00",
        gpu_index="0",
        gpu_instance_id="3",
        pod_namespace="chatbot-production",
        pod_name="deleted-worker-0",
        attribution_method="unattributed_pod_not_found",
    )
    series = MetricSeries.create(
        "DCGM_FI_DEV_GPU_UTIL",
        scope,
        {"cluster": "customer-chatbot-prod", "job": "dcgm-exporter"},
    )
    series.append(MetricSample(timestamp=timestamp, value=0.0) for timestamp in sample_times)
    record.unattributed_telemetry.series[series.series_id] = series
    record.unattributed_telemetry.last_sample_at = sample_times[-1]


def _metric_value(
    metric_name: str,
    spec: JobSpec,
    role: str,
    gpu_number: int,
    sample_index: int,
) -> float:
    phase = sample_index / 6.0 + gpu_number * 0.65
    traffic_wave = math.sin(phase) * 8.0 + math.sin(sample_index / 17.0) * 5.0
    role_adjustment = 7.0 if role == "prefill" else -3.0
    gpu_util = _clamp(spec.base_gpu_util + traffic_wave + role_adjustment, 8.0, 98.0)
    memory_used = GPU_MEMORY_MIB * spec.memory_fraction + math.sin(phase / 3.0) * 850.0
    memory_reserved = 512.0

    values = {
        "DCGM_FI_DEV_GPU_UTIL": gpu_util,
        "DCGM_FI_DEV_MEM_COPY_UTIL": _clamp(gpu_util * 0.72 + 4.0, 0.0, 100.0),
        "DCGM_FI_DEV_FB_USED": memory_used,
        "DCGM_FI_DEV_FB_FREE": GPU_MEMORY_MIB - memory_used - memory_reserved,
        "DCGM_FI_DEV_FB_RESERVED": memory_reserved,
        "DCGM_FI_DEV_POWER_USAGE": 92.0 + gpu_util * 3.15,
        "DCGM_FI_DEV_GPU_TEMP": 38.0 + gpu_util * 0.34,
        "DCGM_FI_DEV_SM_CLOCK": 885.0 + gpu_util * 7.2,
        "DCGM_FI_DEV_MEM_CLOCK": 1_593.0,
        "DCGM_FI_DEV_XID_ERRORS": 0.0,
        "DCGM_FI_PROF_GR_ENGINE_ACTIVE": gpu_util / 100.0 * 0.94,
        "DCGM_FI_PROF_SM_ACTIVE": gpu_util / 100.0 * 0.91,
        "DCGM_FI_PROF_SM_OCCUPANCY": _clamp(0.42 + gpu_util / 210.0, 0.0, 0.92),
        "DCGM_FI_PROF_PIPE_TENSOR_ACTIVE": gpu_util / 100.0 * 0.76,
        "DCGM_FI_PROF_DRAM_ACTIVE": gpu_util / 100.0 * 0.82,
        "DCGM_FI_PROF_PCIE_TX_BYTES": 38_000_000.0 + gpu_util * 1_150_000.0,
        "DCGM_FI_PROF_PCIE_RX_BYTES": 52_000_000.0 + gpu_util * 1_480_000.0,
        "DCGM_FI_PROF_NVLINK_TX_BYTES": 86_000_000.0 + gpu_util * 2_100_000.0,
        "DCGM_FI_PROF_NVLINK_RX_BYTES": 91_000_000.0 + gpu_util * 2_250_000.0,
    }
    return round(values[metric_name], 4)


def _sample_times(started_at: datetime, ended_at: datetime) -> list[datetime]:
    sample_times: list[datetime] = []
    timestamp = started_at
    interval = timedelta(seconds=SAMPLE_INTERVAL_SECONDS)
    while timestamp <= ended_at:
        sample_times.append(timestamp)
        timestamp += interval
    return sample_times


def _clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, value))
