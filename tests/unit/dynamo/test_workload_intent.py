from typing import Any

from tandemn_efficiency_index.dynamo.workload_intent import (
    dgdr_reference,
    parse_dgdr_intent,
)


def test_parses_v1beta1_workload_slo_and_hardware_intent() -> None:
    resource = {
        "apiVersion": "nvidia.com/v1beta1",
        "kind": "DynamoGraphDeploymentRequest",
        "metadata": {
            "namespace": "inference",
            "name": "qwen-request",
            "uid": "dgdr-uid",
        },
        "spec": {
            "model": "Qwen/Qwen3-32B",
            "backend": "vllm",
            "workload": {
                "isl": 3000,
                "osl": 150,
                "requestRate": 25,
                "concurrency": 100,
            },
            "sla": {"ttft": 200.0, "itl": 20.0},
            "hardware": {
                "gpuSku": "h200_sxm",
                "vramMb": 81920,
                "totalGpus": 16,
                "numGpusPerNode": 8,
            },
        },
    }

    intent = parse_dgdr_intent(resource)

    assert intent.source_name == "qwen-request"
    assert intent.model_id == "Qwen/Qwen3-32B"
    assert intent.backend == "vllm"
    assert intent.workload == {
        "input_sequence_length": 3000,
        "output_sequence_length": 150,
        "request_rate": 25,
        "concurrency": 100,
    }
    assert intent.slo == {"ttft_ms": 200.0, "itl_ms": 20.0}
    assert intent.hardware == {
        "gpu_sku": "h200_sxm",
        "vram_mb": 81920,
        "total_gpus": 16,
        "gpus_per_node": 8,
    }


def test_parses_legacy_v1alpha1_nested_intent() -> None:
    resource = {
        "apiVersion": "nvidia.com/v1alpha1",
        "kind": "DynamoGraphDeploymentRequest",
        "metadata": {"namespace": "inference", "name": "legacy-request"},
        "spec": {
            "profilingConfig": {
                "config": {
                    "sla": {"isl": 2048, "osl": 256, "ttft": 300, "itl": 30},
                    "hardware": {"gpu_sku": "a100_sxm", "total_gpus": 8},
                }
            }
        },
    }

    intent = parse_dgdr_intent(resource)

    assert intent.workload == {
        "input_sequence_length": 2048,
        "output_sequence_length": 256,
    }
    assert intent.slo == {"ttft_ms": 300, "itl_ms": 30}
    assert intent.hardware == {"gpu_sku": "a100_sxm", "total_gpus": 8}


def test_reads_exact_dgdr_correlation_labels() -> None:
    dgd: dict[str, Any] = {
        "metadata": {
            "namespace": "inference",
            "name": "qwen-dgd",
            "labels": {
                "dgdr.nvidia.com/name": "qwen-request",
                "dgdr.nvidia.com/namespace": "inference",
            },
        }
    }

    assert dgdr_reference(dgd) == ("inference", "qwen-request")
    assert dgdr_reference({"metadata": {"labels": {}}}) is None
