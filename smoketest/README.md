# Dashboard smoke-test data

This folder is isolated mock infrastructure for dashboard development. Production code does not
import it, so the complete smoke-test setup can be removed by deleting `smoketest/`.

The fixture represents a customer running three disaggregated Dynamo chatbot jobs for 23 minutes
and 17 seconds:

- Qwen (`Qwen/Qwen3-32B`)
- GLM (`zai-org/GLM-4.5-Air`)
- DeepSeek (`deepseek-ai/DeepSeek-V3-0324`)

Together they contain 6 worker Pods, 8 configured H100 GPUs, all 19 DCGM metrics collected by TEI,
and 140 samples per reporting series at a 10-second cadence. One DeepSeek metric is partially
reporting, another is missing for that workload, and one idle MIG-scoped series is deliberately
unattributed. Values are deterministic but vary over time to exercise
charts, aggregation, workload filtering, worker attribution, memory pressure, power, thermals,
clocks, PCIe traffic, reliability states, coverage warnings, and attribution diagnostics.

## Run the dashboard

```shell
uv run python -m smoketest.run_dashboard
```

Then open `http://127.0.0.1:8000`. This uses the production dashboard server and snapshot
serializer with the mock `ClusterRecord`.

## Validate the fixture

```shell
uv run pytest smoketest
```

The contract tests verify the runtime duration, job identities, worker ownership, GPU count,
complete, partial, and missing DCGM coverage, unattributed MIG scope, sample cadence, timestamp
order, dashboard response shape, clock refresh behavior, and JSON serialization.

Edit `JOB_SPECS` and the performance profiles in `mock_data.py` to change the scenario.
