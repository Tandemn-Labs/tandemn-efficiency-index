const PRIMARY_METRICS = [
  {
    name: "DCGM_FI_DEV_GPU_UTIL",
    label: "GPU utilization",
    mode: "percent",
    description: "Compute engine duty cycle",
    benchmark: { kind: "high", good: 80, watch: 50 },
  },
  {
    name: "DCGM_FI_PROF_SM_ACTIVE",
    label: "SM active",
    mode: "ratio",
    description: "Time streaming multiprocessors are active",
    benchmark: { kind: "high", good: 80, watch: 50 },
  },
  {
    name: "DCGM_FI_PROF_SM_OCCUPANCY",
    label: "SM occupancy",
    mode: "ratio",
    description: "Resident warps relative to hardware capacity",
    benchmark: { kind: "context" },
  },
  {
    name: "DCGM_FI_PROF_PIPE_TENSOR_ACTIVE",
    label: "Tensor activity",
    mode: "ratio",
    description: "Tensor pipe active time",
    benchmark: { kind: "high", good: 50, watch: 20 },
  },
  {
    name: "TEI_GPU_MEMORY_PRESSURE",
    sourceNames: [
      "DCGM_FI_DEV_FB_USED",
      "DCGM_FI_DEV_FB_FREE",
      "DCGM_FI_DEV_FB_RESERVED",
    ],
    label: "GPU memory pressure",
    mode: "percent",
    description: "Used framebuffer relative to available capacity",
    benchmark: {
      kind: "band",
      goodMin: 60,
      goodMax: 90,
      watchMin: 40,
      watchMax: 95,
    },
  },
  {
    name: "DCGM_FI_DEV_POWER_USAGE",
    label: "GPU power",
    mode: "watts",
    description: "Average device power draw",
    benchmark: { kind: "context" },
  },
];

const DIAGNOSTIC_METRICS = [
  { name: "DCGM_FI_DEV_GPU_UTIL", label: "GPU utilization", mode: "percent" },
  { name: "DCGM_FI_DEV_MEM_COPY_UTIL", label: "Memory copy utilization", mode: "percent" },
  { name: "DCGM_FI_DEV_FB_USED", label: "Framebuffer used", mode: "mib" },
  { name: "DCGM_FI_DEV_FB_FREE", label: "Framebuffer free", mode: "mib" },
  { name: "DCGM_FI_DEV_FB_RESERVED", label: "Framebuffer reserved", mode: "mib" },
  { name: "DCGM_FI_DEV_POWER_USAGE", label: "Power usage", mode: "watts" },
  { name: "DCGM_FI_DEV_GPU_TEMP", label: "GPU temperature", mode: "celsius" },
  { name: "DCGM_FI_DEV_SM_CLOCK", label: "SM clock", mode: "mhz" },
  { name: "DCGM_FI_DEV_MEM_CLOCK", label: "Memory clock", mode: "mhz" },
  { name: "DCGM_FI_DEV_XID_ERRORS", label: "XID errors", mode: "number" },
  { name: "DCGM_FI_PROF_GR_ENGINE_ACTIVE", label: "Graphics engine active", mode: "ratio" },
  { name: "DCGM_FI_PROF_SM_ACTIVE", label: "SM active", mode: "ratio" },
  { name: "DCGM_FI_PROF_SM_OCCUPANCY", label: "SM occupancy", mode: "ratio" },
  { name: "DCGM_FI_PROF_PIPE_TENSOR_ACTIVE", label: "Tensor pipe active", mode: "ratio" },
  { name: "DCGM_FI_PROF_DRAM_ACTIVE", label: "DRAM active", mode: "ratio" },
  { name: "DCGM_FI_PROF_PCIE_TX_BYTES", label: "PCIe transmit", mode: "bytes" },
  { name: "DCGM_FI_PROF_PCIE_RX_BYTES", label: "PCIe receive", mode: "bytes" },
  { name: "DCGM_FI_PROF_NVLINK_TX_BYTES", label: "NVLink transmit", mode: "bytes" },
  { name: "DCGM_FI_PROF_NVLINK_RX_BYTES", label: "NVLink receive", mode: "bytes" },
];

const DISPLAYED_SIGNAL_NAMES = new Set(DIAGNOSTIC_METRICS.map((metric) => metric.name));
const CHART_WIDTH = 900;

const state = {
  snapshot: null,
  selectedWorkloadId: null,
  windowSeconds: 3600,
  loading: false,
};

bindControls();
loadSnapshot();

function bindControls() {
  document.querySelector("#refreshButton").addEventListener("click", loadSnapshot);
  document.querySelectorAll("[data-window]").forEach((button) => {
    button.addEventListener("click", () => {
      state.windowSeconds = Number(button.dataset.window);
      document.querySelectorAll("[data-window]").forEach((item) => {
        item.classList.toggle("active", item === button);
      });
      loadSnapshot();
    });
  });
}

async function loadSnapshot() {
  if (state.loading) return;
  state.loading = true;
  document.querySelector("#refreshButton").disabled = true;
  try {
    const response = await fetch(
      `./api/v1/snapshot?window_seconds=${state.windowSeconds}&max_points=180`,
      { cache: "no-store" },
    );
    if (!response.ok) throw new Error(`Snapshot request failed with ${response.status}`);
    state.snapshot = await response.json();
    const jobs = state.snapshot.jobs || [];
    if (!jobs.some((job) => job.workload_id === state.selectedWorkloadId)) {
      state.selectedWorkloadId = jobs[0]?.workload_id || null;
    }
    render();
    hideNotice();
  } catch (error) {
    showNotice("Telemetry is unavailable. The collector will retry on its next interval.");
    setLiveState("Unavailable", "danger");
  } finally {
    state.loading = false;
    document.querySelector("#refreshButton").disabled = false;
  }
}

function render() {
  renderHeader();
  renderRunContext();
  renderMetrics();
  renderHealth();
  renderWorkers();
  renderDiagnostics();
  renderDetails();
}

function renderHeader() {
  const freshness = secondsSince(state.snapshot.updated_at);
  const staleAfter = state.snapshot.sample_interval_seconds * 3;
  const isLive = freshness <= staleAfter;
  setLiveState(isLive ? "Live" : "Delayed", isLive ? "success" : "warning");
  document.querySelector("#lastUpdated").textContent = `Last sample ${relativeTime(state.snapshot.updated_at)}`;
}

function renderRunContext() {
  const job = selectedJob();
  const container = document.querySelector("#runContext");
  if (!job) {
    container.innerHTML = emptyState("No configured workload has been observed.");
    return;
  }

  const workload = job.workload;
  const topology = workload.disaggregated ? "Disaggregated" : "Aggregated";
  const selector = state.snapshot.jobs.length > 1
    ? `
      <label class="workload-control">
        <span>Workload</span>
        <select id="workloadSelect">
          ${state.snapshot.jobs.map((item) => `
            <option value="${escapeHtml(item.workload_id)}" ${item.workload_id === job.workload_id ? "selected" : ""}>
              ${escapeHtml(item.workload.name)}
            </option>
          `).join("")}
        </select>
      </label>
    `
    : "";

  container.innerHTML = `
    <div class="run-identity">
      <span>${escapeHtml(workload.runtime)} · ${escapeHtml(workload.namespace)}</span>
      <h2>${escapeHtml(workload.name)}</h2>
      <p>${escapeHtml(workload.model_id)}</p>
    </div>
    <div class="run-facts">
      ${fact("Backend", workload.backend)}
      ${fact("Topology", topology)}
      ${fact("Configured GPUs", formatNumber(workload.total_gpus))}
      ${fact("Worker pods", formatNumber(job.workers.length))}
    </div>
    ${selector}
  `;

  const select = document.querySelector("#workloadSelect");
  if (select) {
    select.addEventListener("change", () => {
      state.selectedWorkloadId = select.value;
      render();
    });
  }
}

function renderMetrics() {
  const job = selectedJob();
  const container = document.querySelector("#metricGrid");
  if (!job) {
    container.innerHTML = emptyState("No workload telemetry is available.");
    return;
  }

  const byMetric = groupSeries(job.telemetry.series);
  container.innerHTML = PRIMARY_METRICS.map((metric) => {
    const sourceNames = metric.sourceNames || [metric.name];
    const series = sourceNames.flatMap((name) => byMetric.get(name) || []);
    const points = metric.name === "TEI_GPU_MEMORY_PRESSURE"
      ? memoryPressureSeries(byMetric)
      : aggregateSeries(series, metric.mode);
    return metricCard(metric, series, points);
  }).join("");
  attachChartHover();
}

function renderHealth() {
  const job = selectedJob();
  const container = document.querySelector("#healthGrid");
  if (!job) {
    container.innerHTML = emptyState("No health signals are available.");
    return;
  }

  const temperatures = latestValues(job, "DCGM_FI_DEV_GPU_TEMP");
  const maxTemperature = temperatures.length ? Math.max(...temperatures) : null;
  const temperatureTone = maxTemperature === null
    ? "muted"
    : maxTemperature >= 90
      ? "danger"
      : maxTemperature >= 80
        ? "warning"
        : "success";

  const xidValues = latestValues(job, "DCGM_FI_DEV_XID_ERRORS");
  const xidErrors = [...new Set(xidValues.filter((value) => value !== 0))];
  const tx = sum(latestValues(job, "DCGM_FI_PROF_PCIE_TX_BYTES"));
  const rx = sum(latestValues(job, "DCGM_FI_PROF_PCIE_RX_BYTES"));
  const observed = observedSignalNames(job);
  const reportingCount = [...DISPLAYED_SIGNAL_NAMES].filter((name) => observed.has(name)).length;
  const missingCount = DISPLAYED_SIGNAL_NAMES.size - reportingCount;

  container.innerHTML = [
    healthItem(
      "Max temperature",
      maxTemperature === null ? "n/a" : `${formatNumber(maxTemperature)} °C`,
      maxTemperature === null ? "No temperature samples" : "Hottest reporting GPU",
      temperatureTone,
    ),
    healthItem(
      "PCIe throughput",
      tx + rx ? formatBytes(tx + rx) : "n/a",
      tx + rx ? `${formatBytes(tx)} TX · ${formatBytes(rx)} RX` : "No transport samples",
      "neutral",
    ),
    healthItem(
      "XID state",
      xidErrors.length ? `XID ${xidErrors.join(", ")}` : xidValues.length ? "Clear" : "n/a",
      xidErrors.length ? "GPU error reported" : xidValues.length ? "No device errors" : "No XID samples",
      xidErrors.length ? "danger" : xidValues.length ? "success" : "muted",
    ),
    healthItem(
      "Displayed signals",
      `${reportingCount} / ${DISPLAYED_SIGNAL_NAMES.size}`,
      missingCount ? `${missingCount} unavailable in this window` : "All reporting",
      missingCount ? "warning" : "success",
    ),
  ].join("");
}

function renderWorkers() {
  const job = selectedJob();
  const workers = job?.workers || [];
  document.querySelector("#workerCount").textContent = `${workers.length} ${plural("worker", workers.length)}`;
  const container = document.querySelector("#workerList");
  if (!job || !workers.length) {
    container.innerHTML = emptyState("No Kubernetes worker pods are attributed to this workload.");
    return;
  }

  container.innerHTML = workers.map((worker) => {
    const series = job.telemetry.series.filter((item) => item.scope.pod_uid === worker.uid);
    const gpuCount = scopedGpuCount(series);
    const utilization = latestAverage(series, "DCGM_FI_DEV_GPU_UTIL");
    const memoryUsed = latestAverage(series, "DCGM_FI_DEV_FB_USED");
    const memoryFree = latestAverage(series, "DCGM_FI_DEV_FB_FREE");
    const memoryReserved = latestAverage(series, "DCGM_FI_DEV_FB_RESERVED") || 0;
    const memoryPressure = memoryUsed !== null && memoryFree !== null && memoryUsed + memoryFree + memoryReserved > 0
      ? (memoryUsed / (memoryUsed + memoryFree + memoryReserved)) * 100
      : null;
    const freshness = secondsSince(worker.last_seen_at);
    const fresh = freshness <= state.snapshot.sample_interval_seconds * 3;

    return `
      <div class="worker-row">
        <div><strong>${escapeHtml(worker.name)}</strong><span>${escapeHtml(worker.namespace)} · ${escapeHtml(worker.container_names.join(", "))}</span></div>
        <div><strong>${escapeHtml(worker.runtime_role || "worker")}</strong><span>${escapeHtml(worker.runtime_state)} · ${escapeHtml(worker.runtime_instance)}</span></div>
        <div><strong>${escapeHtml(worker.node_name || "Unscheduled")}</strong><span>${shortId(worker.uid)}</span></div>
        <div><strong>${gpuCount || "n/a"}</strong><span>${gpuCount ? plural("device", gpuCount) : "No scope"}</span></div>
        <div><strong>${utilization === null ? "n/a" : `${formatNumber(utilization)}%`}</strong><span>Latest average</span></div>
        <div><strong>${memoryPressure === null ? "n/a" : `${formatNumber(memoryPressure)}%`}</strong><span>Framebuffer</span></div>
        <div><strong class="status-text ${fresh ? "success" : "warning"}">${relativeTime(worker.last_seen_at)}</strong><span>${fresh ? "Reporting" : "Delayed"}</span></div>
      </div>
    `;
  }).join("");
}

function renderDiagnostics() {
  const job = selectedJob();
  const container = document.querySelector("#diagnosticList");
  const summary = document.querySelector("#coverageSummary");
  if (!job) {
    summary.textContent = "No coverage information";
    container.innerHTML = emptyState("No telemetry coverage is available.");
    return;
  }

  const coverage = job.coverage || { status: "missing", expected_gpu_count: 0, metrics: [] };
  const coverageByMetric = new Map(
    coverage.metrics.map((metric) => [metric.metric_name, metric]),
  );
  const byMetric = groupSeries(job.telemetry.series);
  summary.innerHTML = `
    <strong class="status-text ${coverageTone(coverage.status)}">${escapeHtml(capitalize(coverage.status))}</strong>
    <span>${coverage.observed_gpu_count || 0} observed / ${coverage.expected_gpu_count || 0} expected GPUs</span>
  `;

  container.innerHTML = DIAGNOSTIC_METRICS.map((metric) => {
    const series = byMetric.get(metric.name) || [];
    const values = series.flatMap((item) => item.samples.map((sample) => scaleValue(sample.value, metric.mode)));
    const latest = latestAverage(series, metric.name, metric.mode);
    const coverageMetric = coverageByMetric.get(metric.name) || {
      status: "missing",
      reporting_gpu_count: 0,
      expected_gpu_count: coverage.expected_gpu_count || 0,
      series_count: 0,
      sample_count: 0,
      latest_sample_at: null,
    };
    const valueRange = values.length
      ? `${formatMetricValue(Math.min(...values), metric.mode)} – ${formatMetricValue(Math.max(...values), metric.mode)}`
      : "n/a";
    return `
      <div class="diagnostic-row">
        <div><strong>${escapeHtml(metric.label)}</strong><span>${escapeHtml(metric.name)}</span></div>
        <div><strong>${formatMetricValue(latest, metric.mode)}</strong><span>Latest GPU average</span></div>
        <div><strong>${valueRange}</strong><span>Window range</span></div>
        <div><strong>${coverageMetric.reporting_gpu_count} / ${coverageMetric.expected_gpu_count}</strong><span>Reporting GPUs</span></div>
        <div><strong>${formatNumber(coverageMetric.sample_count)}</strong><span>${coverageMetric.series_count} ${plural("series", coverageMetric.series_count)}</span></div>
        <div><strong class="status-text ${coverageTone(coverageMetric.status)}">${escapeHtml(capitalize(coverageMetric.status))}</strong><span>${coverageMetric.latest_sample_at ? relativeTime(coverageMetric.latest_sample_at) : "No samples"}</span></div>
      </div>
    `;
  }).join("");
}

function renderDetails() {
  const job = selectedJob();
  const workloadContainer = document.querySelector("#workloadDetails");
  const scopeContainer = document.querySelector("#scopeDetails");
  const attributionContainer = document.querySelector("#attributionDetails");
  if (!job) {
    workloadContainer.innerHTML = emptyState("No workload configuration is available.");
    scopeContainer.innerHTML = emptyState("No GPU scopes are available.");
  } else {
    workloadContainer.innerHTML = workloadDetails(job.workload);
    scopeContainer.innerHTML = scopeDetails(job.telemetry.series);
  }
  attributionContainer.innerHTML = attributionDetails();
}

function workloadDetails(workload) {
  const components = workload.components || [];
  return `
    <div class="detail-facts">
      ${detailFact("Workload UID", workload.uid || "n/a")}
      ${detailFact("API version", workload.api_version)}
      ${detailFact("Pod selectors", formatNumber((workload.pod_selectors || []).length))}
      ${detailFact("Components", formatNumber(components.length))}
    </div>
    <div class="component-list">
      ${components.map((component) => `
        <div class="component-row">
          <div><strong>${escapeHtml(component.name)}</strong><span>${escapeHtml(component.component_type)}</span></div>
          <div><strong>${formatNumber(component.replicas)}</strong><span>Replicas</span></div>
          <div><strong>${formatNumber(component.gpus_per_replica)}</strong><span>GPUs / replica</span></div>
          <div><strong>${escapeHtml(component.image || "n/a")}</strong><span>Image</span></div>
          <details><summary>Engine and placement</summary><pre>${escapeHtml(JSON.stringify({ placement: component.placement, characteristics: component.x }, null, 2))}</pre></details>
        </div>
      `).join("")}
    </div>
  `;
}

function scopeDetails(series) {
  const scopes = new Map();
  series.forEach((item) => {
    const scope = item.scope;
    if (!scope.gpu_uuid && !scope.gpu_index) return;
    const key = [scope.gpu_uuid, scope.node_name, scope.gpu_index, scope.gpu_instance_id, scope.pod_uid, scope.container_name].join(":");
    if (!scopes.has(key)) scopes.set(key, { scope, labels: item.labels });
  });
  if (!scopes.size) return emptyState("No GPU-scoped series are available.");
  return [...scopes.values()].map(({ scope, labels }) => `
    <div class="scope-row">
      <div><strong>${escapeHtml(scope.gpu_uuid || `GPU ${scope.gpu_index}`)}</strong><span>Index ${escapeHtml(scope.gpu_index || "n/a")} · MIG ${escapeHtml(scope.gpu_instance_id || "n/a")}</span></div>
      <div><strong>${escapeHtml(scope.pod_name || scope.pod_uid || "Unattributed")}</strong><span>${escapeHtml(scope.pod_namespace || "n/a")} · ${escapeHtml(scope.container_name || "n/a")}</span></div>
      <div><strong>${escapeHtml(scope.node_name || "Unknown node")}</strong><span>${escapeHtml(scope.attribution_method || "unknown attribution")}</span></div>
      <pre>${escapeHtml(JSON.stringify(labels))}</pre>
    </div>
  `).join("");
}

function attributionDetails() {
  const attribution = state.snapshot.attribution || { unattributed_series_count: 0, reasons: {} };
  const series = state.snapshot.unattributed_telemetry?.series || [];
  if (!series.length) return '<p class="detail-empty status-text success">All reporting series are attributed.</p>';
  return `
    <p class="detail-empty status-text warning">${attribution.unattributed_series_count} unattributed ${plural("series", attribution.unattributed_series_count)}</p>
    ${series.map((item) => `
      <div class="scope-row">
        <div><strong>${escapeHtml(item.metric_name)}</strong><span>${formatNumber(item.samples.length)} samples</span></div>
        <div><strong>${escapeHtml(item.scope.pod_name || item.scope.pod_uid || "No pod identity")}</strong><span>${escapeHtml(item.scope.pod_namespace || "No namespace")}</span></div>
        <div><strong>${escapeHtml(item.scope.gpu_uuid || `GPU ${item.scope.gpu_index || "n/a"}`)}</strong><span>${escapeHtml(item.scope.node_name || "Unknown node")}</span></div>
        <div><strong class="status-text warning">${escapeHtml(item.scope.attribution_method || "unattributed_unknown")}</strong></div>
      </div>
    `).join("")}
  `;
}

function detailFact(label, value) {
  return `<div><span>${escapeHtml(label)}</span><strong>${escapeHtml(value)}</strong></div>`;
}

function coverageTone(status) {
  if (status === "complete") return "success";
  if (status === "partial") return "warning";
  return "danger";
}

function capitalize(value) {
  return value ? `${value[0].toUpperCase()}${value.slice(1)}` : "Unknown";
}

function metricCard(metric, series, points) {
  const latest = points.at(-1)?.value ?? null;
  const devices = scopedGpuCount(series);
  const benchmark = evaluateBenchmark(metric.benchmark, latest);

  return `
    <article class="metric-card">
      <div class="metric-heading">
        <div>
          <h3>${escapeHtml(metric.label)}</h3>
          <p>${escapeHtml(metric.description)}</p>
        </div>
        <div class="metric-value">
          <strong id="value-${metric.name}">${formatMetricValue(latest, metric.mode)}</strong>
          <span class="benchmark-badge ${benchmark.tone}">${benchmark.label}</span>
        </div>
      </div>
      ${points.length ? lineChart(metric, points) : emptyChart("No samples in this window")}
      <p class="metric-source">${devices ? `${devices} ${plural("GPU", devices)}` : "No GPU scope"} · ${escapeHtml(metric.name)}</p>
    </article>
  `;
}

function lineChart(metric, points) {
  const width = CHART_WIDTH;
  const height = 220;
  const padX = 24;
  const padTop = 18;
  const padBottom = 32;
  const values = points.map((point) => point.value);
  const fixedPercentScale = metric.mode === "percent" || metric.mode === "ratio";
  const min = fixedPercentScale ? 0 : Math.min(...values);
  const max = fixedPercentScale ? 100 : Math.max(...values);
  const spread = max - min || 1;
  const chartWidth = width - padX * 2;
  const chartHeight = height - padTop - padBottom;
  const coords = points.map((point, index) => ({
    x: padX + (points.length === 1 ? chartWidth : (index / (points.length - 1)) * chartWidth),
    y: padTop + chartHeight - ((point.value - min) / spread) * chartHeight,
    value: point.value,
    timestamp: point.timestamp,
  }));
  const polyline = coords.map((point) => `${point.x.toFixed(1)},${point.y.toFixed(1)}`).join(" ");
  const last = coords.at(-1);
  const ticks = [coords[0], coords[Math.floor((coords.length - 1) / 2)], last];
  const coordinateData = coords
    .map((point) => `${point.x.toFixed(1)}:${point.y.toFixed(1)}:${point.value}:${Date.parse(point.timestamp)}`)
    .join("|");
  const yForValue = (value) => padTop + chartHeight - ((value - min) / spread) * chartHeight;
  const benchmarkOverlay = renderBenchmarkOverlay(
    metric.benchmark,
    yForValue,
    padX,
    chartWidth,
  );

  return `
    <svg class="metric-chart" viewBox="0 0 ${width} ${height}" role="img"
      aria-label="${escapeHtml(metric.label)} time series"
      data-metric="${metric.name}" data-mode="${metric.mode}" data-coords="${coordinateData}">
      <defs>
        <clipPath id="clip-${metric.name}">
          <rect class="active-clip" x="0" y="0" width="${last.x}" height="${height}"></rect>
        </clipPath>
      </defs>
      <line class="chart-guide" x1="${padX}" y1="${padTop}" x2="${width - padX}" y2="${padTop}"></line>
      <line class="chart-guide" x1="${padX}" y1="${padTop + chartHeight / 2}" x2="${width - padX}" y2="${padTop + chartHeight / 2}"></line>
      <line class="chart-guide" x1="${padX}" y1="${padTop + chartHeight}" x2="${width - padX}" y2="${padTop + chartHeight}"></line>
      ${benchmarkOverlay}
      <polyline class="chart-line-muted" points="${polyline}"></polyline>
      <polyline class="chart-line-active" points="${polyline}" clip-path="url(#clip-${metric.name})"></polyline>
      <line class="chart-hover-line" x1="${last.x}" y1="${padTop}" x2="${last.x}" y2="${padTop + chartHeight}"></line>
      <circle class="chart-hover-dot" cx="${last.x}" cy="${last.y}" r="5"></circle>
      ${ticks.map((point, index) => `
        <text class="chart-label" x="${point.x}" y="${height - 4}" text-anchor="${index === 0 ? "start" : index === 2 ? "end" : "middle"}">
          ${formatTime(point.timestamp)}
        </text>
      `).join("")}
      <rect class="chart-hit-area" x="0" y="0" width="${width}" height="${height}"></rect>
    </svg>
  `;
}

function attachChartHover() {
  document.querySelectorAll(".metric-chart").forEach((chart) => {
    const coords = chart.dataset.coords.split("|").map((entry) => {
      const [x, y, value, timestamp] = entry.split(":");
      return { x: Number(x), y: Number(y), value: Number(value), timestamp: Number(timestamp) };
    });
    const hoverLine = chart.querySelector(".chart-hover-line");
    const hoverDot = chart.querySelector(".chart-hover-dot");
    const activeClip = chart.querySelector(".active-clip");
    const valueElement = document.querySelector(`#value-${chart.dataset.metric}`);
    const metric = PRIMARY_METRICS.find((item) => item.name === chart.dataset.metric);
    const badge = chart.closest(".metric-card").querySelector(".benchmark-badge");
    const setPoint = (point) => {
      hoverLine.setAttribute("x1", point.x);
      hoverLine.setAttribute("x2", point.x);
      hoverDot.setAttribute("cx", point.x);
      hoverDot.setAttribute("cy", point.y);
      activeClip.setAttribute("width", point.x);
      valueElement.textContent = formatMetricValue(point.value, chart.dataset.mode);
      setBenchmarkBadge(badge, evaluateBenchmark(metric.benchmark, point.value));
    };
    chart.addEventListener("mousemove", (event) => {
      const bounds = chart.getBoundingClientRect();
      const x = ((event.clientX - bounds.left) / bounds.width) * CHART_WIDTH;
      const nearest = coords.reduce((best, point) =>
        Math.abs(point.x - x) < Math.abs(best.x - x) ? point : best,
      );
      setPoint(nearest);
      chart.classList.add("hovering");
    });
    chart.addEventListener("mouseleave", () => {
      setPoint(coords.at(-1));
      chart.classList.remove("hovering");
    });
  });
}

function aggregateSeries(series, mode) {
  const byTimestamp = new Map();
  series.forEach((item) => {
    item.samples.forEach((sample) => {
      const values = byTimestamp.get(sample.timestamp) || [];
      values.push(scaleValue(sample.value, mode));
      byTimestamp.set(sample.timestamp, values);
    });
  });
  return [...byTimestamp.entries()]
    .sort(([a], [b]) => new Date(a) - new Date(b))
    .map(([timestamp, values]) => ({
      timestamp,
      value: sum(values) / values.length,
    }));
}

function memoryPressureSeries(byMetric) {
  const used = totalsByTimestamp(byMetric.get("DCGM_FI_DEV_FB_USED") || []);
  const free = totalsByTimestamp(byMetric.get("DCGM_FI_DEV_FB_FREE") || []);
  const reserved = totalsByTimestamp(byMetric.get("DCGM_FI_DEV_FB_RESERVED") || []);
  return [...used.entries()]
    .filter(([timestamp]) => free.has(timestamp))
    .sort(([a], [b]) => new Date(a) - new Date(b))
    .map(([timestamp, usedValue]) => {
      const total = usedValue + free.get(timestamp) + (reserved.get(timestamp) || 0);
      return {
        timestamp,
        value: total > 0 ? (usedValue / total) * 100 : 0,
      };
    });
}

function totalsByTimestamp(series) {
  const totals = new Map();
  series.forEach((item) => {
    item.samples.forEach((sample) => {
      totals.set(sample.timestamp, (totals.get(sample.timestamp) || 0) + sample.value);
    });
  });
  return totals;
}

function evaluateBenchmark(benchmark, value) {
  if (value === null || value === undefined) {
    return { label: "No data", tone: "muted" };
  }
  if (benchmark.kind === "context") {
    return { label: "Context", tone: "context" };
  }
  if (benchmark.kind === "high") {
    if (value >= benchmark.good) return { label: "Good", tone: "good" };
    if (value >= benchmark.watch) return { label: "Watch", tone: "watch" };
    return { label: "Needs attention", tone: "attention" };
  }
  if (value >= benchmark.goodMin && value <= benchmark.goodMax) {
    return { label: "Good", tone: "good" };
  }
  if (value >= benchmark.watchMin && value <= benchmark.watchMax) {
    return { label: "Watch", tone: "watch" };
  }
  return { label: "Needs attention", tone: "attention" };
}

function setBenchmarkBadge(element, benchmark) {
  element.className = `benchmark-badge ${benchmark.tone}`;
  element.textContent = benchmark.label;
}

function renderBenchmarkOverlay(benchmark, yForValue, x, width) {
  if (benchmark.kind === "context") return "";
  if (benchmark.kind === "high") {
    const top = yForValue(100);
    const bottom = yForValue(benchmark.good);
    return `
      <rect class="benchmark-zone" x="${x}" y="${top}" width="${width}" height="${bottom - top}"></rect>
      <line class="benchmark-line" x1="${x}" y1="${bottom}" x2="${x + width}" y2="${bottom}"></line>
    `;
  }
  const top = yForValue(benchmark.goodMax);
  const bottom = yForValue(benchmark.goodMin);
  return `
    <rect class="benchmark-zone" x="${x}" y="${top}" width="${width}" height="${bottom - top}"></rect>
    <line class="benchmark-line" x1="${x}" y1="${top}" x2="${x + width}" y2="${top}"></line>
    <line class="benchmark-line" x1="${x}" y1="${bottom}" x2="${x + width}" y2="${bottom}"></line>
  `;
}

function groupSeries(series) {
  const grouped = new Map();
  series.forEach((item) => {
    const items = grouped.get(item.metric_name) || [];
    items.push(item);
    grouped.set(item.metric_name, items);
  });
  return grouped;
}

function latestValues(job, metricName) {
  return job.telemetry.series
    .filter((series) => series.metric_name === metricName)
    .map((series) => series.samples.at(-1)?.value)
    .filter((value) => typeof value === "number");
}

function latestAverage(series, metricName, mode = "number") {
  const values = series
    .filter((item) => item.metric_name === metricName)
    .map((item) => item.samples.at(-1)?.value)
    .filter((value) => typeof value === "number");
  return values.length ? sum(values.map((value) => scaleValue(value, mode))) / values.length : null;
}

function observedSignalNames(job) {
  return new Set(
    job.telemetry.series
      .filter((series) => series.samples.length)
      .map((series) => series.metric_name),
  );
}

function scopedGpuCount(series) {
  const devices = new Set();
  series.forEach((item) => {
    const scope = item.scope;
    if (scope.gpu_uuid || scope.gpu_index) {
      devices.add(scope.gpu_uuid || `${scope.node_name}:${scope.gpu_index}:${scope.gpu_instance_id || ""}`);
    }
  });
  return devices.size;
}

function selectedJob() {
  return (state.snapshot.jobs || []).find((job) => job.workload_id === state.selectedWorkloadId) || null;
}

function fact(label, value) {
  return `<div><span>${escapeHtml(label)}</span><strong>${escapeHtml(value)}</strong></div>`;
}

function healthItem(label, value, detail, tone) {
  return `
    <div class="health-item">
      <span>${escapeHtml(label)}</span>
      <strong class="status-text ${tone}">${escapeHtml(value)}</strong>
      <p>${escapeHtml(detail)}</p>
    </div>
  `;
}

function emptyState(message) {
  return `<div class="empty-state">${escapeHtml(message)}</div>`;
}

function emptyChart(message) {
  return `<div class="empty-chart"><span></span><p>${escapeHtml(message)}</p></div>`;
}

function setLiveState(label, tone) {
  const element = document.querySelector("#liveState");
  element.className = `live-state ${tone}`;
  element.querySelector("strong").textContent = label;
}

function showNotice(message) {
  const notice = document.querySelector("#notice");
  notice.textContent = message;
  notice.hidden = false;
}

function hideNotice() {
  document.querySelector("#notice").hidden = true;
}

function scaleValue(value, mode) {
  return mode === "ratio" ? value * 100 : value;
}

function formatMetricValue(value, mode) {
  if (value === null || value === undefined || Number.isNaN(value)) return "n/a";
  if (mode === "percent" || mode === "ratio") return `${formatNumber(value)}%`;
  if (mode === "mib") return formatMiB(value);
  if (mode === "watts") return `${formatNumber(value)} W`;
  if (mode === "celsius") return `${formatNumber(value)} °C`;
  if (mode === "mhz") return `${formatNumber(value)} MHz`;
  if (mode === "bytes") return formatBytes(value);
  return formatNumber(value);
}

function formatMiB(value) {
  if (Math.abs(value) >= 1024) return `${formatNumber(value / 1024)} GiB`;
  return `${formatNumber(value)} MiB`;
}

function formatBytes(value) {
  const units = ["B/s", "KB/s", "MB/s", "GB/s", "TB/s"];
  let scaled = Math.abs(value);
  let index = 0;
  while (scaled >= 1000 && index < units.length - 1) {
    scaled /= 1000;
    index += 1;
  }
  if (value < 0) scaled *= -1;
  return `${formatNumber(scaled)} ${units[index]}`;
}

function formatNumber(value) {
  return new Intl.NumberFormat("en-US", { maximumFractionDigits: 1 }).format(Number(value) || 0);
}

function formatTime(value) {
  return new Intl.DateTimeFormat("en-US", { hour: "numeric", minute: "2-digit" }).format(new Date(value));
}

function relativeTime(value) {
  if (!value) return "No samples";
  const seconds = secondsSince(value);
  if (seconds < 5) return "just now";
  if (seconds < 60) return `${seconds}s ago`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
  return `${Math.floor(seconds / 86400)}d ago`;
}

function secondsSince(value) {
  return Math.max(0, Math.floor((Date.now() - new Date(value).getTime()) / 1000));
}

function sum(values) {
  return values.reduce((total, value) => total + value, 0);
}

function plural(word, count) {
  return count === 1 ? word : `${word}s`;
}

function shortId(value) {
  return value.length > 18 ? `${value.slice(0, 8)}…${value.slice(-6)}` : value;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

setInterval(() => {
  if (document.visibilityState === "visible") loadSnapshot();
}, 10000);
