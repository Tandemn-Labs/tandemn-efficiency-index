package tui

import (
	"errors"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/tandemn-labs/tandemn-efficiency-index/internal/control"
)

func TestFormatStatus(t *testing.T) {
	payload := control.Response{
		"lifecycle":              "running",
		"ready":                  true,
		"last_tick_completed_at": "2026-07-17T12:00:00Z",
		"collector": map[string]any{
			"last_prometheus_check_at": "2026-07-17T12:00:00Z",
			"storage":                  map[string]any{"writable": true},
		},
	}

	formatted := FormatStatus(payload)
	for _, expected := range []string{"running", "ready", "connected", "writable"} {
		if !strings.Contains(formatted, expected) {
			t.Fatalf("status does not contain %q: %s", expected, formatted)
		}
	}
}

func TestFormatWorkloads(t *testing.T) {
	payload := control.Response{
		"jobs": []any{
			map[string]any{
				"workload": map[string]any{
					"runtime":   "dynamo",
					"namespace": "inference",
					"name":      "qwen",
					"model_id":  "Qwen/Qwen3",
				},
				"workers":  []any{map[string]any{"name": "qwen-worker"}},
				"coverage": map[string]any{"status": "complete"},
			},
		},
	}

	formatted := FormatWorkloads(payload)
	for _, expected := range []string{"inference/qwen", "Qwen/Qwen3", "1 workers", "complete"} {
		if !strings.Contains(formatted, expected) {
			t.Fatalf("workloads do not contain %q: %s", expected, formatted)
		}
	}
}

func TestFormatResources(t *testing.T) {
	payload := control.Response{
		"resources": map[string]any{
			"rayservices.ray.io": map[string]any{
				"selected_version": "v1",
				"served_versions":  []any{"v1", "v1alpha1"},
			},
			"dynamographdeployments.nvidia.com": map[string]any{
				"selected_version": "v1beta1",
				"served_versions":  []any{"v1beta1"},
			},
		},
	}

	formatted := formatResources(payload)
	for _, expected := range []string{
		"2 Kubernetes resources",
		"dynamographdeployments.nvidia.com",
		"v1beta1",
		"rayservices.ray.io",
		"v1, v1alpha1",
	} {
		if !strings.Contains(formatted, expected) {
			t.Fatalf("resources do not contain %q: %s", expected, formatted)
		}
	}
}

func TestCommandSuggestionsOpenAndFilterFromSlash(t *testing.T) {
	all := commandSuggestions("/")
	if len(all) != len(availableCommands) {
		t.Fatalf("expected %d commands, got %d", len(availableCommands), len(all))
	}

	filtered := commandSuggestions("/st")
	if len(filtered) != 3 {
		t.Fatalf("expected status, start, and stop; got %#v", filtered)
	}
	for index, expected := range []string{"status", "start", "stop"} {
		if filtered[index].name != expected {
			t.Fatalf("expected %s at %d, got %s", expected, index, filtered[index].name)
		}
	}

	if suggestions := commandSuggestions("/snapshot 1h"); suggestions != nil {
		t.Fatalf("expected palette to close for command arguments")
	}
}

func TestCommandSuggestionsSearchCommandAndDescription(t *testing.T) {
	for _, input := range []string{"/art", "/browser"} {
		if suggestions := commandSuggestions(input); len(suggestions) == 0 {
			t.Fatalf("expected suggestions for %q", input)
		}
	}
}

func TestCommandPaletteUsesTwoColumnsOnWideTerminals(t *testing.T) {
	tuiModel := newModel(nil, "")
	tuiModel.width = 100
	tuiModel.input.SetValue("/")

	if !tuiModel.moveSuggestionColumn(1) {
		t.Fatal("expected horizontal palette navigation")
	}
	expectedIndex := (len(availableCommands) + 1) / 2
	if tuiModel.suggestionIndex != expectedIndex {
		t.Fatalf("expected to move to the matching row in the second column, got %d", tuiModel.suggestionIndex)
	}
}

func TestEscapeClearsInputBeforeQuitting(t *testing.T) {
	tuiModel := newModel(nil, "")
	tuiModel.input.SetValue("/stop")
	tuiModel.pendingAction = "/stop"

	updated, command := tuiModel.Update(tea.KeyMsg{Type: tea.KeyEsc})
	result := updated.(model)
	if result.input.Value() != "" || result.pendingAction != "" {
		t.Fatalf("expected escape to clear the input and confirmation state")
	}
	if command != nil {
		t.Fatal("expected escape with input to keep the control room open")
	}
}

func TestStopRequiresASecondEnterToRun(t *testing.T) {
	tuiModel := newModel(nil, "")
	tuiModel.loading = false
	tuiModel.input.SetValue("/stop")

	updated, command := tuiModel.Update(tea.KeyMsg{Type: tea.KeyEnter})
	result := updated.(model)
	if command != nil {
		t.Fatal("expected the first stop submission to request confirmation")
	}
	if result.pendingAction != "/stop" || result.input.Value() != "/stop" {
		t.Fatal("expected /stop to remain in the composer while awaiting confirmation")
	}

	updated, command = result.Update(tea.KeyMsg{Type: tea.KeyEnter})
	result = updated.(model)
	if command == nil || !result.loading {
		t.Fatal("expected the second stop submission to run the command")
	}
}

func TestInitialConnectionFailureRendersOffline(t *testing.T) {
	tuiModel := newModel(nil, "")
	updated, _ := tuiModel.Update(commandResult{
		command:           "status",
		err:               errors.New("connection refused"),
		initial:           true,
		connectionChecked: true,
	})
	result := updated.(model)

	if result.connection != connectionDisconnected {
		t.Fatalf("expected offline connection state, got %q", result.connection)
	}
	if !strings.Contains(result.connectionStatus(), "offline") {
		t.Fatalf("expected offline header status, got %q", result.connectionStatus())
	}
}

func TestFormattersDistinguishHealthReadinessAndLifecycleActions(t *testing.T) {
	payload := control.Response{
		"lifecycle":             "stopped",
		"healthy":               true,
		"ready":                 true,
		"last_transition_error": "retrying collector",
	}

	if output := formatHealth(payload); !strings.Contains(output, "API health") || !strings.Contains(output, "healthy") {
		t.Fatalf("unexpected health output: %s", output)
	}
	if output := formatReadiness(payload); !strings.Contains(output, "Collection readiness") || !strings.Contains(output, "retrying collector") {
		t.Fatalf("unexpected readiness output: %s", output)
	}
	if output := formatLifecycleAction("stop", payload); !strings.Contains(output, "Collection paused") {
		t.Fatalf("unexpected lifecycle action output: %s", output)
	}
}

func TestValidateCommandRejectsUnsupportedArguments(t *testing.T) {
	for _, input := range []string{"/status now", "/snapshot 1h extra", "/snapshot later"} {
		if err := validateCommand(input); err == nil {
			t.Fatalf("expected validation error for %q", input)
		}
	}
	if err := validateCommand("/snapshot 1h"); err != nil {
		t.Fatalf("expected a valid snapshot command: %v", err)
	}
}

func TestDashboardBindsStatusAndWorkloads(t *testing.T) {
	tuiModel := newModel(nil, "")
	tuiModel.status = control.Response{
		"lifecycle": "running",
		"ready":     true,
		"collector": map[string]any{
			"last_prometheus_check_at": "2026-07-20T12:00:00Z",
			"storage":                  map[string]any{"writable": true},
		},
	}
	tuiModel.updateDashboardData("workloads", control.Response{
		"summary": map[string]any{"workload_count": 1, "gpu_count": 4},
		"jobs": []any{map[string]any{
			"workload": map[string]any{
				"namespace": "inference",
				"name":      "qwen",
				"runtime":   "dynamo",
				"model_id":  "Qwen/Qwen3",
			},
			"workers":  []any{map[string]any{"name": "qwen-worker"}},
			"coverage": map[string]any{"status": "complete"},
		}},
	})

	if !strings.Contains(tuiModel.summaryBar(), "WORKLOADS") {
		t.Fatalf("expected workload summary in status bar: %s", tuiModel.summaryBar())
	}
	if output := tuiModel.workloadDetail(48, 10); !strings.Contains(output, "inference/qwen") || !strings.Contains(output, "Qwen/Qwen3") {
		t.Fatalf("unexpected workload detail: %s", output)
	}
}

func TestWorkloadNavigationWraps(t *testing.T) {
	tuiModel := newModel(nil, "")
	tuiModel.snapshot = control.Response{
		"jobs": []any{
			map[string]any{"workload": map[string]any{"name": "first"}},
			map[string]any{"workload": map[string]any{"name": "second"}},
		},
	}

	if !tuiModel.moveWorkload(-1) || tuiModel.workloadIndex != 1 {
		t.Fatalf("expected workload navigation to wrap to the last row, got %d", tuiModel.workloadIndex)
	}
	if !tuiModel.moveWorkload(1) || tuiModel.workloadIndex != 0 {
		t.Fatalf("expected workload navigation to wrap to the first row, got %d", tuiModel.workloadIndex)
	}
}
