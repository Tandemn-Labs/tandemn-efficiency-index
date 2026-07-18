package tui

import (
	"strings"
	"testing"

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
