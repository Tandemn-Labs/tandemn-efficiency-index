package cli

import "testing"

func TestWindowSeconds(t *testing.T) {
	tests := map[string]int{
		"15m": 900,
		"1h":  3600,
		"6h":  21600,
		"24h": 86400,
		"all": 0,
	}
	for input, expected := range tests {
		actual, err := windowSeconds(input)
		if err != nil {
			t.Fatalf("window %s: %v", input, err)
		}
		if actual != expected {
			t.Fatalf("window %s: expected %d, got %d", input, expected, actual)
		}
	}
	if _, err := windowSeconds("2h"); err == nil {
		t.Fatalf("expected invalid window error")
	}
}
