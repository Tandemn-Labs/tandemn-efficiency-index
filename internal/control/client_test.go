package control

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestClientMapsControlEndpointsAndAuthentication(t *testing.T) {
	requests := make(chan *http.Request, 3)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests <- request.Clone(request.Context())
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(Response{"lifecycle": "running"})
	}))
	defer server.Close()

	client := NewClient(server.URL, "secret", time.Second)
	if _, err := client.Status(); err != nil {
		t.Fatalf("status: %v", err)
	}
	if _, err := client.Resources(); err != nil {
		t.Fatalf("resources: %v", err)
	}
	if _, err := client.StopObservation(); err != nil {
		t.Fatalf("stop observation: %v", err)
	}

	statusRequest := <-requests
	if statusRequest.Method != http.MethodGet || statusRequest.URL.Path != "/api/v1/status" {
		t.Fatalf("unexpected status request: %s %s", statusRequest.Method, statusRequest.URL.Path)
	}
	if statusRequest.Header.Get("Authorization") != "Bearer secret" {
		t.Fatalf("missing bearer token")
	}
	resourcesRequest := <-requests
	if resourcesRequest.Method != http.MethodGet || resourcesRequest.URL.Path != "/api/v1/resources" {
		t.Fatalf("unexpected resources request: %s %s", resourcesRequest.Method, resourcesRequest.URL.Path)
	}
	stopRequest := <-requests
	if stopRequest.Method != http.MethodPost || stopRequest.URL.Path != "/api/v1/observation/stop" {
		t.Fatalf("unexpected stop request: %s %s", stopRequest.Method, stopRequest.URL.Path)
	}
}

func TestClientAllowsUnavailableProbeDocumentsOnly(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(writer).Encode(Response{"ready": false, "error": "not ready"})
	}))
	defer server.Close()

	client := NewClient(server.URL, "", time.Second)
	readiness, err := client.Readiness()
	if err != nil {
		t.Fatalf("readiness: %v", err)
	}
	if ready, _ := readiness["ready"].(bool); ready {
		t.Fatalf("expected not-ready document")
	}

	_, err = client.Snapshot(3600, 180)
	apiError, ok := err.(*APIError)
	if !ok || apiError.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected snapshot API error, got %v", err)
	}
}
