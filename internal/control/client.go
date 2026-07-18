package control

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Response map[string]any

type APIError struct {
	StatusCode int
	Message    string
}

func (e *APIError) Error() string {
	return e.Message
}

type Client struct {
	baseURL    string
	token      string
	httpClient *http.Client
}

func NewClient(baseURL, token string, timeout time.Duration) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		token:   token,
		httpClient: &http.Client{
			Timeout: timeout,
		},
	}
}

func (c *Client) Health() (Response, error) {
	return c.request(http.MethodGet, "/healthz", true)
}

func (c *Client) Readiness() (Response, error) {
	return c.request(http.MethodGet, "/readyz", true)
}

func (c *Client) Status() (Response, error) {
	return c.request(http.MethodGet, "/api/v1/status", false)
}

func (c *Client) Snapshot(windowSeconds, maxPoints int) (Response, error) {
	query := url.Values{}
	query.Set("window_seconds", fmt.Sprintf("%d", windowSeconds))
	query.Set("max_points", fmt.Sprintf("%d", maxPoints))
	return c.request(http.MethodGet, "/api/v1/snapshot?"+query.Encode(), false)
}

func (c *Client) StartObservation() (Response, error) {
	return c.request(http.MethodPost, "/api/v1/observation/start", false)
}

func (c *Client) StopObservation() (Response, error) {
	return c.request(http.MethodPost, "/api/v1/observation/stop", false)
}

func (c *Client) RestartObservation() (Response, error) {
	return c.request(http.MethodPost, "/api/v1/observation/restart", false)
}

func (c *Client) request(method, path string, allowUnavailable bool) (Response, error) {
	var body io.Reader
	if method == http.MethodPost {
		body = bytes.NewReader(nil)
	}
	req, err := http.NewRequest(method, c.baseURL+path, body)
	if err != nil {
		return nil, fmt.Errorf("build TEI request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("connect to TEI at %s: %w", c.baseURL, err)
	}
	defer resp.Body.Close()

	payload := Response{}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("decode TEI response: %w", err)
	}
	if resp.StatusCode >= http.StatusBadRequest && !(allowUnavailable && resp.StatusCode == http.StatusServiceUnavailable) {
		message, _ := payload["error"].(string)
		if message == "" {
			message = resp.Status
		}
		return nil, &APIError{StatusCode: resp.StatusCode, Message: message}
	}
	return payload, nil
}
