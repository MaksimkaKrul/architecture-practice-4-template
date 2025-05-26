package integration

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"
)

const baseAddress = "http://balancer:8090"

var client = http.Client{
	Timeout: 5 * time.Second,
}

type SomeDataResponse struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func TestBalancer(t *testing.T) {
	time.Sleep(10 * time.Second)

	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		t.Skip("Integration test is not enabled")
	}

	testKey := "server1"

	var serversSeen = make(map[string]bool)
	const requestCount = 10

	for i := 0; i < requestCount; i++ {
		path := fmt.Sprintf("/api/v1/some-data?key=%s", testKey)
		resp, err := client.Get(fmt.Sprintf("%s%s", baseAddress, path))
		if err != nil {
			t.Errorf("Request %d failed: %v", i+1, err)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			bodyBytes, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			t.Errorf("Request %d: Expected status %d, got %d. Response body: %s", i+1, http.StatusOK, resp.StatusCode, string(bodyBytes))
			continue
		}

		var data SomeDataResponse
		bodyBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			resp.Body.Close()
			t.Errorf("Request %d: Failed to read response body: %v", i+1, err)
			continue
		}
		resp.Body.Close()

		if err := json.Unmarshal(bodyBytes, &data); err != nil {
			t.Errorf("Request %d: Failed to unmarshal JSON response: %v. Body: %s", i+1, err, string(bodyBytes))
			continue
		}

		if data.Key != testKey {
			t.Errorf("Request %d: Expected key '%s' in response, but got '%s'.", i+1, testKey, data.Key)
		}
		if data.Value == "" {
			t.Errorf("Request %d: Received empty value from DB for key '%s'.", i+1, testKey)
		} else {
			t.Logf("Request %d: Successfully retrieved data for key '%s': '%s'", i+1, data.Key, data.Value)
		}

		from := resp.Header.Get("lb-from")
		t.Logf("Request %d handled by: %s", i+1, from)
		if from == "" {
			t.Errorf("Request %d: Missing 'lb-from' header.", i+1)
		} else {
			serversSeen[from] = true
		}
	}

	if len(serversSeen) < 2 {
		t.Errorf("Expected requests to be distributed across at least 2 servers, but got only: %v", serversSeen)
	} else {
		t.Logf("Requests were successfully distributed across servers: %v", serversSeen)
	}
}

func BenchmarkBalancer(b *testing.B) {
	testKey := "server1"

	path := fmt.Sprintf("/api/v1/some-data?key=%s", testKey)

	for i := 0; i < b.N; i++ {
		resp, err := client.Get(fmt.Sprintf("%s%s", baseAddress, path))
		if err != nil {
			b.Fatalf("Benchmark request failed: %v", err)
		}
		resp.Body.Close()
	}
}
