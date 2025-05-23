package main

import (
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"
)

const baseAddress = "http://balancer:8090"

var client = http.Client{
	Timeout: 3 * time.Second,
}

func TestBalancer(t *testing.T) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		t.Skip("Integration test is not enabled")
	}

	const requestCount = 20
	serversSeen := make(map[string]bool)

	for i := 0; i < requestCount; i++ {
		resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
		if err != nil {
			t.Fatalf("Request %d failed: %v", i+1, err)
		}

		serverID := resp.Header.Get("lb-from")
		resp.Body.Close()

		if serverID == "" {
			t.Errorf("Request %d: missing lb-from header", i+1)
		} else {
			t.Logf("Request %d handled by: %s", i+1, serverID)
			serversSeen[serverID] = true
		}
	}

	if len(serversSeen) < 2 {
		t.Errorf("Expected requests to be distributed across at least 2 servers, got: %v", serversSeen)
	} else {
		t.Logf("Requests were distributed across servers: %v", serversSeen)
	}
}

func BenchmarkBalancer(b *testing.B) {
	for i := 0; i < b.N; i++ {
		resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
		if err != nil {
			b.Fatalf("Request failed: %v", err)
		}
		resp.Body.Close()
	}
}
