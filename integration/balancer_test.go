package integration

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
	time.Sleep(5 * time.Second)
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		t.Skip("Integration test is not enabled")
	}

	var servers = make(map[string]bool)
	const requestCount = 10

	for i := 0; i < requestCount; i++ {
		path := fmt.Sprintf("/api/v1/some-data?id=%d", i)
		resp, err := client.Get(fmt.Sprintf("%s%s", baseAddress, path))
		if err != nil {
			t.Errorf("request %d failed: %v", i+1, err)
			continue
		}
		from := resp.Header.Get("lb-from")
		t.Logf("request %d from [%s]", i+1, from)
		servers[from] = true
		resp.Body.Close()
	}

	if len(servers) < 2 {
		t.Errorf("expected responses from at least 2 different servers, got: %v", servers)
	}

}
