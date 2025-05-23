package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSelectServer(t *testing.T) {
	oldHashFunc := hashFunc
	defer func() { hashFunc = oldHashFunc }()
	hashFunc = func(path string) uint32 { return 0 }

	serversPool = []string{"s1", "s2", "s3"}
	healthyMutex.Lock()
	healthyServers = map[string]bool{
		"s1": true,
		"s2": true,
		"s3": true,
	}
	healthyMutex.Unlock()

	server, err := selectServer("/test")
	if err != nil {
		t.Fatal("Expected no error")
	}
	if server != "s1" {
		t.Errorf("Expected s1, got %s", server)
	}
}

func TestSelectServer_UnhealthyFirst(t *testing.T) {
	oldHashFunc := hashFunc
	defer func() { hashFunc = oldHashFunc }()
	hashFunc = func(path string) uint32 { return 0 }

	serversPool = []string{"s1", "s2", "s3"}
	healthyMutex.Lock()
	healthyServers = map[string]bool{
		"s1": false,
		"s2": true,
		"s3": true,
	}
	healthyMutex.Unlock()

	server, err := selectServer("/test")
	if err != nil {
		t.Fatal("Expected no error")
	}
	if server != "s2" {
		t.Errorf("Expected s2, got %s", server)
	}
}

func TestSelectServer_AllUnhealthy(t *testing.T) {
	oldHashFunc := hashFunc
	defer func() { hashFunc = oldHashFunc }()
	hashFunc = func(path string) uint32 { return 0 }

	serversPool = []string{"s1", "s2", "s3"}
	healthyMutex.Lock()
	healthyServers = map[string]bool{
		"s1": false,
		"s2": false,
		"s3": false,
	}
	healthyMutex.Unlock()

	_, err := selectServer("/test")
	if err == nil {
		t.Error("Expected error, got nil")
	}
}

func TestForward(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	rw := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/", nil)

	err := forward(ts.URL[len("http://"):], rw, req)
	if err != nil {
		t.Errorf("Forward error: %v", err)
	}
	if rw.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", rw.Code)
	}
}
