package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/roman-mazur/architecture-practice-4-template/httptools"
	"github.com/roman-mazur/architecture-practice-4-template/signal"
)

var port = flag.Int("port", 8080, "server port")

const confResponseDelaySec = "CONF_RESPONSE_DELAY_SEC"
const confHealthFailure = "CONF_HEALTH_FAILURE"
const dbServiceURL = "DB_SERVICE_URL"

type SomeDataResponse struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type DbPutRequest struct {
	Value string `json:"value"`
}

type DbGetResponse struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func main() {
	flag.Parse()

	serverName := os.Getenv("SERVER_NAME")
	dbURL := os.Getenv(dbServiceURL)
	if dbURL == "" {
		log.Fatal("DB_SERVICE_URL environment variable is not set")
	}

	currentDate := time.Now().Format("2006-01-02")
	log.Printf("Server %s starting. Saving current date '%s' to DB with key '%s'", serverName, currentDate, serverName)

	client := &http.Client{Timeout: 5 * time.Second}
	putReqBody := DbPutRequest{Value: currentDate}
	jsonBody, err := json.Marshal(putReqBody)
	if err != nil {
		log.Fatalf("Failed to marshal date for DB put: %v", err)
	}

	dbPostURL := fmt.Sprintf("%s/db/%s", dbURL, serverName)
	req, err := http.NewRequest(http.MethodPost, dbPostURL, bytes.NewBuffer(jsonBody))
	if err != nil {
		log.Fatalf("Failed to create DB POST request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		log.Fatalf("Failed to put initial date to DB (%s): %v", dbPostURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		respBody, _ := io.ReadAll(resp.Body)
		log.Fatalf("Failed to put initial date to DB. Status: %d, Body: %s", resp.StatusCode, string(respBody))
	}
	log.Printf("Successfully saved initial date '%s' to DB for key '%s'", currentDate, serverName)

	h := new(http.ServeMux)

	h.HandleFunc("/health", func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("content-type", "text/plain")
		if failConfig := os.Getenv(confHealthFailure); failConfig == "true" {
			rw.WriteHeader(http.StatusInternalServerError)
			_, _ = rw.Write([]byte("FAILURE"))
		} else {
			rw.WriteHeader(http.StatusOK)
			_, _ = rw.Write([]byte("OK"))
		}
	})

	report := make(Report)

	h.HandleFunc("/api/v1/some-data", func(rw http.ResponseWriter, r *http.Request) {
		respDelayString := os.Getenv(confResponseDelaySec)
		if delaySec, parseErr := strconv.Atoi(respDelayString); parseErr == nil && delaySec > 0 && delaySec < 300 {
			time.Sleep(time.Duration(delaySec) * time.Second)
		}

		report.Process(r)

		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(rw, "Query parameter 'key' is required", http.StatusBadRequest)
			return
		}

		dbGetURL := fmt.Sprintf("%s/db/%s", dbURL, key)
		dbReq, err := http.NewRequest(http.MethodGet, dbGetURL, nil)
		if err != nil {
			log.Printf("Failed to create DB GET request for key '%s': %v", key, err)
			http.Error(rw, "Internal server error", http.StatusInternalServerError)
			return
		}

		dbResp, err := client.Do(dbReq)
		if err != nil {
			log.Printf("Failed to get data from DB for key '%s': %v", key, err)
			http.Error(rw, "Internal server error: DB communication failed", http.StatusInternalServerError)
			return
		}
		defer dbResp.Body.Close()

		if dbResp.StatusCode == http.StatusNotFound {
			log.Printf("Key '%s' not found in DB, returning 404 to client", key)
			rw.WriteHeader(http.StatusNotFound)
			return
		}

		if dbResp.StatusCode != http.StatusOK {
			respBody, _ := io.ReadAll(dbResp.Body)
			log.Printf("Unexpected status from DB for key '%s': %d, Body: %s", key, dbResp.StatusCode, string(respBody))
			http.Error(rw, "Internal server error: Unexpected DB response", http.StatusInternalServerError)
			return
		}

		var dbGetResponse DbGetResponse
		if err := json.NewDecoder(dbResp.Body).Decode(&dbGetResponse); err != nil {
			log.Printf("Failed to decode DB response for key '%s': %v", key, err)
			http.Error(rw, "Internal server error: Invalid DB response format", http.StatusInternalServerError)
			return
		}

		rw.Header().Set("content-type", "application/json")
		rw.Header().Set("lb-from", serverName)
		rw.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(rw).Encode(SomeDataResponse{
			Key:   dbGetResponse.Key,
			Value: dbGetResponse.Value,
		})
	})

	h.Handle("/report", report)

	server := httptools.CreateServer(*port, h)
	server.Start()
	signal.WaitForTerminationSignal()
}
