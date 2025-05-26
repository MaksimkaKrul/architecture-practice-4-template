package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"strconv"

	"github.com/roman-mazur/architecture-practice-4-template/datastore"
	"github.com/roman-mazur/architecture-practice-4-template/httptools"
	"github.com/roman-mazur/architecture-practice-4-template/signal"
)

type GetResponse struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type PutRequest struct {
	Value json.RawMessage `json:"value"`
}

var (
	port           = flag.Int("port", 8080, "db server port")
	dbDir          = flag.String("db-dir", "/data/db", "directory for database files")
	maxSegmentSize = flag.Int64("max-segment-size", 10*1024*1024, "maximum segment size in bytes")
)

func main() {
	flag.Parse()

	log.Printf("Starting DB server on port %d, DB directory %s, max segment size %d bytes", *port, *dbDir, *maxSegmentSize)

	db, err := datastore.Open(*dbDir, *maxSegmentSize)
	if err != nil {
		log.Fatalf("Failed to open datastore: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("Error closing datastore: %v", err)
		}
	}()

	h := new(http.ServeMux)

	h.HandleFunc("/health", func(rw http.ResponseWriter, r *http.Request) {
		rw.WriteHeader(http.StatusOK)
		_, _ = rw.Write([]byte("OK"))
	})

	h.HandleFunc("/db/", func(rw http.ResponseWriter, r *http.Request) {
		key := r.URL.Path[len("/db/"):]
		if key == "" {
			http.Error(rw, "Key is required for /db/<key>", http.StatusBadRequest)
			return
		}

		switch r.Method {
		case http.MethodGet:
			valueType := r.URL.Query().Get("type")

			value, err := db.Get(key)
			if err != nil {
				if err == datastore.ErrNotFound {
					log.Printf("GET: Key '%s' not found, returning 404", key)
					rw.WriteHeader(http.StatusNotFound)
				} else {
					log.Printf("GET: Error getting key '%s' from DB: %v", key, err)
					http.Error(rw, "Internal server error", http.StatusInternalServerError)
				}
				return
			}

			if valueType == "int64" {
				_, convErr := strconv.ParseInt(value, 10, 64)
				if convErr != nil {
					log.Printf("GET: Value for key '%s' cannot be parsed as int64: %s (value: %s)", key, convErr, value)
					http.Error(rw, "Value cannot be parsed as int64", http.StatusBadRequest)
					return
				}
			}

			resp := GetResponse{Key: key, Value: value}
			rw.Header().Set("Content-Type", "application/json")
			rw.WriteHeader(http.StatusOK)
			json.NewEncoder(rw).Encode(resp)

		case http.MethodPost:
			var req PutRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				log.Printf("POST: Error decoding request body: %v", err)
				http.Error(rw, "Invalid request body", http.StatusBadRequest)
				return
			}

			valueToStore := string(req.Value)
			err = db.Put(key, valueToStore)
			if err != nil {
				log.Printf("POST: Error putting key '%s' into DB: %v", key, err)
				http.Error(rw, "Internal server error", http.StatusInternalServerError)
				return
			}

			log.Printf("POST: Successfully put key '%s' with value '%s'", key, valueToStore)
			rw.WriteHeader(http.StatusCreated)

		default:
			http.Error(rw, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	h.HandleFunc("/db", func(rw http.ResponseWriter, r *http.Request) {
		http.Error(rw, "Invalid path. Use /db/<key>", http.StatusBadRequest)
	})

	server := httptools.CreateServer(*port, h)
	server.Start()
	signal.WaitForTerminationSignal()
}
