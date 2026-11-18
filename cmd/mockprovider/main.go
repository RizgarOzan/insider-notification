// mockprovider stands in for webhook.site: it answers 202 with a messageId, and can be told to fail
// a share of requests so retries are visible. Used by docker compose and for demos.
package main

import (
	"encoding/json"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

func main() {
	failRate, _ := strconv.ParseFloat(os.Getenv("FAIL_RATE"), 64) // 0.2 = 20% of requests get a 503
	var received atomic.Int64

	http.HandleFunc("POST /send", func(w http.ResponseWriter, r *http.Request) {
		received.Add(1)
		if rand.Float64() < failRate {
			http.Error(w, "simulated outage", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]string{
			"messageId": uuid.NewString(), "status": "accepted", "timestamp": time.Now().UTC().Format(time.RFC3339),
		})
	})
	http.HandleFunc("GET /stats", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]int64{"received": received.Load()})
	})

	addr := ":9090"
	log.Printf("mock provider on %s, fail rate %.2f", addr, failRate)
	log.Fatal((&http.Server{Addr: addr, ReadHeaderTimeout: 5 * time.Second}).ListenAndServe())
}
