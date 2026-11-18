// Package api is the HTTP layer: parse, validate, call the store, map errors to status codes.
package api

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"insider-notification/internal/domain"
	"insider-notification/internal/observ"
	"insider-notification/internal/store"
)

//go:embed openapi.yaml
var openapiYAML []byte

type Server struct {
	store   *store.Store
	log     *slog.Logger
	metrics *observ.Metrics
}

func New(st *store.Store, log *slog.Logger, m *observ.Metrics) *Server {
	return &Server{store: st, log: log, metrics: m}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/notifications", s.create)
	mux.HandleFunc("POST /api/v1/notifications/batch", s.createBatch)
	mux.HandleFunc("GET /api/v1/notifications", s.list)
	mux.HandleFunc("GET /api/v1/notifications/{id}", s.get)
	mux.HandleFunc("POST /api/v1/notifications/{id}/cancel", s.cancel)
	mux.HandleFunc("GET /api/v1/batches/{id}", s.getBatch)
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /readyz", s.readyz)
	mux.Handle("GET /metrics", s.metrics.Handler())
	mux.HandleFunc("GET /openapi.yaml", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/yaml")
		w.Write(openapiYAML)
	})
	mux.HandleFunc("GET /docs", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		io.WriteString(w, swaggerPage)
	})
	return observ.Middleware(s.log, s.metrics, mux)
}

const swaggerPage = `<!doctype html><html><head><title>Notification API</title>
<link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css"></head>
<body><div id="ui"></div>
<script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
<script>SwaggerUIBundle({url: "/openapi.yaml", dom_id: "#ui"})</script></body></html>`

type errorBody struct {
	Error   string   `json:"error"`
	Details []string `json:"details,omitempty"`
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string, details ...string) {
	writeJSON(w, code, errorBody{Error: msg, Details: details})
}

func (s *Server) fail(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, domain.ErrNotFound):
		writeError(w, http.StatusNotFound, "not found")
	case errors.Is(err, domain.ErrNotCancellable):
		writeError(w, http.StatusConflict, err.Error())
	case errors.Is(err, domain.ErrIdempotencyMismatch):
		writeError(w, http.StatusUnprocessableEntity, err.Error())
	case errors.Is(err, store.ErrBadCursor):
		writeError(w, http.StatusBadRequest, "invalid cursor")
	case errors.Is(err, context.Canceled):
		// client went away; nothing to send
	default:
		s.log.Error("request failed", "error", err, "correlation_id", observ.CorrelationID(r.Context()))
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}

const maxBody = 5 << 20 // a 1000-item batch with long emails fits comfortably

func decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body", err.Error())
		return false
	}
	if dec.More() {
		writeError(w, http.StatusBadRequest, "invalid JSON body", "unexpected data after the JSON value")
		return false
	}
	return true
}

func idempotencyKey(w http.ResponseWriter, r *http.Request) (string, bool) {
	k := r.Header.Get("Idempotency-Key")
	if len(k) > 200 {
		writeError(w, http.StatusBadRequest, "Idempotency-Key is longer than 200 characters")
		return "", false
	}
	return k, true
}

func (s *Server) create(w http.ResponseWriter, r *http.Request) {
	var in domain.NewNotification
	if !decode(w, r, &in) {
		return
	}
	if problems := in.Validate(); len(problems) > 0 {
		writeError(w, http.StatusUnprocessableEntity, "validation failed", problems...)
		return
	}
	key, ok := idempotencyKey(w, r)
	if !ok {
		return
	}
	n, replayed, err := s.store.Create(r.Context(), in, key, observ.CorrelationID(r.Context()))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if replayed {
		w.Header().Set("Idempotent-Replayed", "true")
	}
	w.Header().Set("Location", "/api/v1/notifications/"+n.ID)
	writeJSON(w, http.StatusAccepted, n)
}

type batchRequest struct {
	Notifications []domain.NewNotification `json:"notifications"`
}

func (s *Server) createBatch(w http.ResponseWriter, r *http.Request) {
	var in batchRequest
	if !decode(w, r, &in) {
		return
	}
	if len(in.Notifications) == 0 {
		writeError(w, http.StatusUnprocessableEntity, "validation failed", "notifications must not be empty")
		return
	}
	if len(in.Notifications) > domain.MaxBatchSize {
		writeError(w, http.StatusUnprocessableEntity, "validation failed",
			fmt.Sprintf("a batch holds at most %d notifications, got %d", domain.MaxBatchSize, len(in.Notifications)))
		return
	}
	var problems []string
	for i, n := range in.Notifications {
		for _, p := range n.Validate() {
			problems = append(problems, fmt.Sprintf("notifications[%d]: %s", i, p))
			if len(problems) >= 50 {
				break
			}
		}
		if len(problems) >= 50 {
			break
		}
	}
	if len(problems) > 0 {
		writeError(w, http.StatusUnprocessableEntity, "validation failed", problems...)
		return
	}
	key, ok := idempotencyKey(w, r)
	if !ok {
		return
	}
	res, replayed, err := s.store.CreateBatch(r.Context(), in.Notifications, key, observ.CorrelationID(r.Context()))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if replayed {
		w.Header().Set("Idempotent-Replayed", "true")
	}
	w.Header().Set("Location", "/api/v1/batches/"+res.BatchID)
	writeJSON(w, http.StatusAccepted, res)
}

func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	n, err := s.store.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, n)
}

func (s *Server) getBatch(w http.ResponseWriter, r *http.Request) {
	b, err := s.store.GetBatch(r.Context(), r.PathValue("id"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, b)
}

func (s *Server) cancel(w http.ResponseWriter, r *http.Request) {
	n, err := s.store.Cancel(r.Context(), r.PathValue("id"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, n)
}

func (s *Server) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := store.ListFilter{Limit: 50, Cursor: q.Get("cursor"), BatchID: q.Get("batchId")}
	var bad []string
	if v := q.Get("status"); v != "" {
		if st, err := domain.ParseStatus(v); err != nil {
			bad = append(bad, err.Error())
		} else {
			f.Status = string(st)
		}
	}
	if v := q.Get("channel"); v != "" {
		if ch, err := domain.ParseChannel(v); err != nil {
			bad = append(bad, err.Error())
		} else {
			f.Channel = string(ch)
		}
	}
	for name, dst := range map[string]**time.Time{"from": &f.From, "to": &f.To} {
		if v := q.Get(name); v != "" {
			t, err := time.Parse(time.RFC3339, v)
			if err != nil {
				bad = append(bad, name+" must be an RFC 3339 timestamp like 2026-11-18T09:00:00Z")
				continue
			}
			*dst = &t
		}
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 200 {
			bad = append(bad, "limit must be between 1 and 200")
		} else {
			f.Limit = n
		}
	}
	if len(bad) > 0 {
		writeError(w, http.StatusBadRequest, "invalid query", bad...)
		return
	}
	res, err := s.store.List(r.Context(), f)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) healthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// readyz also checks the database; healthz only says the process is up.
func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.store.Ping(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "database unreachable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}
