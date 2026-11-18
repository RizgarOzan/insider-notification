package provider

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"insider-notification/internal/domain"
)

func TestSendClassifiesResponses(t *testing.T) {
	cases := []struct {
		name       string
		status     int
		retryAfter string
		body       string
		wantKind   domain.Kind
		wantErr    bool
		wantID     string
	}{
		{"accepted", 202, "", `{"messageId":"m-1","status":"accepted"}`, 0, false, "m-1"},
		{"2xx with junk body is still a success", 200, "", `not json`, 0, false, ""},
		{"bad request is permanent", 400, "", ``, domain.KindPermanent, true, ""},
		{"not found is permanent", 404, "", ``, domain.KindPermanent, true, ""},
		{"timeout status is transient", 408, "", ``, domain.KindTransient, true, ""},
		{"server error is transient", 503, "", ``, domain.KindTransient, true, ""},
		{"429 is rate limited", 429, "7", ``, domain.KindRateLimited, true, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if c.retryAfter != "" {
					w.Header().Set("Retry-After", c.retryAfter)
				}
				w.WriteHeader(c.status)
				w.Write([]byte(c.body))
			}))
			defer srv.Close()

			id, code, err := NewHTTP(srv.URL, time.Second).Send(context.Background(), domain.Notification{ID: "n-1", Channel: domain.SMS})
			if (err != nil) != c.wantErr {
				t.Fatalf("error = %v, want error %v", err, c.wantErr)
			}
			if code != c.status || id != c.wantID {
				t.Fatalf("status %d id %q, want %d %q", code, id, c.status, c.wantID)
			}
			if c.wantErr {
				if got := domain.KindOf(err); got != c.wantKind {
					t.Fatalf("kind = %v, want %v", got, c.wantKind)
				}
			}
		})
	}
}

func TestSendRateLimitCarriesRetryAfter(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "7")
		w.WriteHeader(429)
	}))
	defer srv.Close()
	_, _, err := NewHTTP(srv.URL, time.Second).Send(context.Background(), domain.Notification{})
	if o := domain.Decide(err, 1, 3, func() float64 { return 0 }); o.ChannelWait != 7*time.Second || !o.RefundTry {
		t.Fatalf("outcome %+v, want a 7s channel pause and a refunded attempt", o)
	}
}

func TestSendTimeoutAndUnreachableAreTransient(t *testing.T) {
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
	}))
	defer slow.Close()
	_, code, err := NewHTTP(slow.URL, 50*time.Millisecond).Send(context.Background(), domain.Notification{})
	if err == nil || code != 0 || domain.KindOf(err) != domain.KindTransient {
		t.Fatalf("timeout: code %d err %v kind %v", code, err, domain.KindOf(err))
	}

	gone := httptest.NewServer(http.NotFoundHandler())
	url := gone.URL
	gone.Close()
	if _, _, err := NewHTTP(url, time.Second).Send(context.Background(), domain.Notification{}); domain.KindOf(err) != domain.KindTransient {
		t.Fatalf("connection refused should be transient, got %v", err)
	}
}

func TestSendHeadersAndBody(t *testing.T) {
	var got struct {
		key, corr, ctype string
		body             string
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got.key, got.corr, got.ctype = r.Header.Get("Idempotency-Key"), r.Header.Get("X-Correlation-ID"), r.Header.Get("Content-Type")
		buf := make([]byte, 512)
		n, _ := r.Body.Read(buf)
		got.body = string(buf[:n])
		w.WriteHeader(202)
	}))
	defer srv.Close()
	n := domain.Notification{ID: "n-9", Channel: domain.Email, Recipient: "a@b.c", Content: "hi", CorrelationID: "corr-1"}
	if _, _, err := NewHTTP(srv.URL, time.Second).Send(context.Background(), n); err != nil {
		t.Fatal(err)
	}
	if got.key != "n-9" || got.corr != "corr-1" || got.ctype != "application/json" {
		t.Fatalf("headers: %+v", got)
	}
	if got.body != `{"to":"a@b.c","channel":"email","content":"hi"}` {
		t.Fatalf("body: %s", got.body)
	}
}

func TestParseRetryAfter(t *testing.T) {
	for in, want := range map[string]time.Duration{
		"":     0,
		"abc":  0,
		"-3":   0,
		"0":    0,
		"5":    5 * time.Second,
		"9999": 300 * time.Second, // capped
	} {
		if got := parseRetryAfter(in); got != want {
			t.Errorf("parseRetryAfter(%q) = %v, want %v", in, got, want)
		}
	}
}
