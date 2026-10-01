package capacityquery

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/heainframework/heain-job/internal/jobapp"
	"github.com/heainframework/heain-sdk/capacity"
)

func TestHTTPHeadroomSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Errorf("Authorization header = %q, want %q", got, "Bearer test-token")
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(capacity.Capacity{
			HasGPU:              true,
			GPUTotalMemoryBytes: 1000,
			GPUFreeMemoryBytes:  750,
		})
	}))
	defer srv.Close()

	headroom := HTTP(nil)
	got := headroom(jobapp.ModuleEndpoint{BaseURL: srv.URL, Token: "test-token"})
	if got != 75 {
		t.Errorf("headroom = %v, want 75", got)
	}
}

func TestHTTPHeadroomUnreachableScoresZero(t *testing.T) {
	headroom := HTTP(nil)
	got := headroom(jobapp.ModuleEndpoint{BaseURL: "http://127.0.0.1:1", Token: "x"})
	if got != 0 {
		t.Errorf("headroom = %v, want 0 for an unreachable replica", got)
	}
}

func TestHTTPHeadroomNon200ScoresZero(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	defer srv.Close()

	headroom := HTTP(nil)
	got := headroom(jobapp.ModuleEndpoint{BaseURL: srv.URL, Token: "x"})
	if got != 0 {
		t.Errorf("headroom = %v, want 0 for a non-200 response", got)
	}
}
