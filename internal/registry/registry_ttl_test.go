package registry

import (
	"testing"
	"time"

	"github.com/heainframework/heain-job/internal/jobapp"
)

func TestLookupFailsWhenStale(t *testing.T) {
	r := NewWithTTL(30 * time.Millisecond)
	ep := jobapp.ModuleEndpoint{ModuleName: "m", StrategyName: "m", BaseURL: "http://m", Token: "t"}
	if err := r.Register(ep); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if _, err := r.Lookup("m"); err != nil {
		t.Fatalf("Lookup (fresh): %v", err)
	}

	time.Sleep(40 * time.Millisecond)

	if _, err := r.Lookup("m"); err != ErrStale {
		t.Fatalf("Lookup (expired): got %v, want ErrStale", err)
	}
}

func TestReregisterResetsStaleness(t *testing.T) {
	r := NewWithTTL(40 * time.Millisecond)
	ep := jobapp.ModuleEndpoint{ModuleName: "m", StrategyName: "m", BaseURL: "http://m", Token: "t"}
	if err := r.Register(ep); err != nil {
		t.Fatalf("Register: %v", err)
	}

	time.Sleep(25 * time.Millisecond)
	if err := r.Register(ep); err != nil { // heartbeat refresh
		t.Fatalf("Register (refresh): %v", err)
	}
	time.Sleep(25 * time.Millisecond) // 25ms since refresh, well under the 40ms TTL

	if _, err := r.Lookup("m"); err != nil {
		t.Fatalf("Lookup: got %v, want nil (refresh should have reset staleness)", err)
	}
}
