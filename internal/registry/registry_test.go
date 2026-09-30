package registry

import (
	"testing"

	"github.com/heainframework/heain-job/internal/jobapp"
)

func TestRegisterAndLookup(t *testing.T) {
	r := New()
	ep := jobapp.ModuleEndpoint{
		ModuleName:   "heain-image",
		StrategyName: "heain-image",
		BaseURL:      "http://heain-image:9000",
		Token:        "secret",
	}

	if err := r.Register(ep); err != nil {
		t.Fatalf("Register: %v", err)
	}

	got, err := r.Lookup("heain-image")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if got != ep {
		t.Fatalf("Lookup returned %+v, want %+v", got, ep)
	}
}

func TestLookupNotRegistered(t *testing.T) {
	r := New()
	if _, err := r.Lookup("nope"); err != ErrNotRegistered {
		t.Fatalf("Lookup: got err %v, want ErrNotRegistered", err)
	}
}

func TestRegisterConflict(t *testing.T) {
	r := New()
	first := jobapp.ModuleEndpoint{ModuleName: "heain-image", StrategyName: "video", BaseURL: "http://a", Token: "t"}
	second := jobapp.ModuleEndpoint{ModuleName: "heain-video", StrategyName: "video", BaseURL: "http://b", Token: "t2"}

	if err := r.Register(first); err != nil {
		t.Fatalf("Register(first): %v", err)
	}
	if err := r.Register(second); err != ErrAlreadyRegistered {
		t.Fatalf("Register(second): got %v, want ErrAlreadyRegistered", err)
	}
}

func TestReregisterSameModuleIsRefresh(t *testing.T) {
	r := New()
	ep := jobapp.ModuleEndpoint{ModuleName: "heain-image", StrategyName: "video", BaseURL: "http://a", Token: "t"}
	epUpdated := ep
	epUpdated.BaseURL = "http://a-restarted"

	if err := r.Register(ep); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := r.Register(epUpdated); err != nil {
		t.Fatalf("Register (refresh): %v", err)
	}

	got, err := r.Lookup("video")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if got.BaseURL != "http://a-restarted" {
		t.Fatalf("Lookup returned stale BaseURL %q", got.BaseURL)
	}
}

func TestDeregister(t *testing.T) {
	r := New()
	ep := jobapp.ModuleEndpoint{ModuleName: "heain-image", StrategyName: "video", BaseURL: "http://a", Token: "t"}
	if err := r.Register(ep); err != nil {
		t.Fatalf("Register: %v", err)
	}
	r.Deregister("video")
	if _, err := r.Lookup("video"); err != ErrNotRegistered {
		t.Fatalf("Lookup after Deregister: got %v, want ErrNotRegistered", err)
	}
}

func TestAll(t *testing.T) {
	r := New()
	_ = r.Register(jobapp.ModuleEndpoint{ModuleName: "a", StrategyName: "a", BaseURL: "http://a", Token: "t"})
	_ = r.Register(jobapp.ModuleEndpoint{ModuleName: "b", StrategyName: "b", BaseURL: "http://b", Token: "t"})

	all := r.All()
	if len(all) != 2 {
		t.Fatalf("All() returned %d entries, want 2", len(all))
	}
}
