// Command heain-job is the job-orchestration base app. It is configured
// through the heain-sdk container contract (HEAIN_* variables, see
// heain.StartFromEnv) and runs however the operator likes: a plain
// process, a service unit, or a container.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/heainframework/heain-job/internal/orchestrate"
	"github.com/heainframework/heain-job/internal/plan"
	"github.com/heainframework/heain-sdk/heain"
)

func main() {
	target := flag.Float64("target-unit-seconds", 20, "planner: aim for sub-units of about this many seconds")
	perWorker := flag.Int("max-parts-per-worker", 2, "planner: at most this many sub-units per live worker")
	concurrency := flag.Int("concurrency", 2, "orchestration jobs run at once")
	unitTimeout := flag.Duration("unit-timeout", time.Hour, "how long an orchestration waits for all its sub-units")
	callTimeout := flag.Duration("call-timeout", 10*time.Minute, "bound on each call to a module's split and merge (they carry the whole input and all outputs)")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	app, err := heain.StartFromEnv(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "REFUSED: %v\n", err)
		os.Exit(2)
	}
	state := os.Getenv("HEAIN_STATE_DIR")
	if state == "" {
		state = "/state"
	}
	pl, err := plan.Open(state, plan.Params{TargetUnitSeconds: *target, MaxPartsPerWorker: *perWorker})
	if err != nil {
		log.Fatal(err)
	}
	o := &orchestrate.Orchestrator{App: app, Planner: pl, Self: "job.orchestrate", UnitTimeout: *unitTimeout, CallTimeout: *callTimeout, Logf: log.Printf}
	w := app.NewWorker()
	w.Concurrency = *concurrency
	if err := w.Handle("job.orchestrate", o.Run); err != nil {
		log.Fatal(err)
	}
	log.Printf("heain-job: registered (%s), waiting for admission", app.Status())
	if err := app.WaitActive(ctx); err != nil {
		log.Fatal(err)
	}
	log.Printf("heain-job: active, orchestrating")
	_ = w.Run(ctx)
	_ = app.Close(context.Background())
	log.Printf("heain-job: deregistered")
}
