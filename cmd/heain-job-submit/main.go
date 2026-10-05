// Command heain-job-submit submits one orchestration job to heain-job
// through core and prints the result (a client for tests and demos). It
// is an app itself (heain-job-client) and is configured through the
// HEAIN_* variables like any other.
//
//	heain-job-submit -cap text.upper -file input.txt [-resume transactional]
//	-> JOB <ticket> state=<s> attempts=<n> out=<output>
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/heainframework/heain-sdk/heain"
)

func main() {
	capName := flag.String("cap", "text.upper", "module capability to orchestrate")
	file := flag.String("file", "", "input file")
	resume := flag.String("resume", "", "idempotent | transactional")
	maxParts := flag.Int("max-parts", 0, "upper bound on sub-units")
	flag.Parse()
	input, err := os.ReadFile(*file)
	if err != nil {
		fmt.Println("ERROR", err)
		os.Exit(2)
	}
	ctx := context.Background()
	app, err := heain.StartFromEnv(ctx)
	if err != nil {
		fmt.Println("REFUSED", err)
		os.Exit(2)
	}
	defer app.Close(context.Background())
	wctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	if err := app.WaitActive(wctx); err != nil {
		fmt.Println("ERROR wait", err)
		os.Exit(1)
	}
	cancel()
	body, _ := json.Marshal(map[string]any{"capability": *capName, "input_b64": input, "resume": *resume, "max_parts": *maxParts})
	t, err := app.Submit(ctx, heain.JobRequest{Capability: "job.orchestrate", Payload: body, Classification: heain.Classification{Delivery: "IMMEDIATE"}})
	if err != nil {
		fmt.Println("ERROR submit", err)
		os.Exit(1)
	}
	jctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	st, err := app.WaitJob(jctx, t.TicketID)
	if err != nil {
		fmt.Println("ERROR wait", err)
		os.Exit(1)
	}
	fmt.Printf("JOB %s state=%s attempts=%d out=%s\n", t.TicketID, st.State, st.Attempts, st.Output)
}
