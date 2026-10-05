// Command textmod is a minimal module offering heain-job's orchestration
// contract for text.upper: split by lines, upper-case each sub-unit,
// merge in order. With -fail-first N it fails the first N sub-units it
// runs (retryable), to show core reassigning them.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/heainframework/heain-sdk/heain"
)

func main() {
	failFirst := flag.Int64("fail-first", 0, "fail the first N sub-units (retryable)")
	delay := flag.Duration("delay", 0, "time each sub-unit takes (to show sub-units spreading over instances)")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	app, err := heain.StartFromEnv(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "REFUSED: %v\n", err)
		os.Exit(2)
	}
	srv := app.NewServer()
	type unit struct {
		ID      string `json:"id"`
		Payload []byte `json:"payload_b64,omitempty"`
		Output  []byte `json:"output_b64,omitempty"`
	}
	write := func(w http.ResponseWriter, v any) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(v)
	}
	must := func(err error) {
		if err != nil {
			log.Fatal(err)
		}
	}
	must(srv.HandleFunc("POST /v1/split/text.upper", func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			Parts int    `json:"parts"`
			Input []byte `json:"input_b64"`
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil || in.Parts < 1 {
			http.Error(w, "bad split request", http.StatusBadRequest)
			return
		}
		lines := strings.SplitAfter(string(in.Input), "\n")
		if in.Parts > len(lines) {
			in.Parts = len(lines)
		}
		per := (len(lines) + in.Parts - 1) / in.Parts
		var units []unit
		for i := 0; i < len(lines); i += per {
			end := i + per
			if end > len(lines) {
				end = len(lines)
			}
			units = append(units, unit{ID: fmt.Sprintf("u%03d", len(units)), Payload: []byte(strings.Join(lines[i:end], ""))})
		}
		write(w, map[string]any{"units": units})
	}))
	must(srv.HandleFunc("POST /v1/merge/text.upper", func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			Units []unit `json:"units"`
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			http.Error(w, "bad merge request", http.StatusBadRequest)
			return
		}
		var b strings.Builder
		for _, u := range in.Units {
			b.Write(u.Output)
		}
		write(w, map[string]any{"output_b64": []byte(b.String())})
	}))
	var failed atomic.Int64
	wk := app.NewWorker()
	must(wk.Handle("text.upper.unit", func(ctx context.Context, j *heain.Job) ([]byte, error) {
		if failed.Add(1) <= *failFirst {
			log.Printf("textmod %s: failing sub-unit %s on purpose", app.Instance, j.TicketID)
			return nil, fmt.Errorf("deliberate failure")
		}
		time.Sleep(*delay)
		log.Printf("textmod %s: UNIT %s %d bytes", app.Instance, j.TicketID, len(j.Payload))
		return []byte(strings.ToUpper(string(j.Payload))), nil
	}))
	go func() {
		if app.WaitActive(ctx) == nil {
			_ = wk.Run(ctx)
		}
	}()
	l, err := net.Listen("tcp", heain.Listen("127.0.0.1:19453"))
	must(err)
	log.Printf("textmod %s: serving on %s", app.Instance, l.Addr())
	if err := srv.Serve(ctx, l); err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
	_ = app.Close(context.Background())
}
