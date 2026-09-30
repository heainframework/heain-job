// Command heain-job is the entrypoint for the heain-job orchestration
// service. It never links against heain-core — see the package docs on
// the heain-sdk coreclient package for how it talks to a running heain-core node.
//
// It runs two things side by side:
//   - a coreclient connection to a heain-core node, for the actual P1-P4
//     job lifecycle (ingest/dispatch/execute/staging);
//   - its own local HTTP server, for sibling Layer 3 modules to register
//     their SplitStrategy/MergeStrategy callback endpoints (see
//     internal/httpserver and internal/jobapp.ModuleEndpoint's docs).
package main

import (
	"flag"
	"log"
	"net/http"
	"os"

	"github.com/heainframework/heain-sdk/coreclient"

	"github.com/heainframework/heain-job/internal/audit"
	"github.com/heainframework/heain-job/internal/httpserver"
	"github.com/heainframework/heain-job/internal/orchestrator"
	"github.com/heainframework/heain-job/internal/registry"
)

func main() {
	coreAddr := flag.String("core-addr", "", "base URL of the heain-core node to talk to, e.g. https://127.0.0.1:8443")
	clientCert := flag.String("client-cert", "", "path to this node's mTLS client certificate")
	clientKey := flag.String("client-key", "", "path to this node's mTLS client key")
	caFile := flag.String("ca-file", "", "path to the heain-core deployment's CA certificate")
	serverName := flag.String("server-name", "", "CN/SAN of the target heain-core node's certificate")
	listenAddr := flag.String("addr", ":9400", "address heain-job's own registration/orchestration HTTP server listens on")
	flag.Parse()

	if *coreAddr == "" {
		log.Fatal("heain-job: -core-addr is required (the heain-core node's HTTP/mTLS address)")
	}

	core, err := coreclient.New(*coreAddr, coreclient.TLSConfig{
		ClientCertFile: *clientCert,
		ClientKeyFile:  *clientKey,
		CAFile:         *caFile,
		ServerName:     *serverName,
	})
	if err != nil {
		log.Fatalf("heain-job: building coreclient: %v", err)
	}
	_ = core // wired into the orchestration loop once job submission/dispatch is implemented

	reg := registry.New()
	auditLog := audit.NewLogger(os.Stdout) // v1: stdout only; see internal/audit's docs for the durable-backend plan
	orch := orchestrator.New(reg, auditLog)
	_ = orch // wired into job submission endpoints once they exist

	srv := httpserver.New(reg)

	log.Printf("heain-job: connected client configured for heain-core at %s", *coreAddr)
	log.Printf("heain-job: registration/orchestration HTTP server listening on %s", *listenAddr)
	if err := http.ListenAndServe(*listenAddr, srv); err != nil {
		log.Fatal(err)
	}
}
