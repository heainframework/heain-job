// Command heain-job is the entrypoint for the heain-job orchestration
// service. It never links against heain-core — see the package docs on
// internal/coreclient for how it talks to a running heain-core node.
package main

import (
	"flag"
	"log"

	"github.com/JakkritB/heain-job/internal/coreclient"
)

func main() {
	coreAddr := flag.String("core-addr", "", "base URL of the heain-core node to talk to, e.g. https://127.0.0.1:8443")
	clientCert := flag.String("client-cert", "", "path to this node's mTLS client certificate")
	clientKey := flag.String("client-key", "", "path to this node's mTLS client key")
	caFile := flag.String("ca-file", "", "path to the heain-core deployment's CA certificate")
	serverName := flag.String("server-name", "", "CN/SAN of the target heain-core node's certificate")
	flag.Parse()

	if *coreAddr == "" {
		log.Fatal("heain-job: -core-addr is required (the heain-core node's HTTP/mTLS address)")
	}

	_, err := coreclient.New(*coreAddr, coreclient.TLSConfig{
		ClientCertFile: *clientCert,
		ClientKeyFile:  *clientKey,
		CAFile:         *caFile,
		ServerName:     *serverName,
	})
	if err != nil {
		log.Fatalf("heain-job: building coreclient: %v", err)
	}

	log.Printf("heain-job: connected client configured for heain-core at %s (scaffold — orchestration loop not yet implemented)", *coreAddr)
}
