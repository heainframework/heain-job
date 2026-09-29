# heain-job

`heain-job` is the **Layer 3 Application Profile** module in the [Hybrid Edge AI
Node (HEAIN)](https://github.com/JakkritB/heain-core) architecture that owns
job-orchestration intelligence: load evaluation, split/distribute, checkpoint,
reassign-on-interruption, and merge, on behalf of any other Layer 3 module's
underlying job (`heain-image`, `heain-video`, `heain-sound`, `heain-sd`, …).

## Relationship to `heain-core` — important

`heain-job` is a **standalone repository with zero Go-level dependency on
`heain-core`**. It never imports `heain-core`'s packages and never needs
access to its source. `heain-core` (the private Layer 2 HEAIN Core Protocol
implementation) stays private, unchanged, and fully copyrighted by its
author.

The two talk **only over `heain-core`'s existing HTTP/mTLS API** — the same
P1-P4 endpoints (`/ingest`, `/dispatch`, `/execute`, `/staging/...`) any other
client of the protocol would use — exactly the same pattern P2's own dispatch
client (`HTTPCandidateCapacityFetcher`) already uses to call between nodes
inside `heain-core` itself. `heain-job`'s `internal/coreclient` package is
that HTTP/mTLS client.

This split means:

- Anyone can read, fork, and contribute to `heain-job`'s code without ever
  needing access to `heain-core`'s private repository.
- `heain-core`'s source code can never leak through this project, because
  `heain-job` never contains a copy of it or a build-time dependency on it.
- To actually run `heain-job` against a live `heain-core` node, you need an
  mTLS client certificate issued by that deployment's own CA — a runtime
  trust relationship, entirely separate from source-code access.

## Status

Early scaffold. See `internal/jobapp` for the Application Profile schema
(split-strategy / checkpoint / merge-strategy / resume-semantics) and
`internal/coreclient` for the HTTP/mTLS client against `heain-core`'s P1-P4
API.

## License

Apache License 2.0 — see [LICENSE](./LICENSE). Copyright retained by the
author; contributions are welcome under the same license.
