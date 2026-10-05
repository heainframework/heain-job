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

## heain-job v2 — rebuilt on heain-sdk v1 (Step 4a, 2026-10-06)

**The text above describes the PoC version, kept as tag `legacy-v0`.** That version ran its own registry, shared-secret HTTP, its own retries and failover, and used heain-sdk packages (`coreclient`, `jobclient`) that no longer exist. heain-core 1.3 now does discovery, leases, retries, reassignment and placement itself, so heain-job keeps only the orchestration intelligence (design note "heain-job", Group A).

**Author decisions (2026-10-06):** rebuild in this repo; heain-job drives the fan-out; its AI is a learned split planner.

- **Capability `job.orchestrate`** (execution: job, formal, AI). Payload: `{"capability": C, "input_b64", "origin_zone", "max_parts", "resume"}`. heain-job:
  1. asks its **planner** how many sub-units to make — it learns seconds per byte for each C from finished sub-units (moving average, kept in `HEAIN_STATE_DIR/planner-stats.json`, numbers only) and aims at a target sub-unit length, capped by live workers; each decision is a **signed reasoning record** (factors, model hash);
  2. calls the module's **`C.split`** (direct, mTLS);
  3. submits each sub-unit as a job on **`C.unit`** through core (P1–P4: encrypted, placed, leased, retried and reassigned by core), with the job's origin zone (P7);
  4. waits, then calls the module's **`C.merge`** and returns the result.
- **Module contract** (any module that wants orchestration): `C.split` `POST /v1/split/C` `{"parts","input_b64"} -> {"units":[{"id","payload_b64"}],"state_b64"}`; `C.unit` a job capability; `C.merge` `POST /v1/merge/C` `{"units":[{"id","output_b64"}],"state_b64"} -> {"output_b64"}`. heain-job declares them with the `uses[]` pattern `{app: "*", capabilities: ["*.split", "*.unit", "*.merge"]}`.
- **`examples/textmod`** (heain-textmod) is a minimal module offering the contract for `text.upper`; **`cmd/heain-job-submit`** submits a job and prints the result.
- **Runs any way you like** (plain process, service, container): configured through the heain-sdk `HEAIN_*` variables (`heain.StartFromEnv`).
- **Not yet:** transactional (exactly-once) jobs are refused — core retries sub-units and has no per-job retry control yet; an orchestration must finish within its lease (`dispatch.lease_default`/`lease_max`) — core has no lease extension yet; opaque checkpoints and Swarm (P7) sharing of what the planner learned come later.

Tests: `go test ./...`; live `bash scripts/live_4a.sh` (needs `~/heain-core`, `~/heain-sdk`); conformance `heain-conformance run --app .` (with heain-textmod as companion).

**Update (Step 4a-2, 2026-10-06): both "Not yet" items above are closed** by two heain-core controls:
- **Transactional jobs are fanned out:** with `"resume": "transactional"` every sub-unit is submitted with `max_attempts` 1, so core runs it at most once; a failed or expired sub-unit stops (`retries_exhausted`, the Approver decides) and the orchestration fails without merging. Any other `resume` value than `idempotent`/`transactional` is refused.
- **Long orchestrations keep their lease:** the heain-sdk Worker extends the lease of `job.orchestrate` (and the module's `C.unit` jobs) while they run, up to `dispatch.lease_max` per extension.

Live 4a now has 19 checks: a transactional job under a 3 s lease is fanned out, delivered, its sub-units carry `max_attempts` 1 in core's audit, and both the orchestration and the sub-units extended their leases.
