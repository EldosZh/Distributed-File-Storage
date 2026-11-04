# Verification record

Recorded 2026-11-05 in Asia/Qyzylorda. Results below are actual local executions,
not a claim of production suitability or independent-machine fault tolerance.

## Environment and checks

- Windows amd64; Go 1.26.3; Node 24.12.0.
- Intel Core i5-1155G7 @ 2.50 GHz, as reported by Go's benchmark harness.
- `go mod tidy`: passed. Dependencies downloaded into a project-local module cache.
- `gofmt`: applied; formatting check returned no unformatted paths.
- `go vet ./...`: passed.
- `go test ./...`: passed, including temporary-directory and multi-node HTTP tests.
- All three binaries built: coordinator, storage-node, dfsctl. `go build ./...` passed.
- `npm --prefix web run build`: passed TypeScript and Vite production compilation.
- `docker compose config --quiet`: passed structural configuration validation.
- `go test -race ./...`: **not verified**. With default CGO disabled, Go refuses
  `-race`; explicitly enabling CGO fails because `gcc` is not installed. No supported
  C compiler was found. This is a remaining verification prerequisite, not a pass.
- Docker runtime demo: **not verified**. The installed Docker CLI cannot connect
  to a running daemon (`docker_engine` named pipe is absent). Images, nginx trailer
  forwarding and container startup were not runtime-tested here.
- Browser interaction/visual QA: **not performed**; this session has no browser
  control/preview tool. The dashboard build and live development HTTP proxy were tested.
- A GitHub Actions workflow was added to run race tests on Linux and build/run
  Compose when pushed. It has **not** run in this local session.

Shared Go cache paths were denied by the workspace sandbox, so builds used local
`.cache/go-build` and `.cache/modules`. These are ignored. No global configuration
was changed. No Git commits or pushes were made; Git resolves to a parent repository.

## Process-level demonstration

Executed `scripts/smoke.ps1 -SkipBuild -Dashboard` using freshly built binaries.
The final run returned PASS and stopped all processes it owned.

1. Started a real coordinator and three separate storage-node processes.
2. Waited for authenticated registration, reconciliation, and three HEALTHY nodes.
3. Started Vite, checked the dashboard returned HTTP 200, and verified its live
   `/api/v1/nodes` proxy returned the three real nodes.
4. Uploaded a small text file and a generated **32 MiB** file streamed from disk.
5. Verified **32 chunks**, **three distinct physical replicas per chunk**.
6. Downloaded both through dfsctl and independently compared filesystem SHA-256.
7. Stopped one node, waited for under-replication, and verified fallback download.
8. Started a fourth node, waited for actual repair and zero under-replicated chunks,
   then downloaded and compared checksums again.
9. Restarted the coordinator against the same SQLite database, waited for node
   reconciliation, and verified the same file and checksum.
10. Restarted a storage node against its existing directory, waited for a fresh
    heartbeat and HEALTHY state, and verified another checksum-preserving download.
11. Ran the CLI benchmark below with zero errors in all phases.

An earlier demo attempt filled the host C: drive. Node PUT returned HTTP 507 and
the incomplete upload was rejected. Session-created smoke payloads were removed,
freeing space, and the final full demo passed. No unrelated user files were removed.
The smoke script now cleans its own large payloads by default while retaining logs
and benchmark JSON; `-KeepData` preserves the complete test data intentionally.

## Automated failure/invariant coverage

The Go suite directly covers:

- Empty, small, exact-boundary, multi-chunk, and 64 MiB streamed chunking; SHA-256
  known vector and ID validation.
- Ring determinism across reordered input, 128 virtual nodes, distinct physical
  replicas, insufficient membership, node addition/removal and limited remapping.
- Atomic/idempotent chunk PUT, corrupt PUT rejection, quota exhaustion, absent
  partial chunks, temp cleanup on restart, persistent identity and authenticated APIs.
- Transaction rollback via an injected SQLite trigger, immutable versions, original
  idempotency-version replay, metadata and node persistence, interrupted session recovery.
- Three-replica uploads and byte-for-byte ordered reconstruction across chunks.
- Controlled corruption on each original replica, explicit checksum rejection,
  healthy fallback, and verified repair of the corrupted path.
- Loss of a real node HTTP server, explicit degraded status, addition of a fourth
  storage node, restored healthy replica count, coordinator restart and reconciliation.
- Midstream upload failure, pre-cancelled request, concurrent uploads/downloads,
  partial replica failure and insufficient healthy nodes/capacity.
- Shared content across file IDs and versions, tombstone deletion, actual GC and
  dry-run eligibility, referenced chunk protection, active lifecycle leases and grace.
- Heartbeat persistence, conservative drain safety, absent valid sources, and
  persistent repair retry timestamps/attempts with exponential backoff.

The symlink-root test was explicitly skipped here because Windows did not grant
symlink-creation privilege. It creates a link on capable hosts; this does not
assert protection against concurrent privileged
filesystem mutation. No distributed consensus, permanent total-copy loss recovery,
power-loss simulation, exhaustive commit-stage interruption, or real network
partition test was performed.

## Actual CLI benchmark

Latest successful process run: single Windows host, loopback HTTP, one coordinator,
four registered nodes (three healthy and one offline), replication factor **3**,
**1 MiB chunks**, **1 MiB generated zero-content test file**, **three requests per
phase**, concurrency **2** in concurrent phases. Percentiles use nearest rank.
These very small sample counts validate tooling; they are not credible production
tail-latency estimates. Deduplication and local filesystem cache strongly affect
the results. Upload measurements include strict replica writes; isolated replication
throughput was not measured.

| Phase | MiB/s | Requests/s | p50 ms | p95 ms | p99 ms | Errors |
|---|---:|---:|---:|---:|---:|---:|
| Upload | 6.34 | 6.34 | 163.81 | 186.69 | 186.69 | 0 |
| Concurrent upload | 10.83 | 10.83 | 180.27 | 212.04 | 212.04 | 0 |
| Download | 32.61 | 32.61 | 30.17 | 40.57 | 40.57 | 0 |
| Concurrent download | 77.67 | 77.67 | 22.09 | 24.78 | 24.78 | 0 |
| Metadata lookup | n/a | 501.52 | 1.79 | 2.52 | 2.52 | 0 |
| SHA-256 stream | 673.73 | 673.73 | 1.62 | 1.73 | 1.73 | 0 |

Raw local JSON is ignored under `.cache/smoke-4503289e01284a77a7a5ba61c4003503/benchmark.json`.
Each request is one MiB in the payload phases, hence matching MiB/s and requests/s.
The benchmark tombstones its created file IDs after measurement.

## Actual Go microbenchmarks

Command: `go test ./internal/chunk -run '^$' -bench . -benchmem -benchtime=1s`.

| Benchmark | ns/op | Throughput reported by Go | B/op | allocs/op |
|---|---:|---:|---:|---:|
| 4 MiB SHA-256 | 5,509,339 | 761.31 MB/s | 128 | 2 |
| 64 MiB stream / 4 MiB chunks | 241,305,229 | 278.11 MB/s | 4,195,960 | 6 |

Go's benchmark throughput uses decimal MB/s; the CLI uses binary MiB/s. The stream
benchmark allocated approximately one chunk, not the full 64 MiB input. No memory
claim about unlimited manifest/catalog size follows from this payload benchmark.

## Remaining release checks

Install/configure a supported C compiler and run `go test -race ./...`. Start a
Docker daemon and run the Compose demo, including nginx/trailer behavior. Perform
browser interaction and responsive visual QA. Review changes within a dedicated
project Git repository, excluding all ignored runtime artifacts. Because these
checks remain unverified, this record deliberately does not declare unconditional
readiness to publish or production readiness.
