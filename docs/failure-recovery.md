# Failure recovery guide

| Failure | Expected behavior | Evidence / operator action |
|---|---|---|
| One node stops | Heartbeat becomes SUSPECT then OFFLINE; reads retry copies | Replication status reports degraded until replacement exists |
| Node dies during upload | Any failed required PUT prevents manifest commit | Upload returns error; orphans wait for GC |
| Node dies during download | Next replica is tried within bounded attempts | CLI accepts only complete verified output |
| Replica corrupts | Reject bytes, invalidate replica, count/log corruption | Next maintenance scan queues verified replacement |
| All copies unreadable | Download/repair fail; no invented recovery | Restore a real replica or backup; metadata alone is insufficient |
| Coordinator restarts | Sessions fail safely, work resumes, nodes reconcile | Verify metadata, membership, and final checksums |
| Storage restarts | Stable identity checked; temp files removed | Registration/reconciliation precedes healthy placement |
| Network timeout | Per-request transfer deadline/cancellation stops work | Repair persists error and backs off |
| Quota/disk full | PUT refuses or fails before publication | No successful upload from partial acknowledgements |
| Metadata commit fails | Transaction rolls back file/version ownership | Chunk orphans remain collectible |
| Duplicate request | Committed key returns original immutable version | Failed/pending key returns conflict; use a new key |
| No replacement capacity | Under-replication remains reported | Add allowlisted capacity; do not reduce reported truth |
| Failed repair | Exponential backoff, then persistent FAILED state | Inspect last_error; exhausted retries need operator intervention |
| Node joins | Ring changes; verified copies added | Excess old copies deliberately retained |
| Offline node returns | JOINING inventory and recorded-chunk checksums | Do not assume bytes survived the outage |
| GC interrupted | Repeat sweep; deletion is idempotent | Shared live chunks remain marked and protected |

## Reproduce locally

Run `powershell -NoProfile -ExecutionPolicy Bypass -File scripts/smoke.ps1` from
the project root on Windows. This script owns isolated data/config/log paths under
`.cache/smoke-<unique>` and launches separate processes on ports 18080 and
19001–19004. It polls observable conditions with deadlines instead of sleeping for
assumed startup durations. It leaves diagnostics but stops its own processes even
after failure. It never removes unrelated directories or kills processes by name.

The deterministic Go integration tests use `httptest`, temporary directories and
controlled corruption. The process demo checks actual binaries and persisted disk
state. The Compose commands in the README exercise the same architecture when
a Docker daemon is available. A loopback demo does not demonstrate fault tolerance
across independent power supplies, disks, machines, or networks.

## Exhausted tasks

FAILED tasks are intentionally retained rather than spinning forever. Until a
dedicated retry endpoint is implemented, stop the coordinator and use an SQLite
administration tool to inspect `repair_tasks`. After correcting the cause, an
operator can explicitly reset the relevant failed row to PENDING with zero
attempts and next-attempt time. Back up metadata first and never edit a running
cluster blindly. There is no claim of automatic recovery from permanently lost
data or exhausted operator-required failures.
