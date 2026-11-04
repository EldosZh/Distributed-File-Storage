# Storage format and crash recovery

## Node disk layout

```text
data/node-01/
  node-id                  # stable operator-specified identity
  chunks/
    ab/
      ab<62 remaining lowercase hexadecimal characters>
      .tmp-<random>        # private, never served as a chunk
```

The ID is exactly 64 lowercase hexadecimal characters representing SHA-256 of the
actual payload. Two identical chunks share a path. User filenames never contribute
to paths. Empty files have zero chunks and the SHA-256 of an empty byte stream.

The node rejects nonregular chunk paths and symlink roots/shards. The storage tree
must be owned and exclusively controlled by the service account; these checks do
not provide a sandbox against another local process racing filesystem mutations.

## Atomic PUT

1. Validate ID/auth and acquire a bounded operation slot.
2. Serialize quota accounting and mutation under the node mutex.
3. Limit incoming bytes to the lesser of available quota, OS free space and 16 MiB.
4. Stream to an unpredictable temporary file in the target shard and compute SHA-256.
5. Verify exact content identity; a mismatch returns 422 with no published chunk.
6. If an existing file is verified identical, reuse it and discard the temporary file.
7. Otherwise fsync and close the temporary file, then atomically rename.
8. On Linux fsync the shard directory before acknowledging. On Windows the file
   flush and atomic publication are used; directory power-loss durability is not
   claimed. Only then does PUT return 201.

Failed hash checks, quota failures, and interrupted streams remove temporary files.
Startup removes leftover `.tmp-*` files after verifying the private chunk tree.
A crash after rename but before metadata acknowledgement leaves a valid orphan;
retrying the same PUT is safe. This does not implement protection against arbitrary
disk-controller failures or corruption of acknowledged bytes.

## Coordinator schema

The schema is embedded in `internal/metadata/store.go`. `schema_migrations` records
numbered changes. Migration 1 introduces the tables; migration 2 pins upload-session
replays to their committed file version. SQLite uses WAL, FULL synchronization,
one SQL connection, and a five-second busy timeout.

| Table | Role |
|---|---|
| `files` | Stable IDs, display names, deleted/tombstone bit |
| `file_versions` | Immutable JSON typed manifests keyed by file/version |
| `chunks` | Content ID, size, first observed timestamp |
| `file_chunks` | File/version/sequence ownership references and chunk ID |
| `chunk_replicas` | Chunk/node identity and validity flag |
| `storage_nodes` | Persistent typed node records |
| `upload_sessions` | Lifecycle, idempotency key, file/version result and error |
| `repair_tasks` | Unique chunk/target work, state, attempts, next attempt and error |

Typed manifests contain ID, name, size, content type, UTC creation/update times,
version, whole-file checksum, chunk size, ordered chunk references, and status.
References contain SHA-256 ID/checksum, sequence, offset, size, and upload-time
replica IDs. Single-file metadata lookup refreshes replica IDs from the live
replica table; listing returns immutable manifest snapshots, so use file lookup
for current replica placement.

## Versions, ownership, and collection

The transaction allocating a version inserts the complete manifest, all ordered
references, and COMMITTED session state. A rollback cannot expose an incomplete
manifest. Identical content is shared across IDs and versions. No reference count
is approximated: GC queries committed nondeleted file ownership across all versions.

Tombstones hide the whole file and its versions immediately from subsequent lookups.
Chunk bytes are removed only by GC after no active owner references them, no pending
repair protects them, the grace period expires, and the exclusive lifecycle lease
is acquired. Inventory timestamps define the physical-chunk grace period. Historical
metadata rows are retained; database compaction is future work.

## Restart behavior

Unfinished sessions become FAILED. RUNNING repairs become PENDING. Node membership
loads as JOINING regardless of its prior health and is verified before reuse.
Storage identity mismatch stops node startup. Temporary files are never advertised.
Backups must include metadata and all node trees; copying only SQLite cannot
reconstruct payload bytes. Do not run two coordinators concurrently.
