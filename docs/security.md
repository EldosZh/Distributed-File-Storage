# Security model

This is a trusted local-development cluster. It does not provide multi-tenant,
public object storage security.

## Network boundary

The coordinator binds to 127.0.0.1 by default. Its **public API has no user
authentication**, including upload and delete. Every process/user with network
access to it can operate on files. Keep it private. CORS is not enabled, but CORS
is not an authorization mechanism; untrusted websites and local processes must not
be treated as excluded merely by a lack of browser-readable responses. Do not
put this API on a public interface without implementing real authorization.

Docker binds public ports to host loopback and leaves node ports inside its bridge
network. The dashboard nginx only proxies `/api`. Container services run as an
unprivileged backend user. A localhost port is not an isolation boundary against
other users or processes on the same machine.

## Internal authentication and SSRF

Node registration, heartbeats, inventory, chunk writes/reads/deletes, drain and GC
require a constant-time-compared shared bearer token. Supply a private token with
`DFS_INTERNAL_TOKEN`; all processes must share it. Repository configuration tokens
are explicitly public demo values, not deployment secrets.

The coordinator accepts only exact configured `http://host:port` addresses, refuses
URL userinfo, query, fragments, and path components, and rejects conflicting IDs or
shared addresses. Internal HTTP clients do not use environment proxies and do not
follow redirects. This prevents public callers and accidental node reports from
turning the coordinator into an arbitrary URL fetcher. Allowed DNS, network
infrastructure, administrators, and token holders remain trusted. DNS rebinding,
malicious allowlisted members, or a stolen token are outside this v1 boundary.

There is no TLS or mTLS; tokens and data are plaintext on the network. Token
rotation requires coordinated restarts. The single shared token does not distinguish
administrator and storage-node capabilities. Do not grant it to untrusted clients.

## Filesystem and integrity

Chunk IDs must be exactly lowercase SHA-256 hex. Filesystem paths use only this
validated hash and its first-byte shard. Display filenames are length-checked and
reject path separators, CR/LF/NUL, `.` and `..`; names never determine storage paths.
Nonregular files and symlink roots/shards are refused. Temporary files have random
names, are not served, and are cleaned after restart.

Use a private service account with exclusive write access to the entire data tree.
Checks against symlinks are not an OS-level sandbox against another privileged
process replacing directories between checks. Windows junction/ACL semantics and
local hardlink attacks are not a claimed adversarial boundary. Protect metadata
directories as well as chunk directories. There is no at-rest encryption.

SHA-256 verifies actual chunk bytes on PUT and before forwarding GET data. It is a
data-integrity check, not authentication of an untrusted node: a credentialed member
can lie about persistence or refuse service. Stronger adversarial durability would
need separate identities, attestation/auditing, and a different trust model.

## Resource limits

Uploads have a configurable maximum size. Chunk PUT is capped at 16 MiB, and node
quota/OS free space are checked under the mutation lock. Coordinator operations,
node operations, repair workers, and pending repair jobs are bounded. Headers and
transfer times have limits. GC has grace and lifecycle protections.

This does not provide per-user quotas, request-rate authorization, complete catalog
pagination, or resistance to a malicious authenticated storage writer filling the
metadata database. Inventories/manifests grow with object count. Running the cluster
on an untrusted network would require additional controls, not configuration alone.

Logs contain operations, IDs, status and duration; never file contents or tokens.
Runtime data, `.env`, database journals, caches, dependency folders and generated
binaries are ignored by Git and Docker build contexts.
