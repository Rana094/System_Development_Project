# Phase 4.2 — immutable dataset manifests and ownership contract

8 November 2026, Asia/Dhaka. **4.2 PASS for manifest/validation/ACL components.
STOP before 4.3.** Last accepted subphase was [4.1](15-artifact-registration.md),
followed by the user's 4.2 request. This does not implement the persistent registry,
unique version allocation, job bindings/migrations (4.3), or legacy enrollment
(4.4). No student-facing API or hybrid execution is activated. The central
controller remains a **single point of failure**.

## 1. Implemented behavior and files

| File | Change |
|---|---|
| `hybrid_storage/dataset_manifests.py` | Frozen typed entries/manifests/ACL snapshots; canonical strict codec, version links, server-issued IDs and explicit access checks. |
| `hybrid_storage/dataset_validation.py` | Read-only descriptor-based tree inventory/hash and ZIP metadata preflight; no extraction/registration. |
| `docs/storage/schemas/dataset-manifest-v1.schema.json` | New dataset manifest schema with local references only, file/directory shape and version/predecessor constraints. |
| `docs/storage/examples/dataset-manifest-v1.json` | Canonical synthetic v1 fixture for bytes `example dataset\n`, plus an empty directory. |
| `tests/test_dataset_manifests.py` | 26 codec/identity/ACL/limit/integration tests. |
| `tests/test_dataset_validation.py` | 13 tree/path/race/ZIP tests using disposable local fixtures. |
| `docs/storage/16-dataset-manifests.md`, `16-dataset-manifests-evidence.json` | Exact contracts, results/output, source fingerprints, rollback and remaining limits. |
| `hybrid_storage/README.md`, `tests/README.md`, `docs/README.md`, `docs/storage/implementation-progress.md` | Usage, navigation and progress. |

No pre-existing runtime Python file, legacy/draft schema or existing manifest
bytes changed. The two new modules are explicit trusted-service components with
no runtime call sites, DB imports, configuration reads or import-time filesystem
activity. Source inspection does not publish artifacts or enroll old jobs.

### Dataset identity and immutable versions

```python
entries = inventory_tree(authorized_protected_local_root)
manifest, acl = create_dataset(authenticated_owner, entries)
raw = manifest.canonical_bytes()
identity = manifest.sha256
parsed = parse_manifest(raw)
acl.require_read(authenticated_owner, parsed)
```

`create_dataset` allocates opaque `ds_<32 lowercase UUID hex>` IDs inside the
trusted service. IDs are globally intended identifiers, not credentials. Owner
strings follow the existing controller ID/email convention, up to 128 ASCII
characters, and retain exact case. Pass the authenticated account's stored owner
ID; never take it from an upload form as proof of identity.

Every manifest has exactly `schema_version=1`, `kind=dataset-manifest`,
`dataset_id`, `version`, `owner`, `previous_manifest_sha256`, and `entries`.
`version` starts at 1; its predecessor is null. Later versions have a valid SHA
of the previous canonical manifest. `next_version(previous, current_acl, actor,
entries)` permits only the owner, preserves dataset ID/owner and advances by one.
Content changes produce a new artifact digest and a new version/manifest digest;
old entries/bytes are retained. Even unchanged entries in an explicitly requested
next version have a different version identity. No mutable `latest` filename is
used as the manifest's identity.

Regular file entries contain `path`, `type=file`, `sha256` (64 lowercase hex),
and `size_bytes`. Directory entries contain only `path`, `type=directory`; empty
directories are retained. Each parent directory must be explicitly listed.
Sizes and versions are genuine signed-64-bit nonnegative/positive integers,
excluding bool/float coercion. Symlinks/devices/FIFOs are not valid entry types.
Executable metadata is deliberately absent from this dataset-specific family;
source execution metadata remains in the separate existing source-tree design.

Manifest/entry objects are frozen, with tuple snapshots of supplied iterables;
returned dictionaries are copies. Entries sort by exact logical path. Canonical
bytes use sorted JSON keys, compact separators, ASCII escaping, finite numbers
and one trailing newline. SHA-256 hashes those exact bytes. `parse_manifest`
accepts canonical bytes only: unknown fields/kinds/versions, duplicate keys at
any depth, noncanonical formatting/order, invalid UTF-8/numbers and oversized
payloads fail. It verifies shape/semantics, **not provenance, authorization or
existence of referenced objects**.

This family is separate from the historical draft `dataset-tree` schema and
legacy `dataset_version`/`storage_manifest_sha256`. Their hashes are not
interchangeable. Neither historical schema nor old job/checkpoint identity is
rewritten. Future job binding must explicitly select the new family and verify
all referenced objects before marking metadata usable.

### Ownership and ACL semantics

`DatasetACL(dataset_id, owner, readers=())` represents a snapshot of **current
controller-owned authority**, not a document accepted from students. Defaults
are private. The owner may read every version, create the next version and
replace readers. Explicit readers have read-only access across versions; they
cannot publish, change ACLs or transfer ownership. There are no wildcard/public/
group grants, implicit administrators or owner-transfer API. Reader IDs are
unique, exclude the owner, and follow the same exact principal syntax.

`require_read(actor, manifest)` and `require_write(actor, manifest)` require a
real typed manifest, matching dataset ID and immutable owner. Wrong principals,
cross-dataset/owner bindings and absent/non-manifest inputs are denied. Knowing
an artifact or manifest hash grants nothing. Two separately owned datasets can
physically reuse the same 4.1 object while their access checks remain separate.

`replace_readers(owner, readers)` creates a new current ACL snapshot without
editing old manifests or their hashes. Each new access must load the current
authoritative ACL; revocation then denies reads of old and new versions. An old
snapshot remains an old grant and must not be retained as authority. Revocation
cannot retract already delivered private copies or terminate running jobs by
itself; attempt/launch policy is later integration work.

The ACL helper checks identity and permissions only. A forged manifest with an
otherwise valid owner/ID must **not** be accepted merely because it parses or
passes an ACL identity check. Before materializing anything, the trusted service
must resolve the exact `(dataset_id, version, manifest_sha256)` binding from its
own registry and verify artifact enrollment/provenance. That persistent authority
is 4.3, not present here. Public constructors are not authentication capabilities.

### Path and source validation

Logical paths preserve exact POSIX spelling; reject absolute paths, dot/dotdot,
empty/repeated components, trailing slashes, backslash, colon, controls/DEL and
surrogates. Percent escapes remain literal; never URL-decode a validated name.
Reject duplicate file/directory paths, file-as-parent conflicts, and conflicting
size declarations for the same digest. Check Unicode NFC plus case folding for
every component prefix, including implicit ZIP parents: `Data/x` vs `data/y` and
canonically equivalent Unicode aliases cannot enter one dataset. No silent
renaming/normalization occurs. This is not a general Windows filename contract.

`inventory_tree(root)` uses the existing no-follow root traversal, anchored child
directory/file FDs and regular-file checks. File reads/hash buffers are at most
1 MiB; before/opened/after signatures detect inode, type, size and timestamp
changes. Directory signatures detect membership changes during inspection.
Links/special files and detected races fail. It reads only and preserves source
content; no copying, path creation, ZIP extraction or artifact registration.
The trusted caller supplies an authorized, protected local snapshot. This is
**not an atomic snapshot of a concurrently mutable tree**: a file can change
after its last check. Protect inputs through inspection and subsequent verified
registration, and recheck expected size/hash in the registrar.

`validate_zip_members(zip_file, max_uncompressed_bytes=operator_limit)` validates
central-directory names, duplicate/case/Unicode/parent conflicts, types and total
declared bytes. It rejects unsafe symlink/special/encrypted entries, inconsistent
Unix/DOS directory attributes, directory payloads and NUL-truncated names. It
does not open member data or extract anything, and returns paths only, never
synthetic content digests. ZIP files with implicit directories are valid here;
the eventual inventory must explicitly include actual directories.

Metadata preflight is **not** CRC/hash verification or a reusable safe-open/
extraction token. Bound uploads before constructing `ZipFile` (its central
directory may allocate metadata); later extraction must enforce actual streamed
decompressed-byte limits, path containment, exclusive no-follow creation and
content verification. The existing legacy unpacker/upload flow is unchanged.

Limits: 10,000 entries, 1,024 UTF-8 bytes and 64 components per path, 4 MiB
canonical/decoded manifest, 256 explicit readers. Decoder checks the raw byte
limit before JSON parsing; constructor serialization is additionally bounded by
entry/path limits before rejecting an oversized document. These are metadata
bounds, not a file-content quota or latency guarantee. Inventory is synchronous
local I/O; never put unbounded source reads on scheduler/lease-critical threads.

## 2. API, DB, configuration and security changes

New Python APIs are described above. No HTTP/CLI route, DB table/migration, worker
protocol, scheduler, job/lease/checkpoint state, runtime flag or installed service
changed. The live controller was not imported or initialized; SQLite stays local,
and its main-file hash matches the baseline. Active flags remain legacy/OFF/
resource. No NFS stage/cache/locality execution, live object publication, bulk
copy, legacy enrollment or new student authorization endpoint is enabled.

The schema uses local `$defs` only; runtime code is stdlib-only. Optional system
`jsonschema` was used solely for offline validation, not installed or added to
worker dependencies. Schema validation alone cannot enforce provenance, actual
bytes, path uniqueness/parents/case conflicts, canonical bytes or genuine Python
integer types (JSON Schema permits an integral float). The strict runtime codec
must run too. ACL/version metadata belongs in controller-local protected storage,
never the input NFS export or training workspace. NFS AUTH_SYS is not per-student
authorization; RO/root_squash restrictions and training isolation still apply.
Checkpoints retain the existing trusted publisher flow.

## 3. Tests and actual results

Run from the application root:

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_dataset_manifests.py test_dataset_validation.py test_artifact_registration.py test_storage_readers.py test_storage_selection.py test_storage_config.py test_job_submission.py test_job_storage.py test_student_portal.py test_queue_controls.py test_resource_allocation.py test_storage_inputs.py test_storage_health.py test_storage_backends.py test_nfs_client_setup.py test_nfs_server_setup.py
env PYTHONDONTWRITEBYTECODE=1 python3 tools/run_tests.py --isolated test_storage_manifest_drafts.py
```

Main selection: **275 passed, zero failures/skips, 16/16 files** (39 new plus
236 prior). New tests separately: **39 passed, 2/2 files**. Before-code baseline:
**236 passed, zero failures/skips, 14/14 files**, with no observed pre-existing
failures. System-Python historical draft compatibility: **15 passed, no skips**.
New-schema validation: **28 checks passed**, including valid v1/v2, rejected
shape/path/type cases and deliberate schema-accept/runtime-reject examples.

Review added a regression for a missing manifest argument. Before its fix,
`require_read/require_write(..., None)` incorrectly treated access as an ACL-only
check (two failing subtests); explicit public type checks now deny it. This was
a new implementation issue, not a pre-existing failure. The before-fix output,
final outputs, schema-check program/probes and all exact commands are retained
in [evidence](16-dataset-manifests-evidence.json).

Tests cover immutable snapshots/canonical round trips, version links, changed
registered bytes and physical dedup without ACL sharing, owner/reader/revocation
rules, forged owner/dataset identity, parser bounds/types, traversal/Unicode/case
conflicts, symlink/special-file/root rejection, file/directory mutation, open-time
symlink swaps, missing/permission errors, ZIP slip and metadata budgets. Legacy
job/portal/queue/resource/health/backend regressions remain passing in disposable
controller copies. No live NFS/Pi, actual training/DDP, destructive disk/network
fault, production enrollment or benchmark ran.

## 4. Manual verification and expected outcomes

```bash
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python tools/run_tests.py --isolated test_dataset_manifests.py test_dataset_validation.py
env PYTHONDONTWRITEBYTECODE=1 .controller-venv/bin/python -m hybrid_storage.config
env PYTHONDONTWRITEBYTECODE=1 python3 /tmp/hybrid-storage-4-2/check_schema.py
```

Expected: 39 tests pass; config reports legacy, both new flags false, resource
policy; schema program reports 28 checks with zero failures. All were run. The
schema program text is preserved in evidence if the temporary directory is
removed. Parse the supplied canonical fixture with `parse_manifest` for a safe
read-only example; it is synthetic and creates no real dataset authority.

## 5. Preservation, rollback and remaining risks

Git root is parent `System_Development_Project`, branch `main`, HEAD
`ddc9a7b4a0a61288462c1999fd8c8ef1a2d01977`. Initial dirty work is preserved; no
stash/reset/clean/commit. Hashes/copies of 128 baseline files and logs are under
`/tmp/hybrid-storage-4-2/`; initial status is
`/tmp/hybrid-storage-4-2-status-before.txt`. Existing runtime sources, DB bytes,
config, legacy manifests and historical draft schemas are unchanged. Progress
was appended. No `/etc`, mounts, firewall, SSH, services or remote nodes changed.

Scoped diff: `/tmp/hybrid-storage-4-2.patch`. From the parent Git root, review
and check before reversing only this subphase:

```bash
git apply --reverse --check /tmp/hybrid-storage-4-2.patch
git apply --reverse /tmp/hybrid-storage-4-2.patch
```

Reverse check passed at completion. Preserve later edits and evidence; never
reverse the whole dirty diff or reset to HEAD. No DB/config/host rollback is
needed. Never delete datasets, artifacts, job snapshots or checkpoints as part
of removing code or reverting this documentation.

Residuals: no durable/current ACL registry, manifest persistence/unique
`(dataset_id, version)` constraint, head compare-and-swap, object provenance/
availability transaction, job input binding or enrollment endpoint exists yet.
Two concurrent `next_version` calls can produce competing candidate version
numbers; only 4.3's authoritative transaction may commit one. Untrusted manifest
or stale ACL acceptance would defeat the intended boundary, so runtime remains
gated. Inspection/preflight are not atomic snapshots or extraction. The central
controller remains a SPOF. All-three-Pi future validation remains required unless
explicitly excepted; pi01 remains deferred under the prior pi02/pi03 exception,
and this local work claims no cluster readiness.

## 6. Acceptance decision

| 4.2 criterion | Result |
|---|---|
| Versioned immutable dataset manifest with relative paths, digest, byte size and types | PASS (typed/canonical component) |
| Traversal, unsafe symlinks, case conflicts and ZIP slip rejected | PASS (read-only inspection/preflight; no new extractor) |
| Stable dataset ID, increasing linked versions and explicit ownership/read ACL | PASS (trusted component contract and checks) |
| Legacy compatibility, safe flags and no unintended changes | PASS in selected isolated regressions |

**4.2 PASS within that scope. STOP before 4.3.** Persistent authority, migrations
and production access enforcement remain mandatory before enablement; Phase 4
overall is not complete. No next subphase or deployment implemented.
