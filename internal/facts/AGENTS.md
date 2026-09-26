# Fact Journal DOX

## Purpose

- Own the append-only durable workflow evidence store below `.spynel/runtime/facts`.

## Local Contracts

- Facts are machine evidence, never workflow status authority: Markdown owns status, front matter, and Progress; leases own current launch/liveness ownership; the journal records what Spynel itself did.
- One JSONL journal per document under `task/<doc-key>.jsonl` and `goal/<doc-key>.jsonl`, plus `.lock`, `seq`, and `activation.json`. Directories are 0700 and files 0600.
- The `seq` file stores the LAST allocated sequence number. Allocation runs under the in-process mutex plus the cross-process `.lock` flock: the counter is durably persisted before the journal line is appended, so a crash between the two leaves an allowed gap; duplicates are impossible. A missing seq file with no facts means zero; with existing facts the last value rebuilds from the maximum sequence present (never max+1), so the next append becomes max+1.
- A torn final line (no trailing newline) is crash residue: appends truncate it back to the last complete line and lock-free reads ignore it. A complete line that is invalid JSON or carries an unsupported schema version fails closed with `ErrJournalCorrupt`, attributing the exact path and line number, and fails only that document's evidence gates without stopping unrelated documents.
- Deterministic idempotency keys deduplicate appends: an existing key returns the existing fact without allocating a sequence.
- The create-once `activation.json` marker records `v`, `activated_at`, and `first_seq`; it is never rewritten, and no synthetic facts are fabricated for pre-activation history.
- The full document identity is stored inside every fact; the filesystem path is never authority. `DocKey` uses the document ID when it matches `[A-Za-z0-9._-]{1,100}`, otherwise `h-` plus its SHA-256 prefix.
- `error_detail` is bounded to 512 bytes of valid UTF-8; `error_class` is one bounded line. The fact kind taxonomy is fixed and rejects invented lifecycle symmetry events.

## Child DOX Index

No child DOX files.
