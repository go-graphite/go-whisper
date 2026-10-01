# Experimental shared Whisper store

`store` is a standalone Pebble-backed prototype. It stores many metrics in one
directory, keeps a catalog plus circular archive slots, and syncs every accepted
batch to Pebble's WAL before returning. It has no go-carbon dependency;
consumers inject the same store handle into persistence, reads and administration.

The package exposes classic-style `Update`, `UpdateMany`, `Fetch`, metadata,
paged catalog listing, deletion, snapshots, and archive-preserving classic `.wsp`
import and export. `Replace` publishes a new metric generation atomically.
`FillWSP` atomically fills missing archive slots while preserving destination
values and rejecting incompatible policies. Metadata revisions advance with
synced writes/replacements; `DeleteIfUnchanged` rejects stale transfer snapshots
and delete/recreate races. `Fetch` includes metadata from the same snapshot;
`ExportSnapshot` exports exactly the captured revision. Use `Flush`,
`Compact`, and `Stats` only for maintenance and benchmark measurement.

Run the focused suite with `go test ./...` from this directory. The benchmark
command under `cmd/storebench` accepts copied go-whisper fixtures; do not run it
against live metric files. The optional Python export checker is maintained in
the `scripts/` directory beside this module.

## Prototype limits

This is not a production backend. The compatibility suite pins the current
classic go-whisper update and fetch behavior, including `UpdateMany`'s historic
mixed-age boundary behavior. WAL recovery after an abrupt process exit is
tested; injected disk-full/fsync failure testing is not implemented.

Compressed imports with an `.ooo` sidecar copy the main file and sidecar into a
private temporary directory, merge that copy, then import its normalized archive
contents. The source must be quiesced for this copy to be consistent. `Mix`
compressed files are rejected.

The API is behavioral compatibility for classic Whisper, not a filesystem-level
replacement: consumers must use the catalog and store methods instead of walking
`.wsp` paths. Pebble v1.1.5 is pinned in this separate module; the root Whisper
module does not depend on Pebble; consumers importing `store` add that dependency. SSTables use Pebble's
Snappy compression and its shared WAL/memtables handle out-of-order updates.

Caller batches, whole-catalog lists, snapshots and concurrent exports are not
globally memory-bounded. `ListPage` bounds each catalog read. Consumers must bound
requests, enforce quotas and join callers before closing the store. The module
does not implement online schema migration or own a network administration API. Fault injection for disk-full, failed sync and torn WAL
records remains required before production use.

See [COMPARISON.md](COMPARISON.md) for the measured fixture comparison and exact
reproduction commands. These measurements are a decision input, not a production
capacity or engine-selection gate.
