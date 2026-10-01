# Shared Whisper prototype comparison

Implemented the agreed standalone prototype and comparison milestone. This is a
candidate for further evaluation; no production engine or acceptance threshold
has been selected. go-carbon, carbonserver and buckytools are unchanged, as agreed
at the comparison gate.

## Implementation

The separate `store` Go module uses Pebble v1.1.5 with Snappy-compressed SSTables,
shared memtables and synchronous WAL commits. A catalog maps metric names to IDs
and generations. Archive keys use circular slots; timestamps prevent stale-slot
reads. Fetch uses bounded range iterators, including ring wrap. It does not create
one storage file per metric. Per-metric serialization uses 256 fixed lock stripes.

The API provides create, metadata/list, classic-compatible update/fetch,
atomic generation replacement/deletion, snapshots, classic export, and classic or
compressed import. Compressed sidecars are normalized on a private copy; original
main/sidecar files remain untouched. The caller must quiesce import sources.
Raw compressed archives exclude cwhisper's derived live-buffer fetch tail.

This is behavioral compatibility with the pinned classic Go implementation,
including its mixed-age batch boundary quirks, rather than an unchanged file API
or a claim that every possible Whisper behavior has been proved. `Mix` compressed
files are rejected. The root module gains archive helpers and an explicit-time
internal OOO merge helper; its dependency set is unchanged.

## Fixture results

Measured on macOS 26.6.2, arm64, Go 1.27.1. The corpus consists of the seven local
`tmp/test3_*.wsp` fixtures: 363,360 nonempty physical archive slots, with retentions
1s/172800, 60s/40320 and 3600s/17520. Three classic fixtures are completely empty;
others include sparse or stale slots. Same-name `.cwsp` peers have different data,
so each engine was initialized from the same classic snapshots. The read clock
was frozen to 2020-05-17 15:15:51 UTC to avoid expiring the historical data.

| Engine | Allocated bytes | Files | Read p50, us | p95, us | p99, us |
|---|---:|---:|---:|---:|---:|
| Classic Whisper | 19,382,272 | 7 | 47.2 | 102.0 | 198.8 |
| cwhisper | 7,487,488 | 14 | 96.3 | 755.4 | 875.5 |
| Shared Pebble | 5,226,496 | 8 | 52.8 | 592.9 | 624.5 |

Shared storage uses **5.23 MB**, compared with **19.38 MB** classic and **7.49 MB**
cwhisper: approximately 73% and 30% less, respectively, for this sparse fixture
mix. Shared pre-flush/compaction allocation was **17.73 MB**; this is one sampled
footprint, not peak disk usage. The eight shared files include the database's
WAL, manifest and other metadata. cwhisper's fourteen files include lock files.
Migration input/export files are excluded from database size measurements.

Each read percentile summarizes 140 warm-cache queries: 20 full coarsest-retention
reads per metric. Correctness is checked separately at every retention, including
direct shared-store reads after reopen. The matching post-write query SHA-256 is
`5ce916db6484d2931b7aa34fdb2df313ce9b5905df8f41dff0958d03d35c7063`.

Corpus writes touch only the three blank metrics (eight points each, batches of
four); this deliberately avoids treating stale historical coarse aggregates as a
clean write oracle. Those writes fit cwhisper buffers and do not measure sidecar
merging. Shared writes sync the WAL, while file baselines do not add fsync, so the
recorded write timings are **not an equal-durability throughput comparison**.
Import includes conversion overhead. Reopen opens/closes seven file handles versus
one database. JSON timing values are nanoseconds; zero import/reopen percentiles
mean that only a total was measured.

A separate current-time OOO scenario writes 49 points across five 10-second
windows, then fills a hole in the oldest window with value 42. It verifies actual
points in `.ooo` before calling `MergeOutOfOrder`. After closing/reopening all
engines, complete fine and coarse query grids match classic: the hole is 42 and
the affected coarse sum is 51. This is a functional smoke test, not a sustained
OOO benchmark. Per-engine late-write and merge durations are in the raw report.

## Verification and evidence

- `go test -race ./...` and `go vet ./...` pass in this module.
- Eighteen deterministic randomized classic-oracle traces cover six aggregation
  methods, XFF 0/0.5/1, duplicate timestamps, random/backfill writes, ring wrap,
  future/expired points and fetch boundaries.
- Persistent tests cover abrupt exit without Close and WAL replay, concurrent
  updates/imports, atomic replacement, generation reclamation, malformed snapshots,
  read-only imports, incompatible sidecars and historical OOO normalization with
  unchanged source hashes. Additional independent checks covered compressed
  physical versus derived coarse values, XFF normalization and post-2038 points.
- Python Whisper 1.1.10 verifies all seven post-update classic exports: 84 queries,
  1,616,783 values checked (144,154 present), including every archive and short,
  sub-step and zero-length windows.
- The existing root suite passed in an isolated temporary working directory.
  After the explicit-time OOO helper change, affected OOO, compressed and
  compaction tests passed again. Original fixture hashes remained unchanged.

Saved evidence: [benchmark JSON](results/2026-10-01/report.json),
[Python checks](results/2026-10-01/python-export-check.json),
[fixture inventory](results/2026-10-01/corpus.json), and
[source hashes/environment](results/2026-10-01/source.json).
The source manifest identifies the measured source files against the pinned
base revision; the prototype is not a published release. Large generated data remains under
`/private/tmp/storebench-verified-20261001` and is reproducible.

## Reproduction

From `go-whisper/store`, use a new output path each time:

```sh
go test -race ./...
go vet ./...
mkdir /tmp/whisper-corpus
cp ../tmp/test3_*.wsp /tmp/whisper-corpus/
go run ./cmd/storebench -corpus /tmp/whisper-corpus   -output /tmp/whisper-comparison -updates 8 -batch 4 -read-repetitions 20
```

For the independent Python check, install `whisper==1.1.10` in a disposable venv
and run `scripts/check_python_exports.py --now 1589728551 --pair SOURCE EXPORT`,
repeating `--pair` for each file from `classic/` and the matching
`pebble/exports-after/` directory. The saved fixture inventory is needed to
reproduce the exact measurements: newly generated test data can differ.

## Decision boundary

The result demonstrates shared compressed storage, classic compatibility on the
tested workloads, OOO durability through synchronous WAL writes, and readable
classic exports. It does not establish a production win. Classic reads had lower
tail latency here; sparse fixtures and three empty metrics limit size conclusions.

Before choosing Pebble or integrating consumers, agree performance/size gates and
measure realistic cardinality, sustained ingestion/backfill, concurrent reads,
CPU/RSS, write amplification, compaction backlog, cold reads and equal-durability
baselines. Add disk-full, failed-fsync and torn-WAL fault tests. Caller batches,
List results, snapshots and concurrent exports currently lack global memory
limits. Quotas, online schema migration and the local administration API are
not implemented in this milestone.

After that decision, go-carbon can own one store lifecycle, persister can commit
through the store API, and carbonserver can discover names through its catalog
and fetch through the same engine. Buckytools will need the planned local API for
snapshot/import/delete/rebalance operations instead of filesystem copying. These
consumer changes remain a separate, explicitly gated milestone.
