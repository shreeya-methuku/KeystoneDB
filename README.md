# KeystoneDB

![CI](https://github.com/shreeya-methuku/KeystoneDB/actions/workflows/ci.yml/badge.svg)

KeystoneDB is an embedded **LSM-tree key-value storage engine** written from scratch in C++17. It offers a simple `put` / `get` / `del` / `scan` API backed by a write-ahead log, sorted string tables (SSTables) with sparse indexes and bloom filters, and background compaction. It is designed for single-process embedded use: link it as a static library and point it at a directory.

The design follows the same lineage as production LSM engines like LevelDB, RocksDB, and Couchbase's Magma — an in-memory memtable absorbs writes, periodically flushes to immutable on-disk SSTables, and a background compactor merges those files to reclaim space and bound read cost.

```
put k v   →  WAL (durable)  →  memtable (skiplist, in RAM)
                                     │  when full (4 MB)
                                     ▼
                              SSTable on disk  ──┐
                                                 │  ≥ N files
                                                 ▼
                                    background compaction (k-way merge)
```

## Highlights

| Capability | Detail |
|---|---|
| **Durability** | WAL with CRC32-checksummed records; `fsync` per-write or batched; `F_FULLFSYNC` on macOS for true power-loss durability |
| **Crash safety** | Torn-tail-tolerant WAL replay + an atomic MANIFEST commit; proven by an automated `kill -9` torture test (zero acknowledged writes lost) |
| **Memtable** | Hand-written skiplist with tombstones; flushed to an SSTable at 4 MB |
| **SSTables** | Immutable, sorted, block-based (~4 KB blocks) with a sparse index, a hand-built bloom filter, and a fixed 40-byte footer |
| **Reads** | memtable → SSTables newest-first; bloom filters skip files that can't contain the key; binary-searched sparse index reads a single block, served from an LRU block cache |
| **Range scans** | k-way merging iterator with newest-wins precedence and tombstone shadowing; index seek to the start key + block cache, so a scan is O(range + log n), not O(total data) |
| **Compaction** | Background-thread, size-triggered full compaction on immutable version snapshots; obsolete files GC'd by `shared_ptr` refcount; safe tombstone dropping via the MANIFEST |
| **Durable writes** | Leader/follower **WAL group commit** — concurrent writers batch into one `fsync`; scales ~9× from 1→16 writers |
| **Concurrency** | Multi-writer (group commit) / multi-reader + one background compactor; verified clean under ThreadSanitizer |
| **Tested** | 56 unit tests (incl. randomized oracles), a concurrency stress test, and a crash torture test |

**Non-goals:** transactions/MVCC, secondary indexes, column families, compression, and SQL are all out of scope.

## Quick start

### Build & test

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
cd build && ctest --output-on-failure
```

Requirements: a C++17 compiler and CMake ≥ 3.16. GoogleTest is fetched automatically. SQLite3 is only needed to build the benchmark (`bench`).

### CLI / REPL

```bash
./build/keystone /path/to/dbdir
> put user:42 Shreeya
OK
> get user:42
Shreeya
> del user:42
OK
> get user:42
(nil)
> scan user:00 user:99
(0 results)
> exit
```

### Library API

```cpp
#include "keystone/db.h"

keystone::Options opts;           // opts.sync, opts.flush_threshold_bytes,
                                  // opts.compaction_trigger
auto db = keystone::DB::open("/path/to/dbdir", opts);

db->put("key", "value");
std::optional<std::string> v = db->get("key");   // nullopt if absent/deleted
db->del("key");                                   // inserts a tombstone

db->scan("a", "m", [](std::string_view k, std::string_view v) {
    // called in sorted key order for every live key in [start, end]
});
```

## Architecture

### Write path

1. `put`/`del` appends a CRC32-checksummed record to the **WAL** and (in durable mode) fsyncs it. This is the durability commit — the write is acknowledged only after its bytes are on disk. Concurrent writers are batched by a **leader/follower group commit**: one thread writes and fsyncs the whole batch while the others wait, so N durable writes share a single fsync.
2. The mutation is applied to the in-memory **memtable** (a skiplist). A delete inserts a *tombstone* rather than removing the key.
3. When the memtable reaches the flush threshold (default 4 MB) it is written out as an immutable **SSTable**; the WAL is then cleared and a fresh memtable installed.

### Read path

`get` consults sources newest-first and stops at the first match:

1. **Memtable** — a hit returns the value; a tombstone returns "deleted."
2. **SSTables, newest to oldest** — for each, the **bloom filter** is checked first (a "definitely absent" answer skips the file with zero block reads); otherwise the **sparse index** is binary-searched to locate the one ~4 KB block that could contain the key, which is read and scanned.

`scan` builds a **k-way merging iterator** (a min-heap over the memtable and every SSTable) that yields keys in sorted order, applying newest-wins precedence and dropping tombstoned keys. The same merge engine powers compaction.

### Compaction

Flushing produces many small SSTables; left unchecked they slow reads and waste space on superseded values. When the live SSTable count reaches the trigger (default 4), a **background thread** merges them:

- It takes an **immutable snapshot** of the current file set and performs the entire k-way merge *without holding any lock*, so reads and writes proceed unblocked.
- On completion it commits under a short lock: append a MANIFEST snapshot (the atomic commit point), swap in a new immutable `Version`, and mark the input SSTables obsolete.
- Obsolete files are unlinked only when their reference count hits zero (`shared_ptr`), so a reader mid-scan on an old snapshot is never pulled out from under.

Because the MANIFEST commit is atomic, a full compaction can **drop tombstones** safely — the merged file becomes the oldest, so no older file exists to resurrect a deleted key.

### Concurrency model

The engine supports **many writer threads, many reader threads, and one background compactor**. Writers are serialized through a leader/follower group-commit queue (one leader fsyncs the batch); a single mutex protects the small, fast in-memory state (memtable, current version pointer, live-file list, file counter, MANIFEST appends). Disk reads of SSTables, range scans, and the compaction merge all run *outside* that lock via immutable, refcounted `Version` snapshots (a scan snapshots the in-range memtable entries under the lock, then merges lock-free). The whole model is verified race-free under ThreadSanitizer.

## Durability & crash safety

- **WAL ordering:** the WAL append + fsync happens *before* the memtable mutation and before the write is acknowledged, so any acknowledged write survives a crash.
- **Torn tails:** a crash can leave a half-written record; WAL and MANIFEST replay verify CRC32 per record and stop cleanly at the first bad/short record, treating it as end-of-log.
- **Flush/compaction ordering:** SSTable durable (fsync) → rename into place → directory fsync → MANIFEST snapshot fsync (commit) → clear WAL. A crash at any step leaves a recoverable database; orphaned `.sst` files not referenced by the MANIFEST are cleaned up on open.
- **macOS `F_FULLFSYNC`:** on APFS a plain `fsync` only pushes data to the drive's write cache, not the platters. KeystoneDB issues `fcntl(fd, F_FULLFSYNC)` on macOS for genuine power-loss durability (falling back to `fdatasync` on Linux).

### Torture test

`tools/torture.py` starts a writer, `kill -9`s it at a random moment, reopens the database, and verifies the recovered keys form an unbroken, correct prefix — then repeats, driving the writer across flush and compaction boundaries. Run:

```bash
cmake --build build --target torture
python3 tools/torture.py 100
```

## On-disk formats

See [`docs/FORMAT.md`](docs/FORMAT.md) for the exact byte layouts. In brief:

- **WAL record:** `[crc32][type][keylen][vallen][key][value]` — CRC covers everything after itself.
- **SSTable:** `[data blocks][index block][bloom block][footer]`; each data-block entry is `[tag][keylen][vallen][key][value]` (tag 1 = tombstone). The fixed 40-byte footer holds `index_offset`, `bloom_offset`, `entry_count`, a format version, and a magic number, read first by seeking to `end − 40`.
- **MANIFEST:** an append-only, CRC32-framed log; each record is a full snapshot of the live SSTable numbers in recency order.

## Project layout

```
include/keystone/   public + internal headers (db, memtable, wal, sstable,
                    bloom, merge_iterator, manifest, version, block_cache, crc32)
src/                implementations (+ coding.h: LE encode/decode + fsync helpers)
cmd/keystone/       CLI / REPL
tests/              GoogleTest suites (memtable, wal, sstable, scan, bloom,
                    compaction, manifest, concurrency, group_commit) — 56 tests
tools/              torture test (C++ writer/verifier + Python driver)
bench/              benchmark harness vs SQLite + plotting script
docs/FORMAT.md      on-disk format specification
.github/workflows/  CI (build + tests, ThreadSanitizer, torture)
```

## Testing

```bash
cd build && ctest --output-on-failure          # all unit tests

# ThreadSanitizer build (proves the concurrency model race-free)
cmake -B build_tsan -DKEYSTONE_TSAN=ON
cmake --build build_tsan
cd build_tsan && ctest -R "Concurrency|Scan|Compaction|Manifest"
```

Notable tests: a randomized oracle comparing the memtable and full DB against a `std::map` reference over thousands of ops; a bloom-filter false-positive measurement vs theory; a tombstone-dropped-not-resurrected check; and multi-threaded reader/writer and group-commit stress tests under TSan.

## Continuous integration

Every push runs [`.github/workflows/ci.yml`](.github/workflows/ci.yml), which has three jobs:

- **Build & unit tests** — configures with CMake (Release), builds, and runs the full `ctest` suite.
- **ThreadSanitizer** — rebuilds with `-DKEYSTONE_TSAN=ON` and runs the concurrency-sensitive suites (scan, compaction, concurrency, group commit) to prove the model race-free.
- **Crash torture** — builds the torture tool and runs 30 `kill -9` / recover cycles.

The core library, tests, CLI, and torture tool build without SQLite; SQLite is only required for the `bench` target (`-DKEYSTONE_BENCH=OFF` to skip it).

## Benchmarks

### Methodology

KeystoneDB is benchmarked against **SQLite in WAL mode** using a single table `(key BLOB PRIMARY KEY, value BLOB)` and prepared statements, so the comparison is a clean LSM-vs-B-tree contrast:

- **Durability:** *durable* mode uses per-write sync on both engines (`F_FULLFSYNC` for KeystoneDB, `PRAGMA synchronous=FULL` for SQLite); *batched* mode relaxes sync (`SyncMode::Batched` / `synchronous=NORMAL`).
- **Load phase:** read and scan workloads pre-load data with batched sync (setup), regardless of the `-sync` flag — only the measured operations use the selected sync mode. This avoids multi-hour load times at large `n` when durable sync is selected.
- **Keys:** fixed-width 15-byte strings (`key%012d`), lexicographically ordered. **Values:** 100 bytes of random data (identical across engines).
- **Scans:** each scan uses an index seek to the start key and reads through the block cache; 2,000 scans of 100 keys each.
- **Fresh DB per workload;** latency reported as real per-operation p50/p99, not averages.

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build

# Writes + reads + scan at scale (batched sync; load phase is always batched)
./build/bench -n 1000000 -workloads seq_write,rand_write,rand_read_hit,rand_read_miss,scan -sync batched

# Durable write scaling via group commit (small n — durable is ~hundreds of ops/s)
./build/bench -n 10000 -workloads conc_write -sync durable -writers 1
./build/bench -n 10000 -workloads conc_write -sync durable -writers 4
./build/bench -n 10000 -workloads conc_write -sync durable -writers 8
./build/bench -n 10000 -workloads conc_write -sync durable -writers 16

python3 bench/plot.py          # optional: bench/throughput.png, bench/latency.png
```

### Results

<!-- BENCH_RESULTS_START -->

**Batched sync** (no per-write fsync / `PRAGMA synchronous=NORMAL`, n = 1,000,000):

| Engine     | Workload       |     ops/s | p50 µs | p99 µs |
|------------|----------------|----------:|-------:|-------:|
| KeystoneDB | seq_write      |   116,248 |    5.6 |   10.2 |
| SQLite     | seq_write      |    52,098 |   11.5 |   31.0 |
| KeystoneDB | rand_write     |   108,110 |    6.1 |   10.6 |
| SQLite     | rand_write     |    19,352 |   13.7 |  192.9 |
| KeystoneDB | rand_read_hit  |    30,088 |   32.3 |   47.2 |
| SQLite     | rand_read_hit  |   118,422 |    7.0 |   25.9 |
| KeystoneDB | rand_read_miss |   873,017 |    1.0 |    3.9 |
| SQLite     | rand_read_miss |   421,124 |    2.3 |    2.7 |
| KeystoneDB | scan           |     6,298 |  158.8 |  189.2 |
| SQLite     | scan           |    51,545 |   19.1 |   25.3 |

**Durable group-commit scaling** (`F_FULLFSYNC` / `PRAGMA synchronous=FULL`, n = 10,000):

| Writers | KeystoneDB ops/s | SQLite ops/s | Speedup vs 1-writer |
|--------:|-----------------:|-------------:|--------------------:|
|       1 |              219 |        8,808 |               1.0×  |
|       4 |              538 |        9,804 |               2.5×  |
|       8 |            1,014 |        9,644 |               4.6×  |
|      16 |            1,969 |       10,056 |               9.0×  |

*Apple M3 Pro, macOS 15.5, 100-byte values. Batched table: n = 1,000,000, 2,000 scans of 100 keys each. Durable table: n = 10,000. Numbers are machine- and config-specific; the takeaway is the **shape** of the result — who wins which workload and why.*

<!-- BENCH_RESULTS_END -->

### Analysis: write amplification vs read amplification

**Batched writes — KeystoneDB wins 2–6×** (seq_write ~2.2×, rand_write ~5.6× at n = 1M). This is the core LSM advantage: a write is a sequential WAL append plus an in-memory skiplist insert, with no B-tree page splits and no random I/O. SQLite must maintain a B-tree on disk with in-place updates — and the gap widens on *random* writes, where a B-tree pays for page splits and non-sequential I/O.

**Durable writes — SQLite wins, but group commit closes the gap.** At `F_FULLFSYNC`-per-write with a single writer, KeystoneDB is bounded by fsync latency (~219 ops/s). With WAL **group commit** and 16 concurrent writers, throughput scales to ~1,969 ops/s (9× the single-writer baseline). The comparison is not fully symmetric: `F_FULLFSYNC` forces the drive's write cache to the physical medium, whereas SQLite's `synchronous=FULL` on macOS uses a plain `fsync` that does **not** — so KeystoneDB is buying a *strictly stronger* power-loss guarantee here.

**Point reads — mixed.** At large scale (n = 1M), SQLite wins read *hits* (~4× faster) because its B-tree can serve any key in O(log n) page reads, while KeystoneDB must search across many SSTables. Read *misses* remain KeystoneDB's standout at ~2× faster: a negative bloom-filter check rejects an SSTable without reading any block, so a miss is ~1 µs, while SQLite must still traverse its B-tree to confirm absence.

**Scans — SQLite wins (~8×).** A B-tree stores rows in sequential page order, so a range scan is a sequential read. KeystoneDB must k-way-merge across SSTables, but now uses an index seek to the start key and serves blocks from a 16 MB block cache, so scans are dramatically faster than the early pathological numbers (~6,300 scans/s vs the original ~395). SQLite still wins because its B-tree page layout is inherently sequential.

**The trade-off, in one line:** an LSM shifts work *away from write time* (cheap appends) and pays it back at *read and compaction time* (read amplification + merge cost) — which is exactly why KeystoneDB dominates writes and misses while SQLite wins point-read hits and scans.

## Known limitations & future work

- ~~**WAL group commit**~~ — **done.** Multiple concurrent writers batch into a single fsync; 16 writers reach ~9× single-writer throughput.
- ~~**Fast scans**~~ — **done.** Index seek to the start key + an LRU block cache took scans from O(total data) to O(range + log n) (~395 → ~6,300 scans/s), and the scan now merges lock-free off a memtable snapshot.
- **Leveled compaction** — the current full/size-tiered compaction has high write amplification; leveled compaction (à la LevelDB/RocksDB) would bound it, and improve point-read *hit* latency by reducing the number of overlapping SSTables searched.
- **Per-block SSTable checksums** — the WAL and MANIFEST are CRC32-checked, but SSTable data blocks are not, so a bit-flip in a block is currently undetected. Add a per-block CRC verified on read.
- **Sequence numbers + MVCC snapshots** — tag each write with a monotonic seqno for consistent read snapshots (and as the prerequisite for correct leveled compaction).
- **Block compression** (e.g. LZ4/Snappy) and **bounded recovery** — checkpoint the MANIFEST periodically so it doesn't grow unbounded.
