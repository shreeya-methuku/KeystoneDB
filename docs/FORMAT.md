# On-Disk Format Specification

All multi-byte integers are little-endian.

## WAL Record

Each entry in the write-ahead log is a single variable-length record:

```
[crc32: u32][type: u8][seq: u64][keylen: u32][vallen: u32][key bytes][value bytes]
```

`type`: 1 = PUT, 2 = DELETE (vallen = 0 for deletes). `seq` is a monotonically increasing sequence number assigned at write time. The CRC32 covers everything after itself (type through value bytes).

## SSTable File

An SSTable is an immutable, sorted file of key-value pairs organized into blocks:

```
[data block 0][data block 1]...[index block][bloom block][footer]
```

### Data Block

On disk, each data block is:

```
[entries...][crc32: u32]
```

The trailing CRC32 (little-endian) covers all entry bytes preceding it. The
index entry's `length` for the block **includes** the 4-byte trailer. On read
the checksum is verified and stripped; callers see only the entry payload.
Added in format\_version 3.

### Data Block Entry

```
[tag: u8][seq: u64][keylen: u32][vallen: u32][key bytes][value bytes]
```

- `tag 0` — live value entry.
- `tag 1` — tombstone (vallen = 0, no value bytes).
- `seq` — write sequence number, used for version resolution when the same key
  appears in multiple SSTables.

Entries within a block are sorted by key. Blocks target ~4 KB each; when the
accumulated entries reach the target, the block is flushed and a new one begins.

### Index Block

One entry per data block:

```
[keylen: u32][key bytes][offset: u64][length: u64]
```

`key` is the first key in the data block. `offset` and `length` locate the
block within the file.

### Bloom Block

A serialized bloom filter for probabilistic membership testing. Lets `get()`
skip SSTables that definitely don't contain a key.

```
[k: u32][m_bits: u64][bit array: m_bits/8 bytes]
```

| Offset | Size          | Field     | Notes                                         |
|--------|---------------|-----------|-----------------------------------------------|
| 0      | 4             | k         | Number of hash functions.                     |
| 4      | 8             | m_bits    | Total bits in the filter (multiple of 8).     |
| 12     | m_bits / 8    | bit array | The filter bits.                              |

Hashing uses FNV-1a 64-bit split into two 32-bit halves (h1, h2) with
Kirsch-Mitzenmacher double hashing: bit position `i` = `(h1 + i * h2) % m`
for `i` in `0..k-1`.

Default parameters: 10 bits per key, `k = round(bits_per_key * ln2) = 7`.

### Footer (exactly 40 bytes)

```
[index_offset: u64][bloom_offset: u64][entry_count: u64][max_seq: u64][format_version: u32][magic: u32]
```

| Offset | Size | Field            | Notes                                      |
|--------|------|------------------|--------------------------------------------|
| 0      | 8    | index_offset     | Byte offset of the index block.            |
| 8      | 8    | bloom_offset     | Byte offset of the bloom block.            |
| 16     | 8    | entry_count      | Total entries across all data blocks.       |
| 24     | 8    | max_seq          | Highest sequence number in this SSTable.   |
| 32     | 4    | format_version   | Currently 3.                               |
| 36     | 4    | magic            | `0x4B535442` ("KSTB").                     |

The reader seeks to `end - 40` and validates magic first. The index block
occupies `[index_offset, bloom_offset)`, and the bloom block occupies
`[bloom_offset, end - 40)`.

## MANIFEST

The MANIFEST is an append-only log that records the live SSTable set and its
leveled layout, so the level structure is restored on open. Each append is a
CRC32-framed **full snapshot** of the live files; recovery scans forward and
keeps the last record whose CRC validates (torn-tail tolerant, like the WAL).

```
[crc32: u32][count: u32][file entry 0][file entry 1]...[file entry count-1]
```

Each file entry:

```
[number: u32][level: u32][sklen: u32][smallest_key bytes][lklen: u32][largest_key bytes]
```

- `number` — the SSTable file number (`<number>.sst`).
- `level` — which level (0..6) the file belongs to.
- `smallest_key` / `largest_key` — the file's key range, used for overlap
  checks and range-based placement during leveled compaction.

The CRC32 covers everything after itself (the count and all file entries).

## Leveled layout

SSTables are organized into levels **L0..L6**. L0 files come straight from
memtable flushes and may have **overlapping** key ranges. Each level ≥ 1 holds
files with **non-overlapping**, sorted key ranges (a partitioned sorted run),
and each level's size limit grows geometrically (L1 base, ×10 per level).
Recency across levels is resolved by the entry `seq` (a higher `seq` always
wins), so a key may transiently exist at multiple levels until compaction
merges it down. Tombstones and superseded versions are dropped only when a
compaction reaches the bottommost populated level, where no older version can
survive elsewhere.
