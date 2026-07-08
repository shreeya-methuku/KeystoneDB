# On-Disk Format Specification

All multi-byte integers are little-endian.

## WAL Record

Each entry in the write-ahead log is a single variable-length record:

```
[crc32: u32][type: u8][keylen: u32][vallen: u32][key bytes][value bytes]
```

`type`: 1 = PUT, 2 = DELETE (vallen = 0 for deletes). The CRC32 covers everything after itself (type through value bytes).

## SSTable File

An SSTable is an immutable, sorted file of key-value pairs organized into blocks:

```
[data block 0][data block 1]...[index block][bloom block][footer]
```

### Data Block Entry

```
[tag: u8][keylen: u32][vallen: u32][key bytes][value bytes]
```

- `tag 0` — live value entry.
- `tag 1` — tombstone (vallen = 0, no value bytes).

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
[index_offset: u64][bloom_offset: u64][entry_count: u64][reserved: u64][format_version: u32][magic: u32]
```

| Offset | Size | Field            | Notes                                      |
|--------|------|------------------|--------------------------------------------|
| 0      | 8    | index_offset     | Byte offset of the index block.            |
| 8      | 8    | bloom_offset     | Byte offset of the bloom block.            |
| 16     | 8    | entry_count      | Total entries across all data blocks.       |
| 24     | 8    | reserved         | Must be 0. Reserved for future use.        |
| 32     | 4    | format_version   | Currently 1.                               |
| 36     | 4    | magic            | `0x4B535442` ("KSTB").                     |

The reader seeks to `end - 40` and validates magic first. The index block
occupies `[index_offset, bloom_offset)`, and the bloom block occupies
`[bloom_offset, end - 40)`.
