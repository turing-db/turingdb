# TuringProto: Python sink, network measurements and redesign research

Branch `experiment/turing-proto-sink-and-compression-research`, 2026-10-02 to 2026-10-06.
Everything here is experimental: the code builds and passes the checks described below,
but the decoder hook is not a documented extension point, the StringDType path needs NumPy 2
headers at build time, and nothing has gone through review.

## 1. What is on this branch

- `python/turingdb/_binary/Numpy*`: a `NumpySink` for the binary protocol decoder. Fixed-width
  columns are decoded straight into memory that NumPy takes over with no copy
  (`NumpyBuffer`, `NumpyColumnVector`). Nullable fixed-width columns are decoded in bulk into a
  value block plus a mask (`NumpyMaskedColumnVector`) through a specialisation of
  `OptionalVectorColumnDecoder<T, NumpySink>` declared in `NumpySink.h`. Strings are packed into
  NumPy `StringDType` storage (`NumpyStringArray.cpp`) with no Python object per row.
  Embeddings of one dimension become one `(rows, dim)` float32 array.
- `PyTuringClient::query` runs its own copy of the receive loop (`receiveQuery`) against the
  `NumpySink`, releasing the GIL while it receives and decodes. `TuringClient` exposes its
  transport steps (`sendRequest`, `recvHttpResponseHeaders`, `recvChunkSizeLine`,
  `recvChunkBody`, `recvCrlf`, `getInBuf`) for that.
- `binary_client.py` builds pandas columns from the arrays without copies (`copy=False`,
  `IntegerArray` and `BooleanArray` over the value and mask blocks).
- `net/decoders`: `ProtoDecodeSink` checks nullable columns with `string_view` instead of
  `uint64_t`, so a sink may store fixed-width nullable columns differently; one call in
  `TuringProtoDecoder.h` passes `T` explicitly because template deduction cannot see through a
  `std::conditional_t` column alias. `TuringSink` and `WasmSink` compile to the same code as
  before.
- `NanobindUtils`: `ValueToPyObject` and `entityListToPy` moved out of the anonymous namespace so
  both the embedded client and the new sink use them; `allocColumns` and `appendDfs`, which only
  the old binary path used, are removed.
- `tools/TuringForge/TuringForge.cpp`: `while (client->isRecvComplete())` instead of `if` around
  `advanceCycle()`. `sendQuery` attempts the next exchange inline, so on a fast link the response
  it issued can already be complete; the old code re-armed epoll for nothing and the connection
  idled until the deadline. Before the fix, small loads on loopback reported 0 queries with
  "errors: none", or random throughput (316 to 7,105 queries/s for `RETURN 1`); after it, a
  steady 24,000.

Checks run: a 24-query comparison of the new client against the old one (`query_raw` and
`query`), covering IDs, every scalar type, nullable columns, embeddings, lists, maps,
constants, temporals, aggregates, procedures, empty results and 1M-row results that arrive in
many chunks; identical values on all 24. `test_net_turing_proto_*` and
`test_net_proto_analyze_error` pass. The embedded client still answers `RETURN 1 AS a, 'x' AS s,
[1, {k: 2}] AS l, null AS z` correctly.

## 2. Python client: old client vs NumPy sink

Server and client on one 20-core machine over loopback. Median of 5 to 200 runs after 3
warm-up runs. "Raw" is `query_raw()`, "pandas" is `query()`. Each old/new pair was measured in
the same run; the old client's own times varied by up to 20% between rounds.

| Result | Rows | Raw (ms) | pandas (ms) | Peak client memory (MB) |
|---|---|---|---|---|
| `RETURN 1` | 1 | 0.045 → 0.045 | no change | no change |
| Node IDs | 1M | 8.5 → 1.0 | 13.7 → 7.8 | 39 → 24 |
| Node, edge, node IDs | 1M | 50 → 36 | 74 → 59 | 101 → 77 |
| Int + Double properties | 1M | 54 → 21 | 104 → 22 | 143 → 23 |
| Int + Double properties, 10% null | 1M | 56 → 25 | 160 → 32 | 137 → 53 |
| Bool property | 1M | 20 → 10 | 59 → 10 | 37 → 3 |
| String property | 1M | 52 → 34 | 88 → 120 | 126 → 187 |
| ID + Int + String + Bool | 1M | 110 → 47 | 212 → 149 | 239 → 219 |
| Embeddings, 128 dimensions | 250k | 174 → 82 | 182 → 90 | 443 → 280 |
| Node IDs | 5M | 55 → 19 | 67 → 20 | 185 → 43 |
| Int + Double properties | 5M | 349 → 107 | 623 → 108 | 705 → 99 |
| ID + Int + String + Bool | 5M | 632 → 299 | 1172 → 683 | 1198 → 948 |

Where the time went, in order of removal:

1. The old client copied every chunk into a buffered dataframe, then copied or converted each
   column again into NumPy or Python lists. The sink writes rows into the final block.
2. Nullable columns were Python lists with `None`. They are now a `MaskedArray` (raw) or a pandas
   nullable array (pandas). Building the optional vector and then splitting it cost 212 ms on 5M
   rows of Int + Double; decoding values and bitmask in bulk costs 106 ms. The client's share of
   a 5M-row nullable column is now about 2 ms; the rest is the server.
3. `pd.DataFrame` and `pd.Series` copy by default. `copy=False` took the 5M Int + Double query
   from 132 to 108 ms and its peak memory from 257 to 99 MB.
4. Strings as `StringDType` make `query_raw` faster (52 → 34 ms per 1M) but `query()` slower
   (88 → 120 ms): pandas' `"string"` dtype still holds one Python object per row and builds them
   more slowly from a `StringDType` array than from a list. Options: lists for `query()` and
   `StringDType` for `query_raw()`; or pyarrow string arrays built from offsets and bytes, which
   would remove the per-row object on the pandas side too.

Measured separately: `torch.from_numpy` wraps the `(rows, dim)` embedding array in 70 µs with
no copy, and the tensor stays valid after the NumPy array is dropped.

## 3. Network: local vs remote, one connection

TuringForge, one connection, 15 s per load after a 2 s warm-up, Reactome (2,978,202 nodes,
11,537,843 edges) on both servers. The remote was reached over Tailscale on a direct path;
TCP connect takes 0.28 ms minimum, 0.45 ms median. Response bytes were read from the kernel's
socket counters (`ss -ti`, `bytes_received`).

| Load | Response | p50 latency, local → remote | Queries/s, local → remote | MB/s, local → remote |
|---|---|---|---|---|
| `RETURN 1` | 221 B | 0.04 → 0.33 ms | 24,305 → 2,807 | |
| Point lookup (15 rows) | 484 B | 0.07 → 0.44 ms | 14,041 → 2,305 | |
| `count(n)` | 232 B | 0.04 → 0.38 ms | 22,341 → 2,575 | |
| 10k node IDs | 80 KB | 0.09 → 1.39 ms | 10,354 → 709 | 831 → 57 |
| 100k node IDs | 0.8 MB | 0.35 → 8.1 ms | 2,645 → 124 | 2,117 → 99 |
| 1M node IDs | 8 MB | 1.0 → 74 ms | 977 → 13.5 | 7,818 → 108 |
| 3M node IDs | 24 MB | 3.0 → 219 ms | 330 → 4.3 | 7,861 → 103 |
| 1M strings (`displayName`) | 31 MB | 18 → 282 ms | 53 → 3.5 | 1,623 → 107 |
| 11.5M edges (a, e, b) | 277 MB | 59 → 2,492 ms | 16.9 → 0.4 | 4,680 → 111 |
| `gnn.graphSAGE`, 2048 seeds, 15/15/15 | 1.4 MB | 5.0 → 18 ms | 196 → 56 | 276 → 79 |

- Small queries pay one round trip, about 0.29 ms. With one connection the client waits for it
  on every query, so throughput drops 9×. `1 / (0.3 ms + 0.041 ms) = 2,930/s` matches the
  measured 2,807. The server is idle 92% of the time.
- Large results plateau at 99 to 111 MB/s. The client's NIC is 1 Gbit/s; after Ethernet
  framing that is 117.7 MB/s at MTU 1500, and 111.4 MB/s inside a WireGuard tunnel at MTU 1280.
  We are at 98.7% of the ceiling, so only fewer bytes or a faster link help.
- Forge's "engine" time on the remote equals the full latency for large results (2,456 of
  2,492 ms for the edges). That is the server blocked on a full socket, not query work; see the
  busy-spin defect in section 5.

The Forge workload (`--graph reactome`):

```
RETURN 1
MATCH (n:Pathway {displayName: 'Apoptosis'}) RETURN n, n.dbId
MATCH (n) RETURN count(n)
MATCH (n:Pathway) RETURN n LIMIT 10000
MATCH (n) RETURN n LIMIT 100000
MATCH (n) RETURN n LIMIT 1000000
MATCH (n) RETURN n
MATCH (n) RETURN n.displayName LIMIT 1000000
MATCH (a)-[e]->(b) RETURN a, e, b
CALL gnn.graphSAGE([<2048 random node ids>], [15, 15, 15]) YIELD src_nodes0, tgt_nodes0, src_nodes1, tgt_nodes1, src_nodes2, tgt_nodes2 RETURN src_nodes0, tgt_nodes0, src_nodes1, tgt_nodes1, src_nodes2, tgt_nodes2
```

## 4. Encodings measured on real Reactome result columns

Scalar C++ encoders (`-O3 -march=native`, one thread), sizes and times on the columns the
queries above return. Every encoding decoded back to the original bytes. "Delta + FOR" is the
Parquet `DELTA_BINARY_PACKED` / FastPFor scheme: per block of 128 values, subtract the block's
smallest delta, then bit-pack to the widest remainder.

| Column | Raw | Bit-pack to max width | Delta + varint | Delta + FOR, blocks of 128 | zstd -1 |
|---|---|---|---|---|---|
| 3M node IDs (scan order) | 23.8 MB | 2.9× | 8× | 114× (0.21 MB; 2.2 ms encode, 2.5 ms decode) | 7.9× |
| Edge source `a` (sorted) | 92.3 MB | 2.9× | 8× | 43× (24 ms, 11 ms) | 27× |
| Edge ID `e` (dense) | 92.3 MB | 2.7× | 8× | 114× (8 ms, 12 ms) | 7.9× |
| Edge target `b` (random) | 92.3 MB | 2.9× (56 ms, 7 ms) | 2.8× | 2.9× | 4.4× (168 ms, 72 ms) |
| graphSAGE batch IDs (random, 92,606 values) | 0.74 MB | 2.9× (0.5 ms, 0.05 ms) | 3.3× | 2.7× | 3.9× |

Reactome IDs fit in 22 bits, so plain bit-packing gives 2.9× on any ID column. Sorted or dense
columns collapse under delta encoding. zstd compresses random IDs better (4.4×) at 3 to 10×
the CPU.

| Strings, 1M rows | Distinct values | Raw | Dictionary + bit-packed codes | zstd -1 |
|---|---|---|---|---|
| `displayName` | 674,796 (67%) | 30.5 MB | 20.3 MB (1.5×; 122 ms encode, 1.5 ms decode) | 6.6 MB (4.6×; 38 ms, 13 ms) |
| `schemaClass` | 8 | 23.4 MB | 0.38 MB (62×; 7.4 ms, 0.4 ms) | 2.3 KB (scan-order runs; 1.4 ms, 5.5 ms) |
| `speciesName`, 54% null | 73 | 8.8 MB | 0.40 MB (22×; 5.5 ms, 0.2 ms) | 0.69 MB (12.6×; 6.1 ms, 2.5 ms) |

A dictionary pays off only when values repeat: on `displayName` the dictionary is nearly as
large as the column and costs more to build than the transfer it saves. Sampling the first
1,024 rows of `displayName` shows 14% distinct, but some 64Ki-row chunks are 100% distinct, so
a sample cannot choose the encoding; the server has to try a dictionary with a size budget per
chunk and fall back to plain when it stops paying (the Parquet writers' rule).

Estimated effect at 110 MB/s, transfer + encode + decode in series:

| Result | Today | Encoded |
|---|---|---|
| 1M `displayName` (zstd -1) | 277 ms | 60 + 38 + 13 = 111 ms (2.5×) |
| 1M `schemaClass` (dictionary) | 213 ms | 3.4 + 7.4 + 0.4 = 11 ms (19×) |
| 3M node IDs (delta + FOR) | 217 ms | 1.9 + 2.2 + 2.5 = 6.6 ms (33×) |
| 11.5M edges, a + e + b (bit-pack each) | 2,517 ms | 315 + 89 + 26 = 430 ms (5.9×) |
| graphSAGE batch (bit-pack, no null filler) | 12.8 ms transfer | about 3 ms; the batch goes from 18 to about 8 ms |

On loopback the same encodings lose: the 3M IDs go from 3.0 to about 4.7 ms, zstd on the
strings is 15× slower than raw. Encoding must be negotiated per connection.

Embeddings were not benchmarked: the test graph's embeddings were random floats, which say
nothing about real ones, and Reactome has none. Published numbers (section 6) put lossless
compression of float32 embeddings at 1.0 to 1.2×.

## 5. What the wire carries today

Read from `net/turing_proto_common`, `net/encoders`, `net/turing_proto_server` and
`net/tcp_server`; measured with Forge, `strace` and the socket counters.

Framing. A query is an HTTP/1.1 `POST /query?graph=&commit=&change=` with a bearer token and
the Cypher text as the body; the body may not exceed about 104 KB (`NetBuffer::BUFFER_SIZE / 10`).
The response is always `200 OK` with chunked transfer. Every proto packet is one HTTP chunk: a
10-byte hex size line, a 5-byte proto header (type u8, length u32), the payload, CRLF. Packet
types: `CHUNK_HEADER` (schema, once per response), `CHUNK` (data, up to 1 MiB), `END_CHUNK`
(row count) after every 64Ki-row engine chunk, `END` (execution time, float32), `ERROR`,
`PROTOCOL_ERROR`. A `RETURN 1` response is 221 bytes in 5 packets: 6 `sendmsg` calls on the
server, about 14 `recv` calls on the client.

Column layout, per 64Ki-row chunk, per column: a u32 row count, then the data.

| Column type | On the wire | Cost on real data |
|---|---|---|
| Fixed-width (IDs, Int64, Double, DateTime) | raw bytes, 8 per value | Reactome IDs fit in 22 bits: 2.9× what bit-packing needs, 43 to 114× what delta encoding needs when sorted |
| Nullable | u64 bitmask, then full-width values with zero filler for nulls | a graphSAGE batch is 46% null: 645 KB of its 1.41 MB is filler |
| String | `[len u32][bytes]` per row, interleaved | no bulk copy possible; the lengths are 13% of `displayName`'s bytes |
| Embedding | `[byteSize u32][floats]` per row | the dimension is repeated per row even when fixed |
| List, map | `[count][byteSize]` headers; `byteSize` is the decoder's in-memory footprint | the wire format encodes the client's memory layout |
| Constant | one value | |

No compression, no alternative encodings, no negotiation: loopback and WAN get the same bytes.

Copies. Server: column memory → 1 MiB output buffer (`memcpy`) → kernel (`sendmsg`, 4 iovecs).
Client: kernel → 1 MiB input buffer → column memory → NumPy (zero-copy with the sink). Two
copies per side for fixed-width data; one per side is avoidable because the row count arrives
before the bytes.

Concurrency. One query in flight per connection. The server runs epoll with 8 workers
(edge-triggered, one-shot); a request is parsed, executed and streamed synchronously on one
worker.

Two defects:

1. A slow client pins a server thread. `TuringProtoWriter::flush()` retries `sendmsg` with
   `continue` on `EAGAIN`, and accepted sockets are non-blocking. Measured with a client reading
   at 1 MB/s: one server thread at 100% CPU for the whole transfer. With 8 workers, 8 slow
   readers stall the server; one 277 MB result over the 1 Gbit/s link holds a worker spinning
   for 2.5 s. Not fixed on this branch.
2. TuringForge: the fast-link stall (fixed here), `--output FILE` is parsed but never used, and a
   refused connection counts as zero loads with "errors: none".

## 6. What other systems do

Three research passes over primary sources (specs, kernel docs, papers, vendor engineering
posts), read on 2026-10-02. The numbers are the sources' numbers.

Transport.

- HTTP/2's per-stream window starts at 65,535 bytes, which caps one stream at 3.3 MB/s at
  20 ms RTT; gRPC-Go had to add BDP probing to get a 1 MB RPC from 455 to 195 ms. HPACK is
  stateful header compression for headers we do not have. Arrow Flight reaches 1 GB/s per gRPC
  stream and needs 16 parallel streams for 10 GB/s.
- QUIC costs about 2× TCP's CPU for the same throughput. On a 10 Gbit/s testbed user-space
  QUIC stacks reached 0.09 to 4.9 Gbit/s where TCP reached 8. In-kernel QUIC (patch v14, July
  2026, unmerged) reaches half of kTLS and a quarter of plain TCP.
- Pipelining: libpq pipeline mode takes 100 statements at 300 ms RTT from 30 s to 0.3 s;
  Redis `-P 16` gives 10×; Cassandra puts a 16-bit stream ID in every frame. All three warn of
  the same deadlock: a client that blocks on send while the server blocks on send.
- `MSG_ZEROCOPY` pays only above about 10 KB per send and always copies on loopback; io_uring
  `SEND_ZC` is 2.4× faster than it in the kernel's own numbers; zero-copy receive (`zcrx`,
  Linux 6.15) needs header-split NICs and matters above 25 Gbit/s. At 110 MB/s the kernel copy
  is about 1% of one core.
- WAN: a single stream is capped at `tcp_wmem[2] / RTT` (4 MB / 100 ms = 40 MB/s). Raise the
  sysctl maxima; `setsockopt(SO_SNDBUF)` locks the buffer and disables autotuning. BBR is 2 to
  25× CUBIC under loss on Google's WAN.
- Same host: Unix sockets give 30 to 50% more small-message throughput than loopback TCP
  (Redis +50%, MySQL +33%). Shared memory is flat at 0.6 to 0.7 µs for any size where a Unix
  socket takes 110 µs at 256 KB and 1.7 ms at 4 MB (iceoryx). `memfd_create` + `F_SEAL_WRITE`
  lets a reader map a result read-only knowing the writer cannot change it. Arrow's Plasma store
  was retired in 12.0 for maintenance reasons: pass sealed fds per result, do not build a daemon.
- Framing: Arrow IPC pads every buffer to 8 bytes and recommends 64; Cassandra v5 uses a 6-byte
  header with CRC24, CRC32 on the payload, and 128 KiB frames. Hardware CRC32C runs at 23 to
  94 GB/s per core: about 0.5% of a core at 1 Gbit/s.

Wire protocols.

| System | Mechanism | Number |
|---|---|---|
| Arrow IPC / Flight | Schema once; record batches of raw buffers at 8- or 64-byte offsets; validity bitmap omitted when no nulls; dictionary batches with `isDelta`; per-buffer LZ4/ZSTD with a `-1` opt-out; run-end encoding (1.3); 16-byte string views (1.4) | 3.1 GB/s per stream on localhost, 6 GB/s on 100 Gbit/s. gRPC's misaligned buffers force a copy, so Flight is moving to a "Dissociated IPC" design with metadata and data on separate streams |
| PostgreSQL v3 | Row-oriented: 4 bytes per value, 5 per row; `RowDescription` once; portals with a row limit; pipelining in libpq since PG14; cancel over a new connection with a key | 7.2 GB CSV becomes 10.4 GB on the wire; the cancel key grew from 32 to 256 bits in protocol 3.2 (PG18), the first new version since 2003 |
| Neo4j Bolt | PackStream: a type marker on every value, labels and property keys repeated per node; 64 KiB chunks; `PULL n` costs a round trip per batch | Neo4j routes bulk I/O around Bolt through Arrow Flight: 20× the Java driver, 450× the Python driver. A 5.x buffering change sent 100× more packets (299 B average) and was 1.75× slower |
| ClickHouse native | ~1 MB blocks (65,409 rows); 25-byte compression frame per block, ZSTD level 3 by default, bypassed under 128 bytes; revision negotiated as min(client, server); in-band `Cancel`; `Progress` interleaved with data; `LowCardinality` = a dictionary per block; per-column codecs `Delta`, `DoubleDelta`, `Gorilla`, `T64` chained with ZSTD | The 128-byte bypass saves 27 bytes and 1.3 µs per tiny frame. Delta+ZSTD took an `Id` column from 1.42× to 3.43× and made `ViewCount` larger: codec choice is data-dependent |
| Snowflake, Databricks, Trino, BigQuery | Results above about 1 MB are written as parallel partitions and fetched in parallel; small results inlined; chunks carry a row offset for resume | Databricks 12× on 3.4 GB; Trino 35 s → 9 s; BigQuery up to 1,000 streams. Teradata's Flight SQL gain of 15 to 84× came from parallel streams; a single stream was 10 to 15% slower than JDBC |
| MotherDuck | Flow control by pausing the producing pipeline until the consumer catches up | DuckDB pipelines were made pausable for this |
| SBE | Schema-driven fixed layout, 12-byte header, var-length fields last, read in place | ~25 ns per message vs ~1,000 ns for protobuf |

The reference study is Raasveldt and Mühleisen, "Don't Hold My Data Hostage" (VLDB 2017):
exporting 7.2 GB of TPC-H `lineitem` over loopback took 202 s and 10.4 GB through PostgreSQL
against 10 s for the CSV over `netcat`. Column-major chunks of about 1 MB are optimal (2 KB
chunks: 56 s; 1 MB: 10 s); no compression wins on localhost, Snappy/LZ4 win at 1 Gbit/s and
below, gzip only at 10 Mbit/s; a per-batch acknowledgement collapses with latency (DB2: 167 s
local, 598 s on a LAN); the null bitmask is omitted when statistics prove no nulls; the client
declares its maximum chunk size in bytes at connect.

Column encodings.

- Bit-unpacking is memory-bound: FastLanes (VLDB 2023) interleaves 1024 values so plain C
  auto-vectorises, 70 values per cycle on AVX-512. Lemire's `simdcomp` decodes over 8 G ints/s
  in 128-value blocks. Our scalar 1 to 2 ns per value is the slow end of the family by 5 to
  100×. BtrBlocks (SIGMOD 2023) decodes dictionaries at 19.6 GB/s per core; Vortex, its
  production descendant (Apache-2.0), is 10× smaller than Arrow and 10 to 25× faster to
  decompress than Parquet+ZSTD.
- Patched frame-of-reference (ORC's 95th-percentile width) handles one outlier in an int64
  property without forcing 64 bits on the block.
- Elias-Fano costs 2 + log2(universe / count) bits per sorted ID: 8 bits for 64Ki IDs out of
  3M. Random IDs have no structure: 22 bits per ID is the floor unless values repeat.
- ALP (SIGMOD 2024) encodes decimals as scaled integers plus exceptions: 21.7 bits per double
  over 30 datasets against 42.2 for Gorilla and 20.6 for zstd, decoding 26× faster than zstd.
  DuckDB made it the default; Parquet adopted it in September 2026.
- Embeddings: lossless compression gets 1.0 to 1.2× (FCBench, VLDB 2024). fp16 halves the bytes
  at identical recall (pgvector 0.945 vs 0.945; Faiss SIFT1M R@1 0.991 vs 0.991); int8 with a
  per-vector scale gives 4× at about 1% recall loss (Qdrant, Elastic, Hugging Face). Faiss's
  `ScalarQuantizer` is already in our tree.
- Strings: Parquet writers try a dictionary until it exceeds 1 MB or stops paying, then fall back
  to plain for the rest of the row group. FSST (VLDB 2020) gets 2.2× with one 255-symbol table
  and decodes at 1 to 3 cycles per byte, but a table costs about 2 ms to build: one per column
  per stream. zstd -1 encodes at 0.4 to 0.5 GB/s and decodes at 1.4 to 1.6 GB/s per core; LZ4
  at 0.6 and 3.8 GB/s; LZ4 on blocks under 1 KB stops compressing.
- Nulls: Arrow sends a bitmap plus a slot per row (spaced), omitting the bitmap when there are
  no nulls; Parquet and ORC send only non-null values (sparse) and the reader scatters.
  Sentinels break FOR and ALP.
- Lists and maps: offsets plus a child column decode with one subtraction per row; per-row
  length-prefixed inline data (our layout) allows neither bulk decode nor column-wise
  compression.
- Selection: BtrBlocks samples 1% (10 runs of 64 values), rules out schemes by statistics,
  tries the rest on the sample, and spends 1.2% of its CPU on the choice, picking the optimum
  77% of the time.
- Where encoding stops paying: with chunk pipelining, throughput is min(encode rate, ratio ×
  link rate, decode rate). At 1 Gbit/s every codec is faster than the link. At 10 Gbit/s zstd -1
  and LZ4 are 2 to 3× slower than the link per core. On loopback (8 GB/s) only encoders above 8
  to 12 GB/s per core are not a loss: bit-packing, delta, RLE, ALP, byte shuffle.

## 7. Ideas, ranked

Ranked by gain per unit of work for the workloads measured above. Gains are measured in this
session or taken from the cited systems; estimates are marked.

Fix now, no format change.

1. Stop spinning on a full socket. `flush()` should wait for writability (`poll`, or hand the
   connection back to epoll between chunks) instead of retrying `sendmsg` on `EAGAIN`. Gain: a
   slow client costs a worker 0% CPU instead of 100%. Cost: small for `poll`; the epoll
   hand-back is larger because a query runs synchronously on its worker.
2. Coalesce small packets and buffer reads: `END_CHUNK` + `END` + the terminator in one
   `sendmsg`; a 64 KB client read buffer. Gain: 6 → 2 sends and 14 → 2 receives per tiny
   query; of the 41 µs loopback round trip, 16 µs is transport at about 8 µs per wakeup, so an
   estimated 10 to 20% off small-query latency.

Protocol v2: framing.

3. Raw framed TCP with a 16-byte header `{magic+version, flags, stream id, length}`, body padded
   to 8 bytes, column buffers at 64-byte offsets, frames capped at 1 MiB; REST stays on the same
   port by sniffing the first bytes. Gain: no text parsing per packet; bodies land aligned for
   in-place use; prerequisite for 4 and 7. Cost: framing changes in `TuringClient`,
   `BinaryClient`, the WASM decoder and Forge.
4. Stream IDs, N requests in flight, byte credits, in-band cancel. Up to a negotiated window of
   requests per connection; responses carry the stream ID and may interleave; a credit window in
   bytes per stream (initial 8 to 16 MiB, not HTTP/2's 64 KiB); `CANCEL(stream)` with a key of
   at least 16 bytes, and a reply. Gain: one connection over a 0.3 ms round trip goes from 2,807
   to about 40,000 queries/s per server core at 14 in flight (estimated from the arithmetic);
   a 100 MB result can no longer starve a 100-byte reply. Cost: an async client API; a
   per-connection queue on the server; the reader must never block while sending.
5. A hello with versions and a capability bitmask, gating every later field; append-only enums.
6. Typed side frames: `PROGRESS`, `STATS`, `WARNING`, a final `SUMMARY`; an estimated row count
   up front lets the client size buffers once.

Protocol v2: column bodies.

7. Arrow-shaped column buffers. Per column per chunk: an encoding tag, a byte length, a null
   count, then buffers: validity bitmap only when `null_count > 0`, values contiguous. Strings
   become offsets + bytes, embeddings one float32 block with the dimension in the schema, lists
   offsets + a child column. Gain: every column decodes as a bulk copy; a graphSAGE batch drops
   from 1.41 to about 0.76 MB by dropping null filler alone; pyarrow, Polars and DuckDB can read
   the buffers. Cost: a rewrite of the variable-width encoder and decoder paths and the nested
   writer, in all clients.
8. Per-column lightweight encodings chosen per chunk: FOR + bit-packing in 1024-value blocks,
   delta when sorted, patched exceptions; dictionary with delta dictionaries for repeated
   strings, labels and edge types; run-length; ALP for doubles. Rules from storage statistics
   first, a 1% sample second. Gain measured: IDs 2.9 to 114×, repeated strings 19 to 62×,
   doubles about 3× (reported). Decoding at 10 to 50 GB/s per core is free on every link,
   loopback included. Libraries: FastLanes (MIT), ALP (MIT), FSST (MIT), `simdcomp` (BSD).
9. Outer compression by link class: none on loopback, Unix sockets and 25 Gbit/s up; LZ4 or
   `zstd --fast` on string and byte buffers for 10 Gbit/s and below; skip frames under 128
   bytes. Gain measured: 1M names 277 → 111 ms at 1 Gbit/s; 15× slower on loopback if left on.
10. Negotiated embedding precision: raw by default, fp16 (2×) and int8 with a per-vector scale
    (4×) on request. Lossless is not worth a codec.
11. Prepared statements with binary parameters: register once, execute by handle with typed
    arrays (2,048 seed IDs as 16 KB of `uint64`, not Cypher text). Profile how much of the
    graphSAGE batch's 5 ms server time is parsing first.

Deployment and transport.

12. A Unix-domain listener for same-host clients, same framing: 30 to 50% more small-message
    throughput. Later, `memfd` + `F_SEAL_WRITE` passed over it for zero-copy delivery of large
    same-host results.
13. Kernel settings for WAN deployments, documented not coded: BBR with `fq`, `tcp_wmem[2]` and
    `tcp_rmem[2]` at 2× the bandwidth-delay product; never `SO_SNDBUF`. Once streams are
    multiplexed, `TCP_NOTSENT_LOWAT` around 128 to 256 KiB.
14. Result descriptors with endpoints, later: schema, row estimate and N endpoints with opaque
    tickets; results under about 1 MB inlined; chunks carry a row offset so a fetch can resume.
    Fits the partitioning roadmap: one endpoint per partition.
15. Optional per-frame CRC32C, on by default for plaintext links, off under TLS.

Not recommended: HTTP/2 framing or HPACK (idea 4 is the one thing it would give us); QUIC now
(2× TCP's CPU, in-kernel QUIC unmerged); `MSG_ZEROCOPY`, io_uring zero-copy receive, AF_XDP,
DPDK (at 1 Gbit/s the kernel copy is 1% of a core); RDMA for the client protocol (keep it for
the partitioning shuffle, on UCX or libfabric, with pre-registered pinned pools and credit-based
slots); fixed-width strings or sentinel nulls.

## 8. Roadmap

| Phase | Work | Measure |
|---|---|---|
| 0. Fixes (days) | Ideas 1 and 2; the Forge bugs; server CPU and bytes-on-wire columns in the Forge report | Server CPU during a 1 MB/s read: 100% → 0%. `RETURN 1` on loopback: 41 → about 35 µs |
| 1. Framing (1 to 2 weeks) | Ideas 3 to 6; raw TCP beside REST on one port; async `TuringClient` and an asyncio `BinaryClient` | One connection over 0.3 ms RTT: 2,807 → over 20,000 queries/s. p99 of a 1-row query while a 277 MB result streams on the same connection: under 5 ms |
| 2. Column bodies (2 to 3 weeks) | Idea 7 | graphSAGE batch 1.41 → about 0.76 MB; client decode of 5M nullable rows stays at about 2 ms; pyarrow reads a chunk body without our decoder |
| 3. Encodings (3 to 4 weeks) | Idea 8 in order: FOR + bit-packing, delta, dictionary with deltas, run-length, rule-based selection; then ALP and FSST; idea 9; idea 10 | At 1 Gbit/s: graphSAGE batch 18 → about 8 ms; 3M node IDs 219 → about 7 ms; 1M names 282 → about 110 ms. On loopback: no regression on any Forge load |
| 4. Deployment | Idea 12, then `memfd`; idea 13; idea 11 after profiling; idea 14 with partitioning; RDMA only for the shuffle | Same-host small queries +30%; a 4 MB same-host result in under 100 µs |

Phases 1 to 3 change frames or bodies and ship behind the hello of idea 5: a v1 client keeps
getting today's HTTP/1.1 chunked stream. Phase 0 and the Unix socket change nothing on the wire.

Two decisions before phase 1:

1. Does the native protocol stay inside HTTP/1.1? Raw framing is what every surveyed database
   does; staying in HTTP keeps proxies and load balancers working. A first-bytes sniff on the
   existing port is the middle path: `POST` goes to the HTTP parser, the v2 magic to the framed
   path.
2. Spaced or sparse nulls? Spaced (Arrow) is zero-copy for the client; sparse (Parquet) is fewer
   bytes and what bit-packing prefers. Spaced with null slots filled from the block minimum
   keeps both: bit-packing stays tight and the client stays zero-copy, at one `std::fill` per
   null run on the server.

What this does not fix: server execution time. A 5M-row property scan is about 100 ms of server
time against about 2 ms of client time, and the edge query's server time dominates on every
link. Profiling the encoder's copy into the 1 MiB buffer and the per-chunk allocation is the
next server-side step; idea 3's aligned bodies make scatter-gather sends straight from column
memory possible, which removes that copy.

## 9. Reproducing

- Server: `USE_TURING_PROTO=1 turingdb start -demon -turing-dir <dir> -p <port>`, then
  `LOAD JSONL "reactome.jsonl" AS reactome` (136 s) or `LOAD GRAPH reactome` from a dump (4 s).
  The JSONL file must sit inside `<dir>/data`; the server refuses symlinks that point outside it.
- Forge: `TuringForge --server <host>:<port> --connections 1 --threads 1 --queries <workload.json>
  --graph reactome --duration 15 --warmup 2 --animation none > out.txt` (`--output` is ignored).
- Response bytes: `ss -tinpH "dst <host>:<port>"` before and after one query, `bytes_received`.
- Busy-spin: open a raw socket, send the edges query as an HTTP POST, read 100 KB every 100 ms,
  and sample `utime + stime` from `/proc/<pid>/task/*/stat` for the server's threads.
- Python client comparison: run the same queries through the old and the new package in
  separate processes, pickle `query_raw` and `query` results, compare values and
  `pd.testing.assert_frame_equal`.
- Encodings: dump result columns with `query_raw` to raw `u64` files and a `[len u32][bytes]`
  string file; the scalar encoders are about 300 lines of C++ linked against `libzstd`.
