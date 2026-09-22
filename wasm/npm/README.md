# turingdb

JavaScript client for [TuringDB](https://github.com/turing-db/turingdb). It sends Cypher
over the TuringDB binary protocol and decodes the streamed result columns with a
WebAssembly build of the engine's own decoder, so the browser and the server run the same
decode code.

Works in Node 18+ and in the browser. No dependencies.

## Install

```sh
npm install turingdb
```

## Use

The server speaks the binary protocol when it is launched with `USE_TURING_PROTO=1`. It
listens on port 6666 and takes queries on `/query`.

```js
import { TuringClient } from "turingdb";

const client = new TuringClient({
    url: "http://localhost:6666/query",
    graph: "mygraph",
});

const { chunks, execTimeMs } = await client.query("MATCH (n:Person) RETURN n.name, n.age");

for (const { names, columns } of chunks) {
    console.log(names);
    console.log(columns[0].toArray());
}
```

A query resolves to one entry in `chunks` per streamed dataframe. Each is
`{ names, columns }`, where every column is a `Column`.

`queryData(cypher)` is the shortcut for plain JSON-shaped output: chunks by columns by
rows, with 64-bit integers converted to `Number`.

```js
const data = await client.queryData("MATCH (n:Person) RETURN n.name");
```

## Column

A `Column` holds the decoded buffers and builds JS values only when a row is read.

- `get(row)` — one value. 64-bit integers and entity IDs come back as `BigInt`.
- `toArray()` — every row.
- `toNumberArray()` — every row with `BigInt` converted to `Number`, inside lists and
  paths too. Past 2^53 the precision is lost.
- `values` — fixed-width vector columns as their typed array (`Float64Array`,
  `BigUint64Array`, ...), one element per row and no per-row work. `null` for constants
  and variable-width kinds.
- `validity` — for optional columns, one bit per row, LSB first, set where the row has a
  value. `null` otherwise.
- `length`, `name`, `typeCode`, `encoding`, `isConstant`, `isOptional`.

`ColumnType` and `ColumnEncoding` are exported for reading `typeCode` and `encoding`.

## Client

`new TuringClient(options)`:

- `url` — the query endpoint. Default `/query`.
- `graph` — graph name. Default `default`.
- `authToken` — sent as `Authorization: Bearer`.
- `bufferCapacity` — decoder buffer size in bytes. Default 1 MiB.
- `fetch` — a `fetch` implementation to use instead of the global one.
- `wasmUrl` — where to fetch `turing_wasm_decoder.wasm` from. See The wasm file.

The decoder is a WebAssembly module the client compiles on its first query and shares
across every client in the process. Nothing needs to be awaited to construct a client.

`setGraph`, `setChange`, `setCommit` and `setAuthToken` change the client state.
`query(cypher, options)` takes per-call `graph`, `change`, `commit` and `signal`
overrides.

A failed query throws `TuringQueryError`, carrying the server's `status` and `message`.

## The wasm file

The decoder locates `turing_wasm_decoder.wasm` with `new URL("turing_wasm_decoder.wasm",
import.meta.url)`. Node, browsers and every bundler that understands that pattern
(Vite, webpack 5, Rollup, Parcel) resolve it on their own, so nothing needs configuring.

Pass `wasmUrl` only to serve the file from somewhere else, such as a CDN:

```js
const client = new TuringClient({
    url: "/query",
    wasmUrl: "https://cdn.example.com/turing_wasm_decoder.wasm",
});
```

## License

Business Source License 1.1. See LICENSE.
