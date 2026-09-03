// Browser/Node client for the TuringDB binary protocol: sends Cypher over HTTP and
// decodes the streamed Turing Proto packets with the wasm decoder module.
//
// The server speaks the protocol when launched with USE_TURING_PROTO=1: the response is
// HTTP/1.1 chunked transfer with one proto packet per chunk. fetch() strips the HTTP
// chunk framing, so the packet stream is re-framed here from each packet's own
// [type u8][dataLen u32le] header before being fed to the wasm decoder.
//
// The decoder hands each column back as flat buffers (values, offsets, validity) copied
// out of wasm memory in one call per column; a Column wraps them and builds JS values
// only when a row is read.
//
// Usage:
//     const module = await createTuringDecoderModule();
//     const client = new TuringClient(module, { url: "/api/query", graph: "mygraph" });
//
//     const { chunks, execTimeMs } = await client.query("MATCH (n) RETURN n");
//     // chunks[i] = { names, columns }; columns[j] is a Column: get(row), toArray(),
//     // 64-bit integers and IDs as BigInt
//
//     const data = await client.queryData("MATCH (n) RETURN n");
//     // chunks x columns x rows with BigInt converted to Number — the shape the
//     // visualizer's REST client returned as json.data

const PROTO_HEADER_SIZE = 5;

const MESSAGE_CHUNK_HEADER = 0;
const MESSAGE_CHUNK = 1;
const MESSAGE_END_CHUNK = 2;
const MESSAGE_END = 3;
const MESSAGE_ERROR = 4;
const MESSAGE_PROTOCOL_ERROR = 5;

// Mirrors net::proto::ColumnInternalKind and ColumnKind (TuringProtoHeaders.h).
export const ColumnType = Object.freeze({
    UINT64: 0,
    INT64: 1,
    DOUBLE: 2,
    STRING: 3,
    BOOL: 4,
    PATH: 5,
    EMBEDDING: 6,
    ENTITY_LIST: 7,
    LIST_VIEW: 8,
    VALUE_TYPE: 9,
    LIST_ELEMENT_VIEW: 10,
    NODE_ID: 11,
    EDGE_ID: 12,
    EDGE_TYPE_ID: 13,
    PROPERTY_TYPE_ID: 14,
    LABEL_ID: 15,
    LABEL_SET_ID: 16,
    COMMIT_HASH: 17,
    CHANGE_ID: 18,
    PROPERTY_NULL: 19,
});

export const ColumnEncoding = Object.freeze({
    VECTOR: 0,
    OPTIONAL_VECTOR: 1,
    CONSTANT: 2,
    OPTIONAL_CONSTANT: 3,
});

// Mirrors db::ListBufferTypeTag (storage/list/ListBufferTypeTag.h).
const LIST_TAG_INT = 0;
const LIST_TAG_UINT = 1;
const LIST_TAG_DOUBLE = 2;
const LIST_TAG_BOOL = 3;
const LIST_TAG_STRING = 4;
const LIST_TAG_EMBEDDING = 5;
const LIST_TAG_LIST_VIEW = 6;
const LIST_TAG_NULL = 7;
const LIST_TAG_NODE_ID = 8;
const LIST_TAG_EDGE_ID = 9;

// Mirrors db::QueryStatus::Status (base/QueryStatus.h); the ERROR packet's first
// payload byte indexes into this.
const QUERY_STATUS_NAMES = [
    "OK",
    "GRAPH_NOT_FOUND",
    "PARSE_ERROR",
    "ANALYZE_ERROR",
    "PLAN_ERROR",
    "EXEC_ERROR",
    "COMMIT_NOT_FOUND",
    "COMMIT_NOT_LOADED",
    "CHANGE_NOT_FOUND",
];

// ignoreBOM keeps a leading U+FEFF as a character, so decoded lengths match the
// decoder's UTF-16 offsets.
const utf8Decoder = new TextDecoder("utf-8", { ignoreBOM: true });

// Thrown for every failed query: status carries the server's QueryStatus name (or a
// client-side pseudo-status such as HTTP_ERROR / PROTOCOL_ERROR / DECODE_ERROR) and
// message the human-readable details.
export class TuringQueryError extends Error {
    constructor(status, message) {
        super(message);
        this.name = "TuringQueryError";
        this.status = status;
    }
}

// Reassembles whole proto packets from the arbitrarily-sliced fetch byte stream.
class PacketAssembler {
    constructor() {
        this.chunks = [];
        this.offset = 0; // read offset into chunks[0]
        this.size = 0;
    }

    push(bytes) {
        if (bytes.length > 0) {
            this.chunks.push(bytes);
            this.size += bytes.length;
        }
    }

    // Copies the next count bytes without consuming them (caller checks size first).
    peek(count) {
        const out = new Uint8Array(count);
        let written = 0;
        let chunkIndex = 0;
        let offset = this.offset;

        while (written < count) {
            const chunk = this.chunks[chunkIndex];
            const copied = Math.min(chunk.length - offset, count - written);
            out.set(chunk.subarray(offset, offset + copied), written);
            written += copied;
            chunkIndex += 1;
            offset = 0;
        }

        return out;
    }

    // Consumes and returns the next count bytes (caller checks size first).
    take(count) {
        const out = new Uint8Array(count);
        let written = 0;

        while (written < count) {
            const head = this.chunks[0];
            const copied = Math.min(head.length - this.offset, count - written);
            out.set(head.subarray(this.offset, this.offset + copied), written);
            written += copied;
            this.offset += copied;
            if (this.offset === head.length) {
                this.chunks.shift();
                this.offset = 0;
            }
        }

        this.size -= count;
        return out;
    }

    // Returns the next whole packet ([header][payload]) or null until fully buffered.
    nextPacket() {
        if (this.size < PROTO_HEADER_SIZE) {
            return null;
        }

        const header = this.peek(PROTO_HEADER_SIZE);
        const dataLen = (header[1] | (header[2] << 8) | (header[3] << 16) | (header[4] << 24)) >>> 0;
        const packetSize = PROTO_HEADER_SIZE + dataLen;
        if (this.size < packetSize) {
            return null;
        }

        return this.take(packetSize);
    }
}

// 'main'/'head'/'HEAD' select the tip; a number, BigInt or hex string selects a
// specific change/commit. The server parses these query params as hex.
function normalizeRef(value) {
    if (typeof value === "number" || typeof value === "bigint") {
        return value.toString(16);
    }
    const text = String(value).toLowerCase();
    return text === "main" || text === "head" ? "head" : text;
}

// Fixed-width element kinds: the typed array over the column's value bytes and, where
// the array element is not already the JS value, its conversion.
const FIXED_WIDTH_KINDS = {
    [ColumnType.UINT64]: { size: 8, wrap: (buffer) => new BigUint64Array(buffer) },
    [ColumnType.INT64]: { size: 8, wrap: (buffer) => new BigInt64Array(buffer) },
    [ColumnType.DOUBLE]: { size: 8, wrap: (buffer) => new Float64Array(buffer) },
    [ColumnType.BOOL]: { size: 1, wrap: (buffer) => new Uint8Array(buffer), read: (array, index) => array[index] !== 0 },
    [ColumnType.VALUE_TYPE]: { size: 1, wrap: (buffer) => new Uint8Array(buffer) },
    [ColumnType.NODE_ID]: { size: 8, wrap: (buffer) => new BigUint64Array(buffer) },
    [ColumnType.EDGE_ID]: { size: 8, wrap: (buffer) => new BigUint64Array(buffer) },
    [ColumnType.EDGE_TYPE_ID]: { size: 8, wrap: (buffer) => new BigUint64Array(buffer) },
    [ColumnType.PROPERTY_TYPE_ID]: { size: 2, wrap: (buffer) => new Uint16Array(buffer) },
    [ColumnType.LABEL_ID]: { size: 8, wrap: (buffer) => new BigUint64Array(buffer) },
    [ColumnType.LABEL_SET_ID]: { size: 4, wrap: (buffer) => new Uint32Array(buffer) },
    [ColumnType.CHANGE_ID]: { size: 8, wrap: (buffer) => new BigUint64Array(buffer) },
    [ColumnType.PROPERTY_NULL]: { size: 1, wrap: (buffer) => new Uint8Array(buffer), read: () => null },
};

// Strings are decoded with one TextDecoder call over the whole column and sliced by
// the decoder's UTF-16 offsets. If the decoded length disagrees (invalid UTF-8 was
// replaced with U+FFFD), each string is decoded from its own bytes instead.
function makeStringReader(bytes, byteOffsets, utf16Offsets) {
    let text = null;
    let perString = false;

    return (index) => {
        if (text === null) {
            text = utf8Decoder.decode(bytes);
            perString = text.length !== utf16Offsets[utf16Offsets.length - 1];
        }
        if (perString) {
            return utf8Decoder.decode(bytes.subarray(byteOffsets[index], byteOffsets[index + 1]));
        }
        return text.substring(utf16Offsets[index], utf16Offsets[index + 1]);
    };
}

// Walks the decoder's flat list bytes: a list or a single element from a byte offset.
// A cursor tracks the read position so a nested list resumes where its child ended.
// Strings are referenced by index into the dataframe's list string set.
export class ListReader {
    constructor(bytes, strings) {
        this._bytes = bytes;
        this._view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
        this._readString = makeStringReader(strings.values, new Uint32Array(strings.offsets.buffer), new Uint32Array(strings.utf16Offsets.buffer));
        this._cursor = 0;
    }

    readListAt(offset) {
        this._cursor = offset;
        return this._readList();
    }

    readElementAt(offset) {
        this._cursor = offset;
        return this._readElement();
    }

    _readList() {
        const count = this._view.getUint32(this._cursor, true);
        this._cursor += 4;

        const values = new Array(count);
        for (let index = 0; index < count; index++) {
            values[index] = this._readElement();
        }
        return values;
    }

    _readElement() {
        const tag = this._bytes[this._cursor];
        const payload = this._cursor + 1;

        switch (tag) {
            case LIST_TAG_INT:
                this._cursor = payload + 8;
                return this._view.getBigInt64(payload, true);
            case LIST_TAG_UINT:
            case LIST_TAG_NODE_ID:
            case LIST_TAG_EDGE_ID:
                this._cursor = payload + 8;
                return this._view.getBigUint64(payload, true);
            case LIST_TAG_DOUBLE:
                this._cursor = payload + 8;
                return this._view.getFloat64(payload, true);
            case LIST_TAG_BOOL:
                this._cursor = payload + 1;
                return this._bytes[payload] !== 0;
            case LIST_TAG_NULL:
                this._cursor = payload + 1;
                return null;
            case LIST_TAG_STRING:
                this._cursor = payload + 4;
                return this._readString(this._view.getUint32(payload, true));
            case LIST_TAG_EMBEDDING: {
                const size = this._view.getUint32(payload, true);
                const start = payload + 4;
                this._cursor = start + size;
                // slice() lands the floats in a fresh, aligned buffer.
                return new Float32Array(this._bytes.slice(start, start + size).buffer);
            }
            case LIST_TAG_LIST_VIEW:
                this._cursor = payload;
                return this._readList();
            default:
                throw new TuringQueryError("DECODE_ERROR", `Unknown list element tag ${tag}`);
        }
    }
}

// Entity list entries are [u8 type][u64 id], 9 bytes each.
function readEntityList(bytes, view, start, end) {
    const entries = [];
    for (let offset = start; offset < end; offset += 9) {
        entries.push({ type: bytes[offset], id: view.getBigUint64(offset + 1, true) });
    }
    return entries;
}

// Builds the item reader for a column's buffers: { read: (itemIndex) => JS value,
// values: the typed array over fixed-width values or null, direct: whether values[i]
// already is the JS value }.
function makeItemReader(buffers, listReader) {
    const typeCode = buffers.typeCode;
    const fixed = FIXED_WIDTH_KINDS[typeCode];

    if (fixed !== undefined) {
        if (buffers.elementSize !== fixed.size) {
            throw new TuringQueryError("DECODE_ERROR", `Column type ${typeCode} has element size ${buffers.elementSize}, expected ${fixed.size}`);
        }
        const values = fixed.wrap(buffers.values.buffer);
        const read = fixed.read ?? ((array, index) => array[index]);
        return { read: (index) => read(values, index), values, direct: fixed.read === undefined };
    }

    const offsets = buffers.offsets !== undefined ? new Uint32Array(buffers.offsets.buffer) : null;
    const variable = (read) => ({ read, values: null, direct: false });

    switch (typeCode) {
        case ColumnType.STRING:
            return variable(makeStringReader(buffers.values, offsets, new Uint32Array(buffers.utf16Offsets.buffer)));
        case ColumnType.EMBEDDING:
            return variable((index) => new Float32Array(buffers.values.buffer, offsets[index], (offsets[index + 1] - offsets[index]) / 4));
        case ColumnType.PATH:
            return variable((index) => Array.from(new BigUint64Array(buffers.values.buffer, offsets[index], (offsets[index + 1] - offsets[index]) / 8)));
        case ColumnType.ENTITY_LIST: {
            const view = new DataView(buffers.values.buffer);
            return variable((index) => readEntityList(buffers.values, view, offsets[index], offsets[index + 1]));
        }
        case ColumnType.LIST_VIEW:
            return variable((index) => listReader.readListAt(offsets[index]));
        case ColumnType.LIST_ELEMENT_VIEW:
            return variable((index) => listReader.readElementAt(offsets[index]));
        default:
            throw new TuringQueryError("DECODE_ERROR", `Unsupported column type ${typeCode}`);
    }
}

function bigIntToNumberDeep(value) {
    if (typeof value === "bigint") {
        return Number(value);
    }
    if (Array.isArray(value)) {
        return value.map(bigIntToNumberDeep);
    }
    return value;
}

// One decoded column: the flat buffers the decoder handed over, read row by row on
// demand. 64-bit integers and IDs read as BigInt; a missing optional reads as null.
export class Column {
    constructor(name, buffers, listReader, rowCount) {
        this.name = name;
        this.typeCode = buffers.typeCode;
        this.encoding = buffers.encoding;
        this.length = rowCount;
        this._buffers = buffers;
        this._itemCount = buffers.count;
        this._validity = buffers.validity ?? null;

        const reader = makeItemReader(buffers, listReader);
        this._read = reader.read;
        this._typedValues = reader.values;
        this._direct = reader.direct;
    }

    get isConstant() {
        return this.encoding === ColumnEncoding.CONSTANT || this.encoding === ColumnEncoding.OPTIONAL_CONSTANT;
    }

    get isOptional() {
        return this.encoding === ColumnEncoding.OPTIONAL_VECTOR || this.encoding === ColumnEncoding.OPTIONAL_CONSTANT;
    }

    // Fixed-width vector columns as their typed array (Float64Array, BigUint64Array, ...),
    // one element per row with no per-row work; null for constants and variable-width
    // kinds. A missing optional row holds 0 here: consult validity.
    get values() {
        return this.isConstant ? null : this._typedValues;
    }

    // Optional encodings: one bit per row, LSB first, set where the row has a value.
    get validity() {
        return this._validity;
    }

    get(row) {
        return this._item(this.isConstant ? 0 : row, this._read);
    }

    toArray() {
        if (!this._direct || this.isConstant || this._validity !== null) {
            return this._rows(this._read);
        }

        // An indexed copy: Array.from over a typed array goes through the iterator and is
        // several times slower.
        const values = this._typedValues;
        const out = new Array(this.length);
        for (let row = 0; row < this.length; row++) {
            out[row] = values[row];
        }
        return out;
    }

    // Every row with 64-bit integers and IDs as Number (JSON parity: past 2^53 the
    // precision is lost), inside lists and paths too.
    toNumberArray() {
        const fixed = FIXED_WIDTH_KINDS[this.typeCode];
        if (fixed === undefined) {
            return this.toArray().map(bigIntToNumberDeep);
        }
        if (fixed.size !== 8 || this.typeCode === ColumnType.DOUBLE) {
            return this.toArray();
        }

        // Two 32-bit halves per value: no BigInt allocated per row.
        const halves = new Uint32Array(this._buffers.values.buffer);
        const signed = this.typeCode === ColumnType.INT64;
        return this._rows((index) => {
            const low = halves[2 * index];
            const high = halves[2 * index + 1];
            return (signed ? high | 0 : high) * 4294967296 + low;
        });
    }

    _item(index, read) {
        if (index >= this._itemCount) {
            return undefined;
        }
        if (this._validity !== null && ((this._validity[index >> 3] >> (index & 7)) & 1) === 0) {
            return null;
        }
        return read(index);
    }

    _rows(read) {
        const out = new Array(this.length);
        if (this.isConstant) {
            return out.fill(this._item(0, read));
        }
        for (let row = 0; row < this.length; row++) {
            out[row] = this._item(row, read);
        }
        return out;
    }
}

// Reads the decoder's current dataframe: one Column per decoded column, all sized to
// the dataframe's row count (constants report the row count of their vector siblings).
function readDataframe(decoder) {
    const columnCount = decoder.getColumnCount();
    const names = [];
    const buffers = [];
    let rowCount = 0;
    let hasVector = false;

    for (let index = 0; index < columnCount; index++) {
        names.push(decoder.getColumnName(index));

        const columnBuffers = decoder.getColumnBuffers(index);
        buffers.push(columnBuffers);

        const isVector = columnBuffers.encoding === ColumnEncoding.VECTOR || columnBuffers.encoding === ColumnEncoding.OPTIONAL_VECTOR;
        if (isVector) {
            hasVector = true;
            rowCount = Math.max(rowCount, columnBuffers.count);
        }
    }
    if (!hasVector && columnCount > 0) {
        rowCount = 1;
    }

    const hasLists = buffers.some((column) => column.typeCode === ColumnType.LIST_VIEW || column.typeCode === ColumnType.LIST_ELEMENT_VIEW);
    const listReader = hasLists ? new ListReader(decoder.getListBytes(), decoder.getListStrings()) : null;

    const columns = buffers.map((columnBuffers, index) => new Column(names[index], columnBuffers, listReader, rowCount));
    return { names, columns };
}

// END packet payload: the query execution time as a little-endian f32, milliseconds.
function readExecTime(packet) {
    if (packet.length < PROTO_HEADER_SIZE + 4) {
        return null;
    }
    return new DataView(packet.buffer, packet.byteOffset + PROTO_HEADER_SIZE, 4).getFloat32(0, true);
}

// ERROR packet payload: one QueryStatus byte followed by the UTF-8 message.
function readError(packet) {
    const payload = packet.subarray(PROTO_HEADER_SIZE);
    const statusCode = payload.length > 0 ? payload[0] : 0;
    const status = QUERY_STATUS_NAMES[statusCode] ?? `STATUS_${statusCode}`;
    return { status, message: utf8Decoder.decode(payload.subarray(1)) };
}

// A C++ TuringException crossing the embind boundary is not a JS Error; recover its
// message through the exception handling helpers when the module exports them.
function describeWasmError(module, raised) {
    if (raised instanceof Error) {
        return raised.message;
    }
    if (typeof module.getExceptionMessage === "function") {
        try {
            const [type, message] = module.getExceptionMessage(raised);
            return message ? `${type}: ${message}` : String(type);
        } catch {
            // fall through to the generic stringification
        }
    }
    return String(raised);
}

export class TuringClient {
    // module: an instantiated wasm decoder module (await createTuringDecoderModule()).
    // options: { url, graph, authToken, bufferCapacity, fetch }.
    constructor(module, options = {}) {
        this._module = module;
        this._url = options.url ?? "/query";
        this._graph = options.graph ?? "default";
        this._change = "head";
        this._commit = "head";
        this._authToken = options.authToken ?? null;
        this._bufferCapacity = options.bufferCapacity ?? 1 << 20;
        this._fetch = options.fetch ?? ((...args) => fetch(...args));
    }

    setGraph(name) {
        this._graph = name;
    }

    getGraph() {
        return this._graph;
    }

    setChange(change) {
        this._change = normalizeRef(change);
    }

    setCommit(commit) {
        this._commit = normalizeRef(commit);
    }

    setAuthToken(token) {
        this._authToken = token;
    }

    // Runs one Cypher query. Resolves to { chunks, execTimeMs } where chunks holds one
    // entry per streamed dataframe: { names, columns }, every column a Column.
    // options: { graph, change, commit, signal } override the client state for this call.
    async query(cypher, options = {}) {
        const graph = options.graph ?? this._graph;
        const change = options.change !== undefined ? normalizeRef(options.change) : this._change;
        const commit = options.commit !== undefined ? normalizeRef(options.commit) : this._commit;

        const url = `${this._url}?graph=${encodeURIComponent(graph)}`
            + `&commit=${encodeURIComponent(commit)}&change=${encodeURIComponent(change)}`;

        const headers = { "Content-Type": "application/turing-proto" };
        if (this._authToken) {
            headers["Authorization"] = `Bearer ${this._authToken}`;
        }

        const response = await this._fetch(url, {
            method: "POST",
            headers,
            body: cypher,
            signal: options.signal,
        });
        if (!response.ok) {
            throw new TuringQueryError("HTTP_ERROR", `Server returned HTTP ${response.status}`);
        }

        const decoder = new this._module.TuringDecoder(this._bufferCapacity);
        try {
            return await this._decodeResponse(decoder, response.body.getReader());
        } finally {
            decoder.delete();
        }
    }

    // Drop-in shape for the visualizer's REST client: chunks x columns x row values,
    // all 64-bit integers as Number — matching what JSON gave it (values past 2^53 lose
    // precision exactly as JSON does; use query() to keep BigInt).
    async queryData(cypher, options = {}) {
        const { chunks } = await this.query(cypher, options);
        return chunks.map((chunk) => chunk.columns.map((column) => column.toNumberArray()));
    }

    async _decodeResponse(decoder, reader) {
        const assembler = new PacketAssembler();
        const chunks = [];
        let sawEndChunk = false;
        let sawEnd = false;
        let execTimeMs = null;
        let error = null;

        while (true) {
            const packet = assembler.nextPacket();
            if (packet === null) {
                const { value, done } = await reader.read();
                if (done) {
                    break;
                }
                assembler.push(value);
                continue;
            }

            if (sawEnd) {
                throw new TuringQueryError("PROTOCOL_ERROR", "Unexpected packet after END");
            }

            const type = packet[0];
            if (type === MESSAGE_CHUNK_HEADER || type === MESSAGE_CHUNK) {
                this._decodePacket(decoder, packet);
            } else if (type === MESSAGE_END_CHUNK) {
                // One dataframe is complete: read it out, then drop its rows while
                // keeping the columns — the server sends one CHUNK_HEADER per response
                // and streams every dataframe through the same columns.
                chunks.push(this._readDataframe(decoder));
                decoder.endChunk();
                sawEndChunk = true;
            } else if (type === MESSAGE_END) {
                if (!sawEndChunk) {
                    chunks.push(this._readDataframe(decoder));
                }
                execTimeMs = readExecTime(packet);
                sawEnd = true;
            } else if (type === MESSAGE_ERROR) {
                error = readError(packet);
            } else if (type === MESSAGE_PROTOCOL_ERROR) {
                throw new TuringQueryError("PROTOCOL_ERROR", utf8Decoder.decode(packet.subarray(PROTO_HEADER_SIZE)));
            } else {
                throw new TuringQueryError("PROTOCOL_ERROR", `Unknown packet type ${type}`);
            }
        }

        if (error !== null) {
            throw new TuringQueryError(error.status, error.message);
        }
        if (!sawEnd) {
            throw new TuringQueryError("PROTOCOL_ERROR", "Response ended before the END packet");
        }

        return { chunks, execTimeMs };
    }

    _decodePacket(decoder, packet) {
        try {
            decoder.decodePacket(packet);
        } catch (raised) {
            throw new TuringQueryError("DECODE_ERROR", describeWasmError(this._module, raised));
        }
    }

    _readDataframe(decoder) {
        try {
            return readDataframe(decoder);
        } catch (raised) {
            if (raised instanceof TuringQueryError) {
                throw raised;
            }
            throw new TuringQueryError("DECODE_ERROR", describeWasmError(this._module, raised));
        }
    }
}
