// Copyright Turing Biosystems Ltd
// SPDX-License-Identifier: Apache-2.0

// Node smoke test for the wasm TuringClient: serves hand-crafted wire packets through
// a fake fetch (sliced into small stream chunks to exercise packet reassembly) and
// checks the decoded JS values, list columns and error handling included.
// Run from wasm/npm/: node client_smoke.mjs

import assert from "assert";

import { Column, Point, TuringClient, TuringQueryError, double } from "./TuringClient.mjs";

const MESSAGE_CHUNK_HEADER = 0;
const MESSAGE_CHUNK = 1;
const MESSAGE_END_CHUNK = 2;
const MESSAGE_END = 3;
const MESSAGE_ERROR = 4;
const MESSAGE_PROTOCOL_ERROR = 5;

const TYPE_UINT64 = 0;
const TYPE_DOUBLE = 2;
const TYPE_STRING = 3;
const TYPE_LIST_VIEW = 6;
const TYPE_CELL_VIEW = 8;
const TYPE_NODE_ID = 9;
const TYPE_PROPERTY_NULL = 17;
const TYPE_MAP_VIEW = 18;
const TYPE_GRAPH_PATH = 21;
const TYPE_POINT = 22;

const ENCODING_VECTOR = 0;
const ENCODING_OPTIONAL_VECTOR = 1;
const ENCODING_CONSTANT = 2;
const ENCODING_OPTIONAL_CONSTANT = 3;

const TAG_INT = 0;
const TAG_DOUBLE = 2;
const TAG_BOOL = 3;
const TAG_STRING = 4;
const TAG_EMBEDDING = 5;
const TAG_LIST_VIEW = 6;
const TAG_NULL = 7;
const TAG_NODE_ID = 8;
const TAG_MAP_VIEW = 11;
const TAG_POINT = 14;

const CRS_CARTESIAN = 0;
const CRS_WGS84_3D = 3;

const encoder = new TextEncoder();

class Writer {
    constructor() {
        this.bytes = [];
        this.marks = [];
    }
    // A place the payload may be cut into another CHUNK packet
    mark() {
        this.marks.push(this.bytes.length);
    }
    // A string whose bytes are cut after @param at, as the server streams a long one
    splitString(text, at) {
        const bytes = encoder.encode(text);
        this.u32(bytes.length);
        this.raw(bytes.slice(0, at));
        this.mark();
        this.raw(bytes.slice(at));
    }
    u8(value) {
        this.bytes.push(value & 0xff);
    }
    u32(value) {
        const view = new DataView(new ArrayBuffer(4));
        view.setUint32(0, value, true);
        this.bytes.push(...new Uint8Array(view.buffer));
    }
    u64(value) {
        const view = new DataView(new ArrayBuffer(8));
        view.setBigUint64(0, BigInt(value), true);
        this.bytes.push(...new Uint8Array(view.buffer));
    }
    i64(value) {
        const view = new DataView(new ArrayBuffer(8));
        view.setBigInt64(0, BigInt(value), true);
        this.bytes.push(...new Uint8Array(view.buffer));
    }
    f32(value) {
        const view = new DataView(new ArrayBuffer(4));
        view.setFloat32(0, value, true);
        this.bytes.push(...new Uint8Array(view.buffer));
    }
    f64(value) {
        const view = new DataView(new ArrayBuffer(8));
        view.setFloat64(0, value, true);
        this.bytes.push(...new Uint8Array(view.buffer));
    }
    string(text) {
        const bytes = encoder.encode(text);
        this.u32(bytes.length);
        this.raw(bytes);
    }
    raw(byteArray) {
        this.bytes.push(...byteArray);
    }
    // A point as the server stores it: x, y and z, then its CRS ordinal and 7 bytes of padding
    point(crs, x, y, z) {
        this.f64(x);
        this.f64(y);
        this.f64(z);
        this.u8(crs);
        this.raw(new Uint8Array(7));
    }
    toUint8Array() {
        return Uint8Array.from(this.bytes);
    }
}

function frame(type, payloadWriter) {
    const payload = payloadWriter.toUint8Array();
    const packet = new Writer();
    packet.u8(type);
    packet.u32(payload.length);
    packet.raw(payload);
    return packet.toUint8Array();
}

function concatPackets(packets) {
    const total = packets.reduce((sum, packet) => sum + packet.length, 0);
    const out = new Uint8Array(total);
    let offset = 0;
    for (const packet of packets) {
        out.set(packet, offset);
        offset += packet.length;
    }
    return out;
}

// The framed request body the server reads, built byte by byte apart from the client.
function requestBody(cypher, parameters = null) {
    const query = encoder.encode(cypher);
    const body = new Writer();
    body.u32(query.length);
    body.raw(query);

    if (parameters === null) {
        body.u32(0);
    } else {
        const parameterBytes = parameters.toUint8Array();
        body.u32(parameterBytes.length);
        body.raw(parameterBytes);
    }

    return body.toUint8Array();
}

// Fake fetch: streams the response bytes in small slices so packets straddle reads.
function makeFakeFetch(bytes, captured, status = 200, contentType = "application/turing-proto") {
    return async (url, init) => {
        captured.url = url;
        captured.init = init;
        const stream = new ReadableStream({
            start(controller) {
                const sliceSize = 7;
                for (let offset = 0; offset < bytes.length; offset += sliceSize) {
                    controller.enqueue(bytes.subarray(offset, Math.min(offset + sliceSize, bytes.length)));
                }
                controller.close();
            },
        });
        const headers = new Headers({ "Content-Type": contentType });
        return { ok: status === 200, status, headers, body: stream };
    };
}

// CHUNK_HEADER payload: [colCount] then per column [nameLen][typeCode][encoding][name].
function makeHeader(columns) {
    const header = new Writer();
    header.u32(columns.length);
    for (const column of columns) {
        const nameBytes = encoder.encode(column.name);
        header.u32(nameBytes.length);
        header.u32(column.typeCode);
        header.u8(column.encoding);
        header.raw(nameBytes);
    }
    return header;
}

// First dataframe: a NODE_ID vector, a LIST_VIEW vector (strings, an empty list, and a
// mixed list with a nested list), a DOUBLE constant and an optional STRING vector.
function makeFirstDataframe() {
    const header = makeHeader([
        { name: "ids", typeCode: TYPE_NODE_ID, encoding: ENCODING_VECTOR },
        { name: "labels", typeCode: TYPE_LIST_VIEW, encoding: ENCODING_VECTOR },
        { name: "score", typeCode: TYPE_DOUBLE, encoding: ENCODING_CONSTANT },
        { name: "nick", typeCode: TYPE_STRING, encoding: ENCODING_OPTIONAL_VECTOR },
    ]);

    const chunk = new Writer();

    // ids: [rowCount][u64 * rows]
    chunk.u32(3);
    chunk.u64(1n);
    chunk.u64(2n);
    chunk.u64(3n);

    // labels: [rowCount] then per row [elementCount][listByteSize] + elements.
    // listByteSize mirrors the encoder: sum of sizeof of each element's stored value
    // object on the server's layout — 16 per string/embedding/nested-list view, 8 per
    // 64-bit numeric, 1 per bool. The decoder reserves this footprint up front.
    chunk.u32(3);

    // Row 0: ["remy", "adam"]
    chunk.u32(2);
    chunk.u32(32);
    chunk.u8(TAG_STRING);
    chunk.string("remy");
    chunk.u8(TAG_STRING);
    chunk.string("adam");

    // Row 1: []
    chunk.u32(0);
    chunk.u32(0);

    // Row 2: [7, true, ["a"]]
    chunk.u32(3);
    chunk.u32(25);
    chunk.u8(TAG_INT);
    chunk.i64(7n);
    chunk.u8(TAG_BOOL);
    chunk.u8(1);
    chunk.u8(TAG_LIST_VIEW);
    chunk.u32(1); // nested element count
    chunk.u32(16); // nested byte size
    chunk.u8(TAG_STRING);
    chunk.string("a");

    // score: double constant — just the value
    chunk.f64(1.5);

    // nick: ["r", null, "c"] — [rowCount][null bitmask] then per row [len][bytes], a
    // null row keeping its (zero) length field.
    chunk.u32(3);
    chunk.u64(0b101n);
    chunk.string("r");
    chunk.u32(0);
    chunk.string("c");

    return [frame(MESSAGE_CHUNK_HEADER, header), frame(MESSAGE_CHUNK, chunk)];
}

// Second dataframe: the SAME columns get another batch of rows — the server sends one
// CHUNK_HEADER per response, so a later dataframe is just another CHUNK.
function makeSecondDataframe() {
    const chunk = new Writer();

    // ids
    chunk.u32(2);
    chunk.u64(4n);
    chunk.u64(5n);

    // labels: [["x"], []]
    chunk.u32(2);
    chunk.u32(1);
    chunk.u32(16);
    chunk.u8(TAG_STRING);
    chunk.string("x");
    chunk.u32(0);
    chunk.u32(0);

    // score
    chunk.f64(2.5);

    // nick: [null, "y"]
    chunk.u32(2);
    chunk.u64(0b10n);
    chunk.u32(0);
    chunk.string("y");

    return [frame(MESSAGE_CHUNK, chunk)];
}

// Map columns, and maps nested in lists and lists in maps. A map row is [entryCount]
// [mapByteSize] then entries [keyLen][key][tag][value]; mapByteSize is the server-side sum of
// sizeof over the values, which the wasm sink does not need but the wire always carries.
// One CHUNK packet, or one per stretch between the writer's marks - the server cuts a
// dataframe at its chunk size, so the decoder has to resume wherever a mark falls.
function chunkFrames(chunk, split) {
    if (!split) {
        return [frame(MESSAGE_CHUNK, chunk)];
    }

    const bounds = [0, ...chunk.marks, chunk.bytes.length];
    const frames = [];
    for (let index = 0; index + 1 < bounds.length; index++) {
        const part = new Writer();
        part.raw(chunk.bytes.slice(bounds[index], bounds[index + 1]));
        frames.push(frame(MESSAGE_CHUNK, part));
    }
    return frames;
}

function makeMapDataframe(split) {
    const header = makeHeader([
        { name: "person", typeCode: TYPE_MAP_VIEW, encoding: ENCODING_VECTOR },
        { name: "mixed", typeCode: TYPE_LIST_VIEW, encoding: ENCODING_VECTOR },
        { name: "row", typeCode: TYPE_CELL_VIEW, encoding: ENCODING_VECTOR },
        { name: "tags", typeCode: TYPE_MAP_VIEW, encoding: ENCODING_CONSTANT },
    ]);

    const chunk = new Writer();

    // person: {age: 32, name: "remy"}, {}, {id: node 9, inner: {a: 1}, xs: [true]}
    chunk.u32(3);

    chunk.u32(2);
    chunk.u32(24);
    chunk.splitString("age", 1);
    chunk.u8(TAG_INT);
    chunk.i64(32n);
    chunk.string("name");
    chunk.mark();
    chunk.u8(TAG_STRING);
    chunk.splitString("remy", 2);

    chunk.u32(0);
    chunk.u32(0);

    chunk.u32(3);
    chunk.u32(40);
    chunk.mark();
    chunk.string("id");
    chunk.u8(TAG_NODE_ID);
    chunk.u64(9n);
    chunk.string("inner");
    chunk.u8(TAG_MAP_VIEW);
    chunk.u32(1);
    chunk.u32(8);
    chunk.string("a");
    chunk.mark();
    chunk.u8(TAG_INT);
    chunk.i64(1n);
    chunk.string("xs");
    chunk.u8(TAG_LIST_VIEW);
    chunk.u32(1);
    chunk.u32(1);
    chunk.u8(TAG_BOOL);
    chunk.u8(1);

    // mixed: [{a: 1}, 7], [], [{"__proto__": 5}]
    chunk.u32(3);

    chunk.u32(2);
    chunk.u32(24);
    chunk.u8(TAG_MAP_VIEW);
    chunk.u32(1);
    chunk.u32(8);
    chunk.string("a");
    chunk.u8(TAG_INT);
    chunk.i64(1n);
    chunk.mark();
    chunk.u8(TAG_INT);
    chunk.i64(7n);

    chunk.u32(0);
    chunk.u32(0);

    chunk.u32(1);
    chunk.u32(16);
    chunk.u8(TAG_MAP_VIEW);
    chunk.u32(1);
    chunk.u32(8);
    chunk.string("__proto__");
    chunk.u8(TAG_INT);
    chunk.i64(5n);

    // row: one element per row - {b: 2}, 7, {c: null} - the path that records each row's view
    chunk.u32(3);
    chunk.u32(40);
    chunk.u8(TAG_MAP_VIEW);
    chunk.u32(1);
    chunk.u32(8);
    chunk.string("b");
    chunk.u8(TAG_INT);
    chunk.i64(2n);
    chunk.mark();
    chunk.u8(TAG_INT);
    chunk.i64(7n);
    chunk.u8(TAG_MAP_VIEW);
    chunk.u32(1);
    chunk.u32(1);
    chunk.string("c");
    chunk.u8(TAG_NULL);
    chunk.u8(0);

    // tags: {k: "v", n: 1.5} - a constant has no row count
    chunk.u32(2);
    chunk.u32(24);
    chunk.string("k");
    chunk.u8(TAG_STRING);
    chunk.string("v");
    chunk.string("n");
    chunk.u8(TAG_DOUBLE);
    chunk.f64(1.5);

    return [frame(MESSAGE_CHUNK_HEADER, header), ...chunkFrames(chunk, split)];
}

function makeEnd(execTimeMs) {
    const end = new Writer();
    end.f32(execTimeMs);
    return frame(MESSAGE_END, end);
}

// A chunk closes with its row count: a constant column holds one value however many rows
// it stands for, so nothing else on the wire says how many there are.
function makeEndChunk(rowCount) {
    const footer = new Writer();
    footer.u32(rowCount);
    return frame(MESSAGE_END_CHUNK, footer);
}

async function testQuery() {
    const bytes = concatPackets([
        ...makeFirstDataframe(),
        makeEndChunk(3),
        ...makeSecondDataframe(),
        makeEndChunk(2),
        makeEnd(12.5),
    ]);

    const captured = {};
    const client = new TuringClient({
        url: "/api/query",
        graph: "simpledb",
        authToken: "secret",
        fetch: makeFakeFetch(bytes, captured),
    });

    const { chunks, execTimeMs } = await client.query("MATCH (n) RETURN n");

    assert.strictEqual(captured.url, "/api/query?graph=simpledb&commit=head&change=head");
    assert.strictEqual(captured.init.method, "POST");
    assert.deepStrictEqual(captured.init.body, requestBody("MATCH (n) RETURN n"));
    assert.strictEqual(captured.init.headers["Content-Type"], "application/turing-proto");
    assert.strictEqual(captured.init.headers["Authorization"], "Bearer secret");

    assert.strictEqual(execTimeMs, 12.5);
    assert.strictEqual(chunks.length, 2);

    const [ids, labels, score, nick] = chunks[0].columns;
    assert.deepStrictEqual(chunks[0].names, ["ids", "labels", "score", "nick"]);
    assert.ok(ids instanceof Column);
    assert.strictEqual(ids.typeCode, TYPE_NODE_ID);
    assert.strictEqual(ids.length, 3);
    assert.deepStrictEqual(ids.toArray(), [1n, 2n, 3n]);
    assert.strictEqual(ids.get(2), 3n);
    assert.deepStrictEqual(labels.toArray(), [["remy", "adam"], [], [7n, true, ["a"]]]);
    assert.deepStrictEqual(labels.get(2), [7n, true, ["a"]]);
    assert.ok(score.isConstant);
    assert.strictEqual(score.get(1), 1.5);
    assert.deepStrictEqual(score.toArray(), [1.5, 1.5, 1.5]);
    assert.ok(nick.isOptional);
    assert.deepStrictEqual(nick.toArray(), ["r", null, "c"]);
    assert.strictEqual(nick.get(1), null);

    assert.deepStrictEqual(chunks[1].names, ["ids", "labels", "score", "nick"]);
    assert.deepStrictEqual(chunks[1].columns[0].toArray(), [4n, 5n]);
    assert.deepStrictEqual(chunks[1].columns[1].toArray(), [["x"], []]);
    assert.deepStrictEqual(chunks[1].columns[2].toArray(), [2.5, 2.5]);
    assert.deepStrictEqual(chunks[1].columns[3].toArray(), [null, "y"]);
}

// The query length counts UTF-8 bytes, not UTF-16 code units: "é" is 2 bytes, the emoji 4.
async function testMultiByteQueryBody() {
    const captured = {};
    const bytes = concatPackets([makeEnd(1.0)]);
    await new TuringClient({ fetch: makeFakeFetch(bytes, captured) }).query("RETURN 'é😀'");

    const body = captured.init.body;
    const queryLength = new DataView(body.buffer, body.byteOffset).getUint32(0, true);
    assert.strictEqual(queryLength, 15);
    assert.deepStrictEqual(body, requestBody("RETURN 'é😀'"));
}

// A map's keys go sorted, each value as [tag][payload], and a list or map as its count and
// a byte size of 0, which the server's parameter reader does not read
async function testParameters() {
    const captured = {};
    const client = new TuringClient({ fetch: makeFakeFetch(makeEnd(1.0), captured) });
    const parameters = {
        window: [470, 500.5],
        name: "Oxford",
        open: true,
        none: null,
        big: 2n ** 62n,
        ratio: double(2),
        stop: { rank: -1, crs: "PAD" },
        profile: new Float32Array([1, 0.5]),
    };
    await client.query("RETURN $window", { parameters });

    const map = new Writer();
    map.u32(8);
    map.u32(0);
    map.string("big");
    map.u8(TAG_INT);
    map.i64(2n ** 62n);
    map.string("name");
    map.u8(TAG_STRING);
    map.string("Oxford");
    map.string("none");
    map.u8(TAG_NULL);
    map.u8(0);
    map.string("open");
    map.u8(TAG_BOOL);
    map.u8(1);
    map.string("profile");
    map.u8(TAG_EMBEDDING);
    map.u32(8);
    map.f32(1);
    map.f32(0.5);
    map.string("ratio");
    map.u8(TAG_DOUBLE);
    map.f64(2);
    map.string("stop");
    map.u8(TAG_MAP_VIEW);
    map.u32(2);
    map.u32(0);
    map.string("crs");
    map.u8(TAG_STRING);
    map.string("PAD");
    map.string("rank");
    map.u8(TAG_INT);
    map.i64(-1n);
    map.string("window");
    map.u8(TAG_LIST_VIEW);
    map.u32(2);
    map.u32(0);
    map.u8(TAG_INT);
    map.i64(470n);
    map.u8(TAG_DOUBLE);
    map.f64(500.5);

    assert.deepStrictEqual(captured.init.body, requestBody("RETURN $window", map));
}

async function testRejectsParametersAQueryCannotHold() {
    const client = new TuringClient({ fetch: makeFakeFetch(makeEnd(1.0), {}) });
    const rejected = [
        [{ when: new Date(0) }, "$when"],
        [{ profile: new Float64Array(2) }, "$profile"],
        [{ stop: { crs: undefined } }, "$stop.crs"],
        [{ calls: [1, [2, Symbol("x")]] }, "$calls[1][1]"],
        [{ big: 2n ** 63n }, "$big"],
    ];

    for (const [parameters, path] of rejected) {
        await assert.rejects(client.query("RETURN 1", { parameters }), (raised) => {
            assert.ok(raised instanceof TuringQueryError);
            assert.strictEqual(raised.status, "PARAMETER_ERROR");
            assert.ok(raised.message.includes(path), raised.message);
            return true;
        });
    }

    assert.throws(() => double("2"), TypeError);
    assert.throws(() => double(2n), TypeError);
}

// The decoder's buffer is only where it starts: a packet larger than it grows it
async function testGrowsTheDecoderBuffer() {
    const bytes = concatPackets([...makeFirstDataframe(), makeEndChunk(3), makeEnd(1.0)]);
    const client = new TuringClient({ fetch: makeFakeFetch(bytes, {}), bufferCapacity: 8 });
    const { chunks } = await client.query("MATCH (n) RETURN n");

    assert.deepStrictEqual(chunks[0].names, ["ids", "labels", "score", "nick"]);
    assert.deepStrictEqual(chunks[0].columns[3].toArray(), ["r", null, "c"]);
}

// A constant null has no bytes, so no CHUNK packet carries it: each END_CHUNK completes it
async function testConstantNullInEveryChunk() {
    const header = makeHeader([{ name: "absent", typeCode: TYPE_PROPERTY_NULL, encoding: ENCODING_CONSTANT }]);
    const bytes = concatPackets([frame(MESSAGE_CHUNK_HEADER, header), makeEndChunk(2), makeEndChunk(3), makeEnd(1.0)]);

    const { chunks } = await new TuringClient({ fetch: makeFakeFetch(bytes, {}) }).query("RETURN null");

    assert.deepStrictEqual(chunks.map((chunk) => chunk.columns[0].toArray()), [[null, null], [null, null, null]]);
}

// What a slice of a constant list comes out as: a list, or no list
async function testOptionalConstantLists() {
    const header = makeHeader([
        { name: "slice", typeCode: TYPE_LIST_VIEW, encoding: ENCODING_OPTIONAL_CONSTANT },
        { name: "none", typeCode: TYPE_LIST_VIEW, encoding: ENCODING_OPTIONAL_CONSTANT },
    ]);

    const chunk = new Writer();
    chunk.u8(1);
    chunk.u32(2);
    chunk.u32(16);
    chunk.u8(TAG_INT);
    chunk.i64(8n);
    chunk.u8(TAG_INT);
    chunk.i64(9n);
    chunk.u8(0);

    const bytes = concatPackets([frame(MESSAGE_CHUNK_HEADER, header), frame(MESSAGE_CHUNK, chunk), makeEndChunk(2), makeEnd(1.0)]);
    const { chunks } = await new TuringClient({ fetch: makeFakeFetch(bytes, {}) }).query("RETURN [7, 8, 9][1..]");
    const [slice, none] = chunks[0].columns;

    assert.deepStrictEqual(slice.toArray(), [[8n, 9n], [8n, 9n]]);
    assert.deepStrictEqual(none.toArray(), [null, null]);
}

async function testQueryData() {
    const bytes = concatPackets([
        ...makeFirstDataframe(),
        makeEndChunk(3),
        makeEnd(1.0),
    ]);

    const client = new TuringClient({ fetch: makeFakeFetch(bytes, {}) });
    const data = await client.queryData("MATCH (n) RETURN n");

    assert.deepStrictEqual(data, [[
        [1, 2, 3],
        [["remy", "adam"], [], [7, true, ["a"]]],
        [1.5, 1.5, 1.5],
        ["r", null, "c"],
    ]]);
}

async function testError() {
    const error = new Writer();
    error.u8(2); // PARSE_ERROR
    error.raw(encoder.encode("boom"));
    const bytes = concatPackets([frame(MESSAGE_ERROR, error), makeEnd(0.5)]);

    const client = new TuringClient({ fetch: makeFakeFetch(bytes, {}) });

    await assert.rejects(client.query("MATCH ("), (raised) => {
        assert.ok(raised instanceof TuringQueryError);
        assert.strictEqual(raised.status, "PARSE_ERROR");
        assert.strictEqual(raised.message, "boom");
        return true;
    });
}

// A bad request gets its own status with a PROTOCOL_ERROR packet as the body.
async function testRejectedRequest() {
    const message = new Writer();
    message.raw(encoder.encode("Unauthorized: missing or invalid authentication token"));
    const bytes = concatPackets([frame(MESSAGE_PROTOCOL_ERROR, message)]);

    const client = new TuringClient({ fetch: makeFakeFetch(bytes, {}, 401) });

    await assert.rejects(client.query("RETURN 1"), (raised) => {
        assert.ok(raised instanceof TuringQueryError);
        assert.strictEqual(raised.status, "PROTOCOL_ERROR");
        assert.strictEqual(raised.message, "Unauthorized: missing or invalid authentication token");
        return true;
    });
}

async function testErrorWithoutPackets() {
    const bytes = encoder.encode("Bad Gateway");
    const client = new TuringClient({ fetch: makeFakeFetch(bytes, {}, 502, "text/plain") });

    await assert.rejects(client.query("RETURN 1"), (raised) => {
        assert.strictEqual(raised.status, "HTTP_ERROR");
        assert.strictEqual(raised.message, "Server returned HTTP 502");
        return true;
    });
}

async function testMaps(split) {
    const bytes = concatPackets([...makeMapDataframe(split), makeEndChunk(3), makeEnd(1.0)]);

    const client = new TuringClient({ fetch: makeFakeFetch(bytes, {}) });
    const { chunks } = await client.query("RETURN {a: 1}");
    const [person, mixed, row, tags] = chunks[0].columns;

    assert.strictEqual(person.typeCode, TYPE_MAP_VIEW);
    assert.deepStrictEqual(person.toArray(), [
        { age: 32n, name: "remy" },
        {},
        { id: 9n, inner: { a: 1n }, xs: [true] },
    ]);
    assert.deepStrictEqual(mixed.toArray(), [
        [{ a: 1n }, 7n],
        [],
        [Object.fromEntries([["__proto__", 5n]])],
    ]);
    assert.deepStrictEqual(row.toArray(), [{ b: 2n }, 7n, { c: null }]);
    assert.ok(tags.isConstant);
    assert.deepStrictEqual(tags.toArray(), [{ k: "v", n: 1.5 }, { k: "v", n: 1.5 }, { k: "v", n: 1.5 }]);

    const data = await new TuringClient({ fetch: makeFakeFetch(bytes, {}) }).queryData("RETURN {a: 1}");
    assert.deepStrictEqual(data[0][0], [{ age: 32, name: "remy" }, {}, { id: 9, inner: { a: 1 }, xs: [true] }]);
}

// A column may carry fewer items than the dataframe has rows; queryData reads the missing
// rows as undefined rather than failing, map columns included.
async function testRaggedQueryData() {
    const header = makeHeader([
        { name: "name", typeCode: TYPE_STRING, encoding: ENCODING_VECTOR },
        { name: "props", typeCode: TYPE_MAP_VIEW, encoding: ENCODING_VECTOR },
        { name: "n", typeCode: TYPE_UINT64, encoding: ENCODING_VECTOR },
    ]);

    const chunk = new Writer();

    chunk.u32(1);
    chunk.string("a");

    chunk.u32(1);
    chunk.u32(1);
    chunk.u32(8);
    chunk.string("k");
    chunk.u8(TAG_INT);
    chunk.i64(1n);

    chunk.u32(3);
    chunk.u64(1n);
    chunk.u64(2n);
    chunk.u64(3n);

    const bytes = concatPackets([frame(MESSAGE_CHUNK_HEADER, header), frame(MESSAGE_CHUNK, chunk), makeEndChunk(3), makeEnd(1.0)]);
    const data = await new TuringClient({ fetch: makeFakeFetch(bytes, {}) }).queryData("RETURN 1");

    assert.deepStrictEqual(data, [[
        ["a", undefined, undefined],
        [{ k: 1 }, undefined, undefined],
        [1, 2, 3],
    ]]);
}

// A path reads as its IDs in the order it runs, node, edge, node. query() keeps them BigInt,
// so an id above 2^53 survives exactly.
async function testGraphPathIdsStayExact() {
    const header = makeHeader([{ name: "path", typeCode: TYPE_GRAPH_PATH, encoding: ENCODING_VECTOR }]);

    const chunk = new Writer();
    chunk.u32(1);
    chunk.u32(2);
    chunk.u64(9007199254740993n);
    chunk.u64(9007199254740995n);
    chunk.u64(5n);

    const bytes = concatPackets([frame(MESSAGE_CHUNK_HEADER, header), frame(MESSAGE_CHUNK, chunk), makeEndChunk(1), makeEnd(1.0)]);
    const { chunks } = await new TuringClient({ fetch: makeFakeFetch(bytes, {}) }).query("RETURN 1");

    assert.deepStrictEqual(chunks[0].columns[0].toArray(), [[9007199254740993n, 9007199254740995n, 5n]]);
}

// The null path an OPTIONAL MATCH leaves arrives as a path of 0 nodes and reads as null,
// as the Python client reads it.
async function testNullGraphPath() {
    const header = makeHeader([{ name: "path", typeCode: TYPE_GRAPH_PATH, encoding: ENCODING_VECTOR }]);

    const chunk = new Writer();
    chunk.u32(2);
    chunk.u32(1);
    chunk.u64(7n);
    chunk.u32(0);

    const bytes = concatPackets([frame(MESSAGE_CHUNK_HEADER, header), frame(MESSAGE_CHUNK, chunk), makeEndChunk(2), makeEnd(1.0)]);
    const data = await new TuringClient({ fetch: makeFakeFetch(bytes, {}) }).queryData("RETURN 1");

    assert.deepStrictEqual(data, [[[[7], null]]]);
}

// A point reads as a Point, in a column of its own and inside a list. A 2D point has no z.
async function testPoints() {
    const header = makeHeader([
        { name: "at", typeCode: TYPE_POINT, encoding: ENCODING_VECTOR },
        { name: "route", typeCode: TYPE_LIST_VIEW, encoding: ENCODING_VECTOR },
    ]);

    const chunk = new Writer();
    chunk.u32(2);
    chunk.point(CRS_CARTESIAN, 1.5, -2, 0);
    chunk.point(CRS_WGS84_3D, 2.3522, 48.8566, 35);

    chunk.u32(2);
    chunk.u32(1);
    chunk.u32(32);
    chunk.u8(TAG_POINT);
    chunk.point(CRS_WGS84_3D, 2.3522, 48.8566, 35);
    chunk.u32(0);
    chunk.u32(0);

    const bytes = concatPackets([frame(MESSAGE_CHUNK_HEADER, header), frame(MESSAGE_CHUNK, chunk), makeEndChunk(2), makeEnd(1.0)]);
    const { chunks } = await new TuringClient({ fetch: makeFakeFetch(bytes, {}) }).query("RETURN 1");
    const [at, route] = chunks[0].columns;

    const cartesian = at.get(0);
    assert.ok(cartesian instanceof Point);
    assert.strictEqual(cartesian.srid, 7203);
    assert.strictEqual(cartesian.crs, "cartesian");
    assert.strictEqual(cartesian.x, 1.5);
    assert.strictEqual(cartesian.y, -2);
    assert.strictEqual(cartesian.z, undefined);

    const paris = at.get(1);
    assert.strictEqual(paris.srid, 4979);
    assert.strictEqual(paris.crs, "wgs-84-3d");
    assert.deepStrictEqual([paris.x, paris.y, paris.z], [2.3522, 48.8566, 35]);

    const [inList] = route.get(0);
    assert.ok(inList instanceof Point);
    assert.deepStrictEqual([inList.srid, inList.x, inList.y, inList.z], [4979, 2.3522, 48.8566, 35]);
    assert.deepStrictEqual(route.get(1), []);

    const data = await new TuringClient({ fetch: makeFakeFetch(bytes, {}) }).queryData("RETURN 1");
    assert.ok(data[0][0][0] instanceof Point);
}

await testQuery();
await testParameters();
await testRejectsParametersAQueryCannotHold();
await testGrowsTheDecoderBuffer();
await testConstantNullInEveryChunk();
await testOptionalConstantLists();
await testQueryData();
await testMultiByteQueryBody();
await testError();
await testRejectedRequest();
await testErrorWithoutPackets();
await testMaps(false);
await testMaps(true);
await testRaggedQueryData();
await testGraphPathIdsStayExact();
await testNullGraphPath();
await testPoints();

console.log("wasm client smoke test passed: query, parameters, buffer growth, constant nulls, optional constant lists, queryData, multi-byte body, error, rejected request, error without packets, maps, ragged, paths, null paths, points");
