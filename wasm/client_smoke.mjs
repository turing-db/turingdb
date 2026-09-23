// Node smoke test for the wasm TuringClient: serves hand-crafted wire packets through
// a fake fetch (sliced into small stream chunks to exercise packet reassembly) and
// checks the decoded JS values, list columns and error handling included.
// Run from wasm/: node client_smoke.mjs

import assert from "assert";
import { createRequire } from "module";

import { Column, TuringClient, TuringQueryError } from "./TuringClient.mjs";

const require = createRequire(import.meta.url);
const createTuringDecoderModule = require("./build/turing_wasm_decoder.js");

const MESSAGE_CHUNK_HEADER = 0;
const MESSAGE_CHUNK = 1;
const MESSAGE_END_CHUNK = 2;
const MESSAGE_END = 3;
const MESSAGE_ERROR = 4;

const TYPE_UINT64 = 0;
const TYPE_DOUBLE = 2;
const TYPE_STRING = 3;
const TYPE_ENTITY_LIST = 7;
const TYPE_LIST_VIEW = 8;
const TYPE_LIST_ELEMENT_VIEW = 10;
const TYPE_NODE_ID = 11;
const TYPE_MAP_VIEW = 20;

const ENCODING_VECTOR = 0;
const ENCODING_OPTIONAL_VECTOR = 1;
const ENCODING_CONSTANT = 2;

const TAG_INT = 0;
const TAG_BOOL = 3;
const TAG_STRING = 4;
const TAG_LIST_VIEW = 6;
const TAG_MAP_VIEW = 11;

// A map value carries MapBufferTypeTag, whose ordinals differ from the list tags past 6
const MAP_TAG_INT = 0;
const MAP_TAG_DOUBLE = 2;
const MAP_TAG_STRING = 4;
const MAP_TAG_LIST_VIEW = 6;
const MAP_TAG_MAP_VIEW = 7;
const MAP_TAG_NULL = 8;
const MAP_TAG_NODE_ID = 9;

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

// Fake fetch: streams the response bytes in small slices so packets straddle reads.
function makeFakeFetch(bytes, captured) {
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
        return { ok: true, status: 200, body: stream };
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
        { name: "row", typeCode: TYPE_LIST_ELEMENT_VIEW, encoding: ENCODING_VECTOR },
        { name: "tags", typeCode: TYPE_MAP_VIEW, encoding: ENCODING_CONSTANT },
    ]);

    const chunk = new Writer();

    // person: {age: 32, name: "remy"}, {}, {id: node 9, inner: {a: 1}, xs: [true]}
    chunk.u32(3);

    chunk.u32(2);
    chunk.u32(24);
    chunk.splitString("age", 1);
    chunk.u8(MAP_TAG_INT);
    chunk.i64(32n);
    chunk.string("name");
    chunk.mark();
    chunk.u8(MAP_TAG_STRING);
    chunk.splitString("remy", 2);

    chunk.u32(0);
    chunk.u32(0);

    chunk.u32(3);
    chunk.u32(40);
    chunk.mark();
    chunk.string("id");
    chunk.u8(MAP_TAG_NODE_ID);
    chunk.u64(9n);
    chunk.string("inner");
    chunk.u8(MAP_TAG_MAP_VIEW);
    chunk.u32(1);
    chunk.u32(8);
    chunk.string("a");
    chunk.mark();
    chunk.u8(MAP_TAG_INT);
    chunk.i64(1n);
    chunk.string("xs");
    chunk.u8(MAP_TAG_LIST_VIEW);
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
    chunk.u8(MAP_TAG_INT);
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
    chunk.u8(MAP_TAG_INT);
    chunk.i64(5n);

    // row: one element per row - {b: 2}, 7, {c: null} - the path that records each row's view
    chunk.u32(3);
    chunk.u32(40);
    chunk.u8(TAG_MAP_VIEW);
    chunk.u32(1);
    chunk.u32(8);
    chunk.string("b");
    chunk.u8(MAP_TAG_INT);
    chunk.i64(2n);
    chunk.mark();
    chunk.u8(TAG_INT);
    chunk.i64(7n);
    chunk.u8(TAG_MAP_VIEW);
    chunk.u32(1);
    chunk.u32(1);
    chunk.string("c");
    chunk.u8(MAP_TAG_NULL);
    chunk.u8(0);

    // tags: {k: "v", n: 1.5} - a constant has no row count
    chunk.u32(2);
    chunk.u32(24);
    chunk.string("k");
    chunk.u8(MAP_TAG_STRING);
    chunk.string("v");
    chunk.string("n");
    chunk.u8(MAP_TAG_DOUBLE);
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

async function testQuery(module) {
    const bytes = concatPackets([
        ...makeFirstDataframe(),
        makeEndChunk(3),
        ...makeSecondDataframe(),
        makeEndChunk(2),
        makeEnd(12.5),
    ]);

    const captured = {};
    const client = new TuringClient(module, {
        url: "/api/query",
        graph: "simpledb",
        authToken: "secret",
        fetch: makeFakeFetch(bytes, captured),
    });

    const { chunks, execTimeMs } = await client.query("MATCH (n) RETURN n");

    assert.strictEqual(captured.url, "/api/query?graph=simpledb&commit=head&change=head");
    assert.strictEqual(captured.init.method, "POST");
    assert.strictEqual(captured.init.body, "MATCH (n) RETURN n");
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

async function testQueryData(module) {
    const bytes = concatPackets([
        ...makeFirstDataframe(),
        makeEndChunk(3),
        makeEnd(1.0),
    ]);

    const client = new TuringClient(module, { fetch: makeFakeFetch(bytes, {}) });
    const data = await client.queryData("MATCH (n) RETURN n");

    assert.deepStrictEqual(data, [[
        [1, 2, 3],
        [["remy", "adam"], [], [7, true, ["a"]]],
        [1.5, 1.5, 1.5],
        ["r", null, "c"],
    ]]);
}

async function testError(module) {
    const error = new Writer();
    error.u8(2); // PARSE_ERROR
    error.raw(encoder.encode("boom"));
    const bytes = concatPackets([frame(MESSAGE_ERROR, error), makeEnd(0.5)]);

    const client = new TuringClient(module, { fetch: makeFakeFetch(bytes, {}) });

    await assert.rejects(client.query("MATCH ("), (raised) => {
        assert.ok(raised instanceof TuringQueryError);
        assert.strictEqual(raised.status, "PARSE_ERROR");
        assert.strictEqual(raised.message, "boom");
        return true;
    });
}

async function testMaps(module, split) {
    const bytes = concatPackets([...makeMapDataframe(split), makeEndChunk(3), makeEnd(1.0)]);

    const client = new TuringClient(module, { fetch: makeFakeFetch(bytes, {}) });
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

    const data = await new TuringClient(module, { fetch: makeFakeFetch(bytes, {}) }).queryData("RETURN {a: 1}");
    assert.deepStrictEqual(data[0][0], [{ age: 32, name: "remy" }, {}, { id: 9, inner: { a: 1 }, xs: [true] }]);
}

// A column may carry fewer items than the dataframe has rows; queryData reads the missing
// rows as undefined rather than failing, map columns included.
async function testRaggedQueryData(module) {
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
    chunk.u8(MAP_TAG_INT);
    chunk.i64(1n);

    chunk.u32(3);
    chunk.u64(1n);
    chunk.u64(2n);
    chunk.u64(3n);

    const bytes = concatPackets([frame(MESSAGE_CHUNK_HEADER, header), frame(MESSAGE_CHUNK, chunk), makeEndChunk(3), makeEnd(1.0)]);
    const data = await new TuringClient(module, { fetch: makeFakeFetch(bytes, {}) }).queryData("RETURN 1");

    assert.deepStrictEqual(data, [[
        ["a", undefined, undefined],
        [{ k: 1 }, undefined, undefined],
        [1, 2, 3],
    ]]);
}

// An entity list's entries are plain objects, but not maps: queryData keeps their ids BigInt,
// so an id above 2^53 survives exactly.
async function testEntityListIdsStayExact(module) {
    const header = makeHeader([{ name: "path", typeCode: TYPE_ENTITY_LIST, encoding: ENCODING_VECTOR }]);

    const chunk = new Writer();
    chunk.u32(1);
    chunk.u32(1);
    chunk.u8(0);
    chunk.u64(9007199254740993n);

    const bytes = concatPackets([frame(MESSAGE_CHUNK_HEADER, header), frame(MESSAGE_CHUNK, chunk), makeEndChunk(1), makeEnd(1.0)]);
    const data = await new TuringClient(module, { fetch: makeFakeFetch(bytes, {}) }).queryData("RETURN 1");

    assert.deepStrictEqual(data, [[[[{ type: 0, id: 9007199254740993n }]]]]);
}

createTuringDecoderModule().then(async (module) => {
    await testQuery(module);
    await testQueryData(module);
    await testError(module);
    await testMaps(module, false);
    await testMaps(module, true);
    await testRaggedQueryData(module);
    await testEntityListIdsStayExact(module);
    console.log("wasm client smoke test passed: query, queryData, error, maps, ragged, entity lists");
});
