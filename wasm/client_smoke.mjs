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
const TYPE_LIST_VIEW = 8;
const TYPE_NODE_ID = 11;

const ENCODING_VECTOR = 0;
const ENCODING_OPTIONAL_VECTOR = 1;
const ENCODING_CONSTANT = 2;

const TAG_INT = 0;
const TAG_BOOL = 3;
const TAG_STRING = 4;
const TAG_LIST_VIEW = 6;

const encoder = new TextEncoder();

class Writer {
    constructor() {
        this.bytes = [];
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

function makeEnd(execTimeMs) {
    const end = new Writer();
    end.f32(execTimeMs);
    return frame(MESSAGE_END, end);
}

async function testQuery(module) {
    const bytes = concatPackets([
        ...makeFirstDataframe(),
        frame(MESSAGE_END_CHUNK, new Writer()),
        ...makeSecondDataframe(),
        frame(MESSAGE_END_CHUNK, new Writer()),
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
        frame(MESSAGE_END_CHUNK, new Writer()),
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

createTuringDecoderModule().then(async (module) => {
    await testQuery(module);
    await testQueryData(module);
    await testError(module);
    console.log("wasm client smoke test passed: query, queryData, error");
});
