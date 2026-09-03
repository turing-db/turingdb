// Node smoke test for the wasm Turing Proto decoder: hand-crafts wire packets
// (CHUNK_HEADER + CHUNK + END_CHUNK) and checks the column buffers handed to JS.
// Run from wasm/: node decoder_smoke.js

const assert = require("assert");
const createTuringDecoderModule = require("./build/turing_wasm_decoder.js");

const MESSAGE_CHUNK_HEADER = 0;
const MESSAGE_CHUNK = 1;
const MESSAGE_END_CHUNK = 2;

const TYPE_UINT64 = 0;
const TYPE_DOUBLE = 2;
const TYPE_STRING = 3;
const TYPE_NODE_ID = 11;

const ENCODING_VECTOR = 0;
const ENCODING_OPTIONAL_VECTOR = 1;
const ENCODING_CONSTANT = 2;

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
    f64(value) {
        const view = new DataView(new ArrayBuffer(8));
        view.setFloat64(0, value, true);
        this.bytes.push(...new Uint8Array(view.buffer));
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

const encoder = new TextEncoder();

// CHUNK_HEADER: [colCount] then per column [nameLen][typeCode][encoding][name].
const columns = [
    { name: "ids", typeCode: TYPE_NODE_ID, encoding: ENCODING_VECTOR },
    { name: "labels", typeCode: TYPE_STRING, encoding: ENCODING_VECTOR },
    { name: "score", typeCode: TYPE_DOUBLE, encoding: ENCODING_CONSTANT },
    { name: "counts", typeCode: TYPE_UINT64, encoding: ENCODING_OPTIONAL_VECTOR },
];

const header = new Writer();
header.u32(columns.length);
for (const column of columns) {
    const nameBytes = encoder.encode(column.name);
    header.u32(nameBytes.length);
    header.u32(column.typeCode);
    header.u8(column.encoding);
    header.raw(nameBytes);
}

// CHUNK: the columns' data, in header order.
const chunk = new Writer();

// ids: NODE_ID vector — [rowCount][u64 * rows]
chunk.u32(3);
chunk.u64(1n);
chunk.u64(2n);
chunk.u64(999999999999999999n);

// labels: string vector — [rowCount] then per row [len][bytes]
chunk.u32(3);
for (const label of ["remy", "adam", "computers"]) {
    const bytes = encoder.encode(label);
    chunk.u32(bytes.length);
    chunk.raw(bytes);
}

// score: double constant — just the value
chunk.f64(1.5);

// counts: optional uint64 vector — [rowCount][null bitmask][u64 * rows, zero when null]
chunk.u32(3);
chunk.u64(0b101n); // bits 0 and 2 set: rows 0 and 2 have values
chunk.u64(10n);
chunk.u64(0n);
chunk.u64(30n);

createTuringDecoderModule().then((Module) => {
    const decoder = new Module.TuringDecoder(1 << 20);

    decoder.decodePacket(frame(MESSAGE_CHUNK_HEADER, header));
    decoder.decodePacket(frame(MESSAGE_CHUNK, chunk));
    decoder.decodePacket(frame(MESSAGE_END_CHUNK, new Writer()));

    assert.strictEqual(decoder.getColumnCount(), 4);
    assert.strictEqual(decoder.getColumnName(0), "ids");
    assert.strictEqual(decoder.getColumnName(3), "counts");

    const ids = decoder.getColumnBuffers(0);
    assert.strictEqual(ids.typeCode, TYPE_NODE_ID);
    assert.strictEqual(ids.encoding, ENCODING_VECTOR);
    assert.strictEqual(ids.count, 3);
    assert.strictEqual(ids.elementSize, 8);
    assert.deepStrictEqual(Array.from(new BigUint64Array(ids.values.buffer)), [1n, 2n, 999999999999999999n]);
    assert.strictEqual(ids.validity, undefined);

    const labels = decoder.getColumnBuffers(1);
    assert.strictEqual(labels.typeCode, TYPE_STRING);
    assert.strictEqual(labels.count, 3);
    assert.strictEqual(new TextDecoder().decode(labels.values), "remyadamcomputers");
    assert.deepStrictEqual(Array.from(new Uint32Array(labels.offsets.buffer)), [0, 4, 8, 17]);
    assert.deepStrictEqual(Array.from(new Uint32Array(labels.utf16Offsets.buffer)), [0, 4, 8, 17]);

    const score = decoder.getColumnBuffers(2);
    assert.strictEqual(score.encoding, ENCODING_CONSTANT);
    assert.strictEqual(score.count, 1);
    assert.deepStrictEqual(Array.from(new Float64Array(score.values.buffer)), [1.5]);

    const counts = decoder.getColumnBuffers(3);
    assert.strictEqual(counts.encoding, ENCODING_OPTIONAL_VECTOR);
    assert.strictEqual(counts.count, 3);
    assert.deepStrictEqual(Array.from(new BigUint64Array(counts.values.buffer)), [10n, 0n, 30n]);
    assert.deepStrictEqual(Array.from(counts.validity), [0b101]);

    assert.strictEqual(decoder.getListBytes().length, 0);
    assert.strictEqual(decoder.getListStrings().count, 0);

    decoder.delete();
    console.log("wasm decoder smoke test passed: 4 columns decoded");
});
