// Node test for error messages the server splits over several packets: a message longer than
// its buffer goes as several ERROR or PROTOCOL_ERROR packets, the first ERROR packet alone
// carrying the status. Run from wasm/npm/: node split_error_smoke.mjs

import assert from "assert";

import { TuringClient, TuringQueryError } from "./TuringClient.mjs";

const MESSAGE_END = 3;
const MESSAGE_ERROR = 4;
const MESSAGE_PROTOCOL_ERROR = 5;
const STATUS_ANALYZE_ERROR = 3;

const utf8Encoder = new TextEncoder();

function packet(type, payload) {
    const bytes = new Uint8Array(5 + payload.length);
    const view = new DataView(bytes.buffer);
    view.setUint8(0, type);
    view.setUint32(1, payload.length, true);
    bytes.set(payload, 5);
    return bytes;
}

function endPacket() {
    const payload = new Uint8Array(4);
    new DataView(payload.buffer).setFloat32(0, 1.5, true);
    return packet(MESSAGE_END, payload);
}

function concat(parts) {
    const bytes = new Uint8Array(parts.reduce((total, part) => total + part.length, 0));
    let offset = 0;
    for (const part of parts) {
        bytes.set(part, offset);
        offset += part.length;
    }
    return bytes;
}

function clientServing(bytes) {
    return new TuringClient({
        fetch: async () => ({
            ok: true,
            status: 200,
            body: new ReadableStream({
                start(controller) {
                    controller.enqueue(bytes);
                    controller.close();
                },
            }),
        }),
    });
}

async function expectQueryError(bytes, status, message) {
    await assert.rejects(clientServing(bytes).query("RETURN 1"), (raised) => {
        assert.ok(raised instanceof TuringQueryError, `${raised}`);
        assert.strictEqual(raised.status, status);
        assert.strictEqual(raised.message, message);
        return true;
    });
}

// The 2-byte é is cut between the two packets
const variableMessage = utf8Encoder.encode("Variable 'café' not found");
const variableCut = variableMessage.indexOf(0xc3) + 1;

await expectQueryError(concat([
    packet(MESSAGE_ERROR, concat([new Uint8Array([STATUS_ANALYZE_ERROR]), variableMessage.subarray(0, variableCut)])),
    packet(MESSAGE_ERROR, variableMessage.subarray(variableCut)),
    endPacket(),
]), "ANALYZE_ERROR", "Variable 'café' not found");

const unauthorized = utf8Encoder.encode("Unauthorized: missing or invalid authentication token");

await expectQueryError(concat([
    packet(MESSAGE_PROTOCOL_ERROR, unauthorized.subarray(0, 16)),
    packet(MESSAGE_PROTOCOL_ERROR, unauthorized.subarray(16, 32)),
    packet(MESSAGE_PROTOCOL_ERROR, unauthorized.subarray(32)),
]), "PROTOCOL_ERROR", "Unauthorized: missing or invalid authentication token");

console.log("npm client split error test passed");
