// Node test for the largest packet payload the npm client accepts: a packet header that
// announces more than the binary protocol allows is refused before its payload is awaited.
// Run from wasm/npm/: node payload_limit_smoke.mjs

import assert from "assert";

import { TuringClient, TuringQueryError } from "./TuringClient.mjs";

const MESSAGE_CHUNK = 1;
const MAX_PAYLOAD_SIZE = 64 * 1024 * 1024;

const header = new Uint8Array(5);
const view = new DataView(header.buffer);
view.setUint8(0, MESSAGE_CHUNK);
view.setUint32(1, MAX_PAYLOAD_SIZE + 1, true);

const client = new TuringClient({
    fetch: async () => ({
        ok: true,
        status: 200,
        body: new ReadableStream({
            start(controller) {
                controller.enqueue(header);
                controller.close();
            },
        }),
    }),
});

await assert.rejects(client.query("RETURN 1"), (raised) => {
    assert.ok(raised instanceof TuringQueryError, `${raised}`);
    assert.strictEqual(raised.status, "PROTOCOL_ERROR");
    assert.ok(raised.message.includes(`${MAX_PAYLOAD_SIZE} bytes the binary protocol allows`), raised.message);
    return true;
});

console.log("npm client payload limit test passed");
