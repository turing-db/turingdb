// Node test for a response that ends without its END packet: the npm client reports it as
// cut short, even when an ERROR packet came before the end, as the C++ clients do.
// Run from wasm/npm/: node response_without_end_smoke.mjs

import assert from "assert";

import { TuringClient, TuringQueryError } from "./TuringClient.mjs";

const MESSAGE_ERROR = 4;
const STATUS_PARSE_ERROR = 2;

const message = new TextEncoder().encode("Unexpected toke");
const packet = new Uint8Array(5 + 1 + message.length);
const view = new DataView(packet.buffer);
view.setUint8(0, MESSAGE_ERROR);
view.setUint32(1, 1 + message.length, true);
packet[5] = STATUS_PARSE_ERROR;
packet.set(message, 6);

const client = new TuringClient({
    fetch: async () => ({
        ok: true,
        status: 200,
        body: new ReadableStream({
            start(controller) {
                controller.enqueue(packet);
                controller.close();
            },
        }),
    }),
});

await assert.rejects(client.query("RETURN 1"), (raised) => {
    assert.ok(raised instanceof TuringQueryError, `${raised}`);
    assert.strictEqual(raised.status, "PROTOCOL_ERROR");
    assert.strictEqual(raised.message, "Response ended before the END packet");
    return true;
});

console.log("npm client response without END test passed");
