// Node test for a response the npm client stops reading: when a query fails after the
// response arrived (an HTTP error status, or a decoding failure), the client cancels the
// response stream instead of leaving the server streaming into it.
// Run from wasm/npm/: node cancel_on_error_smoke.mjs

import assert from "assert";

import { TuringClient, TuringQueryError } from "./TuringClient.mjs";

const UNKNOWN_PACKET_TYPE = 99;

async function expectCancelled(ok, status, bytes, expectedStatus) {
    let cancelled = false;

    const client = new TuringClient({
        fetch: async () => ({
            ok,
            status,
            body: new ReadableStream({
                start(controller) {
                    controller.enqueue(bytes);
                },
                cancel() {
                    cancelled = true;
                },
            }),
        }),
    });

    await assert.rejects(client.query("RETURN 1"), (raised) => {
        assert.ok(raised instanceof TuringQueryError, `${raised}`);
        assert.strictEqual(raised.status, expectedStatus);
        return true;
    });
    assert.ok(cancelled, `the response stream was left open after ${expectedStatus}`);
}

await expectCancelled(true, 200, new Uint8Array([UNKNOWN_PACKET_TYPE, 0, 0, 0, 0]), "PROTOCOL_ERROR");
await expectCancelled(false, 502, new TextEncoder().encode("Bad Gateway"), "HTTP_ERROR");

console.log("npm client cancel on error test passed");
