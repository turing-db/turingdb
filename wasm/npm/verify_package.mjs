// Drives the packed or published turingdb package through its public entry points, so a
// bad files list or exports map fails in CI rather than on a user's machine. Bare
// specifiers resolve from this file's own location, so it is copied next to the
// installed package rather than run out of the source tree.
import assert from "node:assert/strict";

import { TuringClient, Column, TuringQueryError, ColumnType, ColumnEncoding } from "turingdb";

assert.equal(typeof TuringClient, "function", "turingdb must export TuringClient");
assert.equal(typeof Column, "function", "turingdb must export Column");
assert.equal(typeof TuringQueryError, "function", "turingdb must export TuringQueryError");
assert.equal(typeof ColumnEncoding, "object", "turingdb must export ColumnEncoding");
assert.equal(ColumnType.NODE_ID, 11, "ColumnType must match the wire type codes");

// The decoder is an implementation detail the client loads for itself. Reaching it would
// couple a caller to the wasm ABI, so the package must refuse to hand it out.
await assert.rejects(() => import("turingdb/decoder"),
                     (raised) => raised.code === "ERR_PACKAGE_PATH_NOT_EXPORTED",
                     "turingdb/decoder must not be a public entry point");

// An END packet alone is a complete empty result: [type u8][dataLen u32le][ms f32le].
// Serving one runs the client through the decoder module it loads internally, which is
// what proves the wasm binary made it into the package and instantiates.
const endPacket = new Uint8Array(9);
const endPacketView = new DataView(endPacket.buffer);
endPacketView.setUint8(0, 3);
endPacketView.setUint32(1, 4, true);
endPacketView.setFloat32(5, 1.5, true);

function serveOnce(bytes) {
    let sent = false;

    const read = async () => {
        if (sent) {
            return { value: undefined, done: true };
        }

        sent = true;
        return { value: bytes, done: false };
    };

    return async () => ({ ok: true, body: { getReader: () => ({ read }) } });
}

const client = new TuringClient({ url: "/query", fetch: serveOnce(endPacket) });
const { chunks, execTimeMs } = await client.query("MATCH (n) RETURN n");

assert.equal(chunks.length, 1, "an END-only response carries one empty chunk");
assert.deepEqual(chunks[0].names, [], "an empty chunk names no column");
assert.ok(Math.abs(execTimeMs - 1.5) < 1e-6, "the client must read the END packet's exec time");

console.log("turingdb package verified: exports, decoder encapsulation, wasm decode path");
