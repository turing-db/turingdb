// Node test for the parameters the npm client turns away before it sends a request: needs no
// wasm and no server. Run from wasm/npm/: node parameter_errors_smoke.mjs

import assert from "assert";

import { Point, TuringClient, TuringQueryError } from "./TuringClient.mjs";

class RequestSent extends Error {}

const client = new TuringClient({
    fetch: async () => {
        throw new RequestSent();
    },
});

async function expectParameterError(parameters, path, reason = "") {
    await assert.rejects(client.query("RETURN 1", { parameters }), (raised) => {
        assert.ok(raised instanceof TuringQueryError, `${raised}`);
        assert.strictEqual(raised.status, "PARAMETER_ERROR");
        assert.ok(raised.message.includes(path), raised.message);
        assert.ok(raised.message.includes(reason), raised.message);
        return true;
    });
}

const loopedMap = { name: "loop" };
loopedMap.self = loopedMap;

const loopedList = [1];
loopedList.push([loopedList]);

const parameters = {};
parameters.again = parameters;

await expectParameterError({ orphan: Object.create(Object.create(null)) }, "$orphan");
await expectParameterError({ loopedMap }, "$loopedMap.self");
await expectParameterError({ loopedList }, "$loopedList[1][0]");
await expectParameterError(parameters, "$again");

await expectParameterError({ here: new Point(1234, 1, 2) }, "$here", "SRID 1234");
await expectParameterError({ here: new Point(7203, 1, 2, 3) }, "$here", "a cartesian point has 2");
await expectParameterError({ route: [new Point(9157, 1, 2)] }, "$route[0]", "a cartesian-3d point has 3");
await expectParameterError({ stop: { at: new Point(4326, "2.5", 48.5) } }, "$stop.at", "not all numbers");
await assert.rejects(client.query("RETURN 1", { parameters: { here: new Point(4979, 2.5, 48.5, 35) } }), RequestSent);

const shared = { crs: "OXF" };
await assert.rejects(client.query("RETURN 1", { parameters: { from: shared, to: shared, stops: [shared, shared] } }), RequestSent);

console.log("npm client parameter error test passed");
