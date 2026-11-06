// Node test for the values of a point column: a point has no typed array, so Column.values is
// null, as for a variable-width kind, and each row reads as a Point through get().
// Run from wasm/npm/: node point_column_values_smoke.mjs

import assert from "assert";

import { Column, Point } from "./TuringClient.mjs";

const TYPE_POINT = 22;
const ENCODING_VECTOR = 0;
const POINT_SIZE = 32;
const CRS_WGS84 = 2;

const bytes = new Uint8Array(2 * POINT_SIZE);
const view = new DataView(bytes.buffer);
for (let row = 0; row < 2; row++) {
    view.setFloat64(row * POINT_SIZE, row + 1, true);
    view.setFloat64(row * POINT_SIZE + 8, row + 2, true);
    view.setUint8(row * POINT_SIZE + 24, CRS_WGS84);
}

const column = new Column("at", { typeCode: TYPE_POINT, encoding: ENCODING_VECTOR, count: 2, elementSize: POINT_SIZE, values: bytes }, null, 2);

assert.strictEqual(column.values, null);

const second = column.get(1);
assert.ok(second instanceof Point);
assert.deepStrictEqual([second.srid, second.x, second.y, second.z], [4326, 2, 3, undefined]);
assert.deepStrictEqual(column.toArray().map((point) => point.x), [1, 2]);

console.log("npm client point column values test passed");
