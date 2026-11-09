// Copyright Turing Biosystems Ltd
// SPDX-License-Identifier: Apache-2.0

// Node test for the schema IDs and value types a list or a map holds: each reads as the
// column of its kind reads, a label or an edge type ID as a bigint, a property type ID and
// a value type as a number.
// Run from wasm/npm/: node schema_id_cells_smoke.mjs

import assert from "assert";

import { NestedReader } from "./TuringClient.mjs";

const TAG_LIST_VIEW = 6;
const TAG_MAP_VIEW = 11;
const TAG_LABEL_ID = 15;
const TAG_EDGE_TYPE_ID = 16;
const TAG_PROPERTY_TYPE_ID = 17;
const TAG_VALUE_TYPE = 18;

const VALUE_TYPE_BOOL = 5;

const bytes = [];

const pushUint32 = (value) => {
    const view = new DataView(new ArrayBuffer(4));
    view.setUint32(0, value, true);
    bytes.push(...new Uint8Array(view.buffer));
};

const pushCell = (tag, size, write) => {
    const view = new DataView(new ArrayBuffer(size));
    write(view);
    bytes.push(tag, ...new Uint8Array(view.buffer));
};

const pushSchemaIDs = () => {
    pushCell(TAG_LABEL_ID, 8, (view) => view.setBigUint64(0, 3n, true));
    pushCell(TAG_EDGE_TYPE_ID, 8, (view) => view.setBigUint64(0, 1n, true));
    pushCell(TAG_PROPERTY_TYPE_ID, 2, (view) => view.setUint16(0, 7, true));
    pushCell(TAG_VALUE_TYPE, 1, (view) => view.setUint8(0, VALUE_TYPE_BOOL));
};

// A list of the four kinds, then a nested list and a map holding them again
pushUint32(6);
pushSchemaIDs();
bytes.push(TAG_LIST_VIEW);
pushUint32(4);
pushSchemaIDs();
bytes.push(TAG_MAP_VIEW);
pushUint32(1);
pushUint32(0);
pushCell(TAG_LABEL_ID, 8, (view) => view.setBigUint64(0, 8n, true));

const strings = {
    values: new TextEncoder().encode("label"),
    offsets: new Uint8Array(new Uint32Array([0, 5]).buffer),
    utf16Offsets: new Uint8Array(new Uint32Array([0, 5]).buffer),
};

const reader = new NestedReader(new Uint8Array(bytes), strings);
const expected = [3n, 1n, 7, VALUE_TYPE_BOOL];

assert.deepStrictEqual(reader.readListAt(0), [...expected, expected, { label: 8n }]);

console.log("npm client schema ID cells test passed");
