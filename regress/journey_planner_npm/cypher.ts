import assert from "node:assert/strict";

import { Column, ColumnEncoding, ColumnType, TuringQueryError, double } from "turingdb";
import type { Parameters } from "turingdb";

import { at, embedding, minutes } from "./harness.js";
import type { Case, Check } from "./harness.js";

const OXFORD_FACILITIES = { parking: true, cycleSpaces: 400n, retail: ["café", "newsagent"] };

// Every value type the engine stores or computes, as the npm client decodes it: a 64-bit
// integer or an ID as a BigInt, a double as a number, a DateTime as microseconds since the
// epoch and a Duration as microseconds, both BigInt, and an embedding as a Float32Array
export const VALUE_CASES: Case[] = [
    {
        name: "reads the scalar properties of the stations",
        query: "MATCH (s:STATION) RETURN s.crs, s.name, s.minInterchange, s.lat, s.stepFree ORDER BY s.crs",
        types: ["STRING", "STRING", "INT64", "DOUBLE", "BOOL"],
        ordered: true,
        rows: [
            ["DID", "Didcot Parkway", 4n, 51.611, true],
            ["OXF", "Oxford", 5n, 51.7535, true],
            ["PAD", "London Paddington", 10n, 51.5154, true],
            ["RDG", "Reading", 6n, 51.4589, true],
            ["SWI", "Swindon", 5n, 51.5655, false],
        ],
    },
    {
        name: "reads the temporal properties of the stations",
        query: "MATCH (s:STATION) RETURN s.crs, s.opened, s.minConnection, s.opened.year ORDER BY s.opened",
        types: ["STRING", "DATE_TIME", "DURATION", "INT64"],
        ordered: true,
        rows: [
            ["RDG", at("1840-03-30T00:00:00Z"), minutes(6), 1840n],
            ["SWI", at("1842-07-17T00:00:00Z"), minutes(5), 1842n],
            ["DID", at("1844-06-12T00:00:00Z"), minutes(4), 1844n],
            ["OXF", at("1852-10-01T00:00:00Z"), minutes(5), 1852n],
            ["PAD", at("1854-01-16T00:00:00Z"), minutes(10), 1854n],
        ],
    },
    {
        name: "reads the list and map properties of the stations",
        query: "MATCH (s:STATION) RETURN s.crs, s.platforms, s.facilities ORDER BY s.crs",
        types: ["STRING", "LIST_VIEW", "MAP_VIEW"],
        ordered: true,
        rows: [
            ["DID", [1n, 2n, 3n, 4n, 5n], { parking: true, cycleSpaces: 120n, retail: ["café"] }],
            ["OXF", [1n, 2n, 3n, 4n], OXFORD_FACILITIES],
            ["PAD", [1n, 2n, 3n, 4n, 5n, 6n, 7n, 8n, 9n, 10n, 11n, 12n, 13n, 14n], { parking: false, cycleSpaces: 250n, retail: ["bookshop", "café", "florist"] }],
            ["RDG", [7n, 8n, 9n, 10n, 11n, 12n, 13n, 14n, 15n], { parking: true, cycleSpaces: 800n, retail: ["café", "pharmacy", "bookshop"] }],
            ["SWI", [1n, 2n, 3n, 4n], { parking: true, cycleSpaces: 60n, retail: [] }],
        ],
    },
    {
        name: "reads the embedding property of the stations",
        query: "MATCH (s:STATION) RETURN s.crs, s.profile ORDER BY s.crs",
        types: ["STRING", "EMBEDDING"],
        ordered: true,
        rows: [
            ["DID", embedding(0.4, 0.8, 0.2)],
            ["OXF", embedding(0.9, 0.3, 0.5)],
            ["PAD", embedding(1.0, 0.7, 0.9)],
            ["RDG", embedding(0.8, 0.9, 0.4)],
            ["SWI", embedding(0.5, 0.6, 0.3)],
        ],
    },
    {
        name: "reads a property only one station has as null on the others",
        query: "MATCH (s:STATION) RETURN s.crs, s.sign ORDER BY s.crs",
        ordered: true,
        rows: [["DID", null], ["OXF", "Oxford 🚆"], ["PAD", null], ["RDG", null], ["SWI", null]],
    },
    {
        name: "reads a property no node has as null",
        query: "MATCH (s:STATION) RETURN s.crs, s.neverWritten ORDER BY s.crs",
        ordered: true,
        rows: [["DID", null], ["OXF", null], ["PAD", null], ["RDG", null], ["SWI", null]],
    },
    {
        name: "reads a property no node has as null in a query of nothing else",
        query: "MATCH (s:STATION) RETURN s.neverWritten",
        rows: [[null], [null], [null], [null], [null]],
    },
    {
        name: "reads every property of a station as one map",
        query: "MATCH (s:STATION {crs: 'OXF'}) RETURN properties(s)",
        types: ["MAP_VIEW"],
        rows: [[{
            crs: "OXF",
            nlc: "3115",
            name: "Oxford",
            minInterchange: 5n,
            maxInterchange: 60n,
            lat: 51.7535,
            lon: -1.27,
            stepFree: true,
            opened: at("1852-10-01T00:00:00Z"),
            minConnection: minutes(5),
            platforms: [1n, 2n, 3n, 4n],
            facilities: OXFORD_FACILITIES,
            profile: embedding(0.9, 0.3, 0.5),
            sign: "Oxford 🚆",
        }]],
    },
    {
        name: "lists the keys of a station",
        query: "MATCH (s:STATION {crs: 'OXF'}) UNWIND keys(s) AS key RETURN key ORDER BY key",
        ordered: true,
        rows: [
            ["crs"], ["facilities"], ["lat"], ["lon"], ["maxInterchange"], ["minConnection"], ["minInterchange"],
            ["name"], ["nlc"], ["opened"], ["platforms"], ["profile"], ["sign"], ["stepFree"],
        ],
    },
    {
        name: "decodes strings of one, two, three and four UTF-8 bytes per character",
        query: "UNWIND ['café', 'Oxford 🚆', 'OXF → PAD', '', 'naïve'] AS text RETURN text",
        ordered: true,
        rows: [["café"], ["Oxford 🚆"], ["OXF → PAD"], [""], ["naïve"]],
    },
    {
        name: "unwinds a list held in a map property",
        query: "MATCH (s:STATION) UNWIND s.facilities.retail AS shop RETURN DISTINCT shop ORDER BY shop",
        ordered: true,
        rows: [["bookshop"], ["café"], ["florist"], ["newsagent"], ["pharmacy"]],
    },
    {
        name: "returns literals of every scalar type",
        query: "RETURN 9223372036854775807 AS largest, -42 AS negative, 2.5 AS double, true AS yes, 'text' AS text",
        types: ["INT64", "INT64", "DOUBLE", "BOOL", "STRING"],
        rows: [[9223372036854775807n, -42n, 2.5, true, "text"]],
    },
    {
        name: "returns a list holding every kind of value",
        query: "RETURN [1, 2.5, 'three', true, null, [4, [5]], {six: 6}, datetime('2026-11-04T08:00:00Z'), duration({minutes: 5})]",
        rows: [[[1n, 2.5, "three", true, null, [4n, [5n]], { six: 6n }, at("2026-11-04T08:00:00Z"), minutes(5)]]],
    },
    {
        name: "returns maps nested in maps and lists",
        query: "RETURN {stop: {crs: 'OXF', calls: [{service: 'S1', depart: 480}, {service: 'F1', depart: null}]}}",
        rows: [[{ stop: { crs: "OXF", calls: [{ service: "S1", depart: 480n }, { service: "F1", depart: null }] } }]],
    },
    {
        name: "returns nodes, edges and paths as their IDs",
        query: "MATCH p = (o:STATION {crs: 'OXF'})-[t:THU]->(m:MODE) RETURN o, t, m, p, id(o), id(t), id(m)",
        types: ["NODE_ID", "EDGE_ID", "NODE_ID", "GRAPH_PATH", "INT64", "INT64", "INT64"],
        check: ({ rows }) => {
            assert.equal(rows.length, 1);
            const [station, edge, callingPoint, path, stationId, edgeId, callingPointId] = rows[0];

            assert.equal(station, stationId);
            assert.equal(edge, edgeId);
            assert.equal(callingPoint, callingPointId);
            assert.deepEqual(path, [station, edge, callingPoint]);
        },
    },
    {
        name: "collects nodes as a list of their IDs",
        query: "MATCH (:STATION {crs: 'OXF'})-[:WED]->(m:MODE) WITH m ORDER BY m.service RETURN collect(m), collect(id(m))",
        check: ({ rows }) => {
            const [nodes, ids] = rows[0];

            assert.ok(Array.isArray(nodes));
            assert.equal(nodes.length, 11);
            assert.deepEqual(nodes, ids);
        },
    },
    {
        name: "adds a duration to a datetime",
        query: "WITH datetime('2026-11-04T08:00:00Z') AS departs, duration({hours: 1, minutes: 5}) AS takes " +
            "RETURN departs + takes, takes, departs + takes > departs, (departs + takes).hour",
        types: ["DATE_TIME", "DURATION", "BOOL", "INT64"],
        rows: [[at("2026-11-04T09:05:00Z"), minutes(65), true, 9n]],
    },
    {
        name: "converts between types",
        query: "RETURN toInteger('42'), toFloat('2.5'), toString(42), toBoolean('true'), toInteger(2.9), toString(2.5)",
        rows: [[42n, 2.5, "42", true, 2n, "2.5"]],
    },
    {
        name: "applies the string functions to a station's name",
        query: "MATCH (s:STATION {crs: 'PAD'}) " +
            "RETURN toUpper(s.name), toLower(s.crs), left(s.name, 6), right(s.name, 10), substring(s.name, 7, 4), " +
            "replace(s.name, 'London ', ''), split(s.name, ' '), size(s.name), trim('  PAD  '), reverse(s.crs), " +
            "s.name STARTS WITH 'London', s.name ENDS WITH 'ton', s.name CONTAINS 'Padd'",
        rows: [["LONDON PADDINGTON", "pad", "London", "Paddington", "Padd", "Paddington", ["London", "Paddington"], 17n, "PAD", "DAP", true, true, true]],
    },
    {
        name: "computes with integers and doubles",
        query: "RETURN abs(-3), sqrt(16), ceil(2.1), floor(2.9), 7 / 2, 7 % 3, 7.0 / 2, 2 + 0.5, -(3)",
        rows: [[3n, 4.0, 3.0, 2.0, 3n, 1n, 3.5, 2.5, -3n]],
    },
];

// The timetable is 5 stations, 34 calling points and 2 fixed links: 36 ARRIVE edges, 20 NEXT
// edges, and 19 WED and 1 THU edges to the calling points boardable at a station. Oxford
// has 12 calling points, Didcot 10, Reading and Paddington 6 each, Swindon 2.
export const MECHANISM_CASES: Case[] = [
    {
        name: "counts the nodes of each label set",
        query: "MATCH (n) RETURN labels(n), count(n)",
        rows: [[["STATION"], 5n], [["MODE"], 34n], [["MODE", "FIXED"], 2n]],
    },
    {
        name: "counts the edges of each type",
        query: "MATCH ()-[r]->() RETURN type(r), count(r)",
        rows: [["ARRIVE", 36n], ["NEXT", 20n], ["WED", 19n], ["THU", 1n]],
    },
    {
        name: "counts the calling points of each station with a COUNT subquery",
        query: "MATCH (s:STATION) RETURN s.crs, COUNT { (s)<-[:ARRIVE]-(:MODE) }",
        rows: [["OXF", 12n], ["DID", 10n], ["RDG", 6n], ["PAD", 6n], ["SWI", 2n]],
    },
    {
        name: "keeps the stations that board a Thursday service with EXISTS",
        query: "MATCH (s:STATION) WHERE EXISTS { (s)-[:THU]->(:MODE) } RETURN s.crs",
        rows: [["OXF"]],
    },
    {
        name: "pads the stations with no Thursday service with OPTIONAL MATCH",
        query: "MATCH (s:STATION) OPTIONAL MATCH (s)-[:THU]->(m:MODE) RETURN s.crs, m.service",
        rows: [["DID", null], ["OXF", "S9"], ["PAD", null], ["RDG", null], ["SWI", null]],
    },
    {
        name: "aggregates the Wednesday departures of each station",
        query: "MATCH (s:STATION)-[:WED]->(m:MODE) WHERE m.depart IS NOT NULL " +
            "RETURN s.crs, count(m), min(m.depart), max(m.depart), sum(m.depart), avg(m.depart)",
        rows: [
            ["OXF", 9n, 470n, 520n, 4392n, 488.0],
            ["DID", 5n, 497n, 540n, 2551n, 510.2],
            ["RDG", 3n, 517n, 535n, 1578n, 526.0],
        ],
    },
    {
        name: "collects each station's departures in the order of their time",
        query: "MATCH (s:STATION)-[:WED]->(m:MODE) WHERE m.depart IS NOT NULL " +
            "WITH s, m ORDER BY m.depart, m.service RETURN s.crs, collect(m.service)",
        rows: [
            ["OXF", ["S5", "S6", "S1", "S10", "S4", "S2", "S7", "S3", "S8"]],
            ["DID", ["S1", "S10", "S13", "S11", "S12"]],
            ["RDG", ["S1", "S11", "S14"]],
        ],
    },
    {
        name: "keeps the services with more than two calls",
        query: "MATCH (m:MODE) WITH m.service AS service, count(*) AS calls WHERE calls > 2 RETURN service, calls",
        rows: [["S1", 4n], ["S10", 3n], ["S11", 3n]],
    },
    {
        name: "pages through the distinct services",
        query: "MATCH (m:MODE) RETURN DISTINCT m.service AS service ORDER BY service SKIP 2 LIMIT 3",
        ordered: true,
        rows: [["S1"], ["S10"], ["S11"]],
    },
    {
        name: "merges the direct services to Paddington with UNION",
        query: "MATCH (:STATION {crs: 'OXF'})-[:WED]->(m:MODE)-[:NEXT*]->(:MODE)-[:ARRIVE]->(:STATION {crs: 'PAD'}) RETURN m.service AS service " +
            "UNION " +
            "MATCH (:STATION {crs: 'DID'})-[:WED]->(m:MODE)-[:NEXT*]->(:MODE)-[:ARRIVE]->(:STATION {crs: 'PAD'}) RETURN m.service AS service",
        rows: [["S1"], ["S8"], ["S11"], ["S12"]],
    },
    {
        name: "keeps both boardings of S1 with UNION ALL",
        query: "MATCH (:STATION {crs: 'OXF'})-[:WED]->(m:MODE)-[:NEXT*]->(:MODE)-[:ARRIVE]->(:STATION {crs: 'PAD'}) RETURN m.service AS service " +
            "UNION ALL " +
            "MATCH (:STATION {crs: 'DID'})-[:WED]->(m:MODE)-[:NEXT*]->(:MODE)-[:ARRIVE]->(:STATION {crs: 'PAD'}) RETURN m.service AS service",
        rows: [["S1"], ["S8"], ["S1"], ["S11"], ["S12"]],
    },
    {
        name: "finds the first departure of each station in a CALL subquery",
        query: "MATCH (s:STATION) CALL (s) { MATCH (s)-[:WED]->(m:MODE) WHERE m.depart IS NOT NULL " +
            "RETURN m.service AS first ORDER BY m.depart LIMIT 1 } RETURN s.crs, first",
        rows: [["OXF", "S5"], ["DID", "S1"], ["RDG", "S1"]],
    },
    {
        name: "counts the early departures of each station with a pattern comprehension",
        query: "MATCH (s:STATION) RETURN s.crs, size([(s)-[:WED]->(m:MODE) WHERE m.depart < 500 | m.service])",
        rows: [["OXF", 7n], ["DID", 2n], ["RDG", 0n], ["PAD", 0n], ["SWI", 0n]],
    },
    {
        name: "projects a station into a map",
        query: "MATCH (s:STATION {crs: 'RDG'}) RETURN s {.crs, .name, interchange: [s.minInterchange, s.maxInterchange]}",
        rows: [[{ crs: "RDG", name: "Reading", interchange: [6n, 50n] }]],
    },
    {
        name: "decides the step-free access of a service with all, any, none and single",
        query: "MATCH (m:MODE)-[:ARRIVE]->(s:STATION) WHERE m.service IN ['S1', 'S4', 'S10'] " +
            "WITH m.service AS service, collect(s.stepFree) AS stepFree " +
            "RETURN service, all(x IN stepFree WHERE x), any(x IN stepFree WHERE NOT x), " +
            "none(x IN stepFree WHERE NOT x), single(x IN stepFree WHERE NOT x)",
        rows: [["S1", true, false, true, false], ["S4", false, true, false, true], ["S10", false, true, false, true]],
    },
    {
        name: "names each station's role with a simple CASE",
        query: "MATCH (s:STATION) RETURN s.crs, CASE s.crs WHEN 'PAD' THEN 'terminus' WHEN 'OXF' THEN 'origin' ELSE 'interchange' END",
        rows: [["OXF", "origin"], ["DID", "interchange"], ["RDG", "interchange"], ["PAD", "terminus"], ["SWI", "interchange"]],
    },
    {
        name: "applies the list functions to a station's platforms",
        query: "MATCH (s:STATION {crs: 'RDG'}) " +
            "RETURN head(s.platforms), last(s.platforms), size(s.platforms), tail([7, 8, 9]), reverse([7, 8, 9]), " +
            "9 IN s.platforms, 1 IN s.platforms",
        rows: [[7n, 15n, 9n, [8n, 9n], [9n, 8n, 7n], true, false]],
    },
    {
        name: "builds a constant list with range",
        query: "RETURN range(0, 10, 5)",
        rows: [[[0n, 5n, 10n]]],
    },
    {
        name: "slices a constant list",
        query: "WITH [7, 8, 9] AS platforms RETURN platforms[1..], platforms[..-1]",
        rows: [[[8n, 9n], [7n, 8n]]],
    },
    {
        name: "indexes a constant list of lists",
        query: "WITH [[7, 8], [9]] AS platforms RETURN platforms[0]",
        rows: [[[7n, 8n]]],
    },
    {
        name: "streams a departure board longer than one chunk",
        query: "MATCH (s:STATION)-[:WED]->(m:MODE) WHERE m.depart IS NOT NULL " +
            "UNWIND range(1, 10000) AS day RETURN s.crs, m.service, m.depart + 1440 * day AS minute",
        check: ({ rows, chunkCount }) => {
            assert.equal(rows.length, 170000);
            assert.ok(chunkCount > 1, `expected the rows over several chunks, got ${chunkCount}`);

            const total = rows.reduce((sum, row) => sum + (row[2] as bigint), 0n);
            assert.equal(total, 10000n * 8521n + 17n * 1440n * 50005000n);
        },
    },
];

export const ERROR_CASES: Case[] = [
    {
        name: "reports a parse error",
        query: "MATCH (s:STATION RETURN s",
        error: { status: "PARSE_ERROR", message: "syntax error, unexpected RETURN" },
    },
    {
        name: "reports an unknown variable",
        query: "RETURN unknownVariable",
        error: { status: "ANALYZE_ERROR", message: "unknownVariable" },
    },
    {
        name: "reports a graph that does not exist",
        query: "MATCH (n) RETURN n",
        options: { graph: "nowhere" },
        error: { status: "GRAPH_NOT_FOUND", message: "nowhere" },
    },
    {
        name: "reports a change that does not exist",
        query: "MATCH (n) RETURN n",
        options: { change: 9999 },
        error: { status: "CHANGE_NOT_FOUND", message: "9999" },
    },
    {
        name: "reports a commit that does not exist",
        query: "MATCH (n) RETURN n",
        options: { commit: "deadbeefdeadbeef" },
        error: { status: "COMMIT_NOT_FOUND", message: "Commit 'deadbeefdeadbeef' does not exist" },
    },
];

// The engine takes a parameter as a label so far (TUR-48), so the values the cases after the
// labels read are declared up front with WITH. The parameter map is parsed whole either way,
// so a map holding every kind of value still proves the client encodes each one.
export const PARAMETER_CASES: Case[] = [
    {
        name: "matches the label a parameter names",
        query: "MATCH (s:$label) RETURN s.crs",
        options: { parameters: { label: "STATION" } },
        rows: [["OXF"], ["DID"], ["RDG"], ["PAD"], ["SWI"]],
    },
    {
        name: "matches every label of a list parameter",
        query: "MATCH (m:$labels) RETURN m.service",
        options: { parameters: { labels: ["MODE", "FIXED"] } },
        rows: [["F1"], ["F2"]],
    },
    {
        name: "parses a parameter map holding every kind of value",
        query: "MATCH (s:$label) RETURN count(s)",
        options: {
            parameters: {
                label: "STATION",
                unused: [470, 2.5, double(2), -1n, 2n ** 62n, "Oxford 🚆", true, null, [], {}, { stop: { crs: "PAD", platforms: [1, 2] } }],
            },
        },
        rows: [[5n]],
    },
    {
        name: "reports a parameter the query names but the map lacks",
        query: "MATCH (s:$label) RETURN s",
        options: { parameters: { lable: "STATION" } },
        error: { status: "EXEC_ERROR", message: "Parameter $label is not defined" },
    },
    {
        name: "returns a constant of every scalar type",
        query: "WITH 470 AS integer, 2.5 AS ratio, 2.0 AS whole, 'Oxford 🚆' AS text, true AS flag, null AS nothing " +
            "RETURN integer, ratio, whole, text, flag, nothing",
        rows: [[470n, 2.5, 2.0, "Oxford 🚆", true, null]],
    },
    {
        name: "returns a list and a map constant",
        query: "WITH [480, 'S1', [2.5], null] AS calls, {crs: 'PAD', platforms: [1, 2]} AS stop RETURN calls, stop",
        rows: [[[480n, "S1", [2.5], null], { crs: "PAD", platforms: [1n, 2n] }]],
    },
    {
        name: "returns an integer constant past a double's precision",
        query: "WITH 4611686018427387905 AS big RETURN big",
        rows: [[2n ** 62n + 1n]],
    },
    {
        name: "keeps the stations a list constant names",
        query: "WITH ['OXF', 'PAD'] AS stops MATCH (s:STATION) WHERE s.crs IN stops RETURN s.name",
        rows: [["Oxford"], ["London Paddington"]],
    },
    {
        name: "reads a key of a map constant",
        query: "WITH {from: 470, to: 500} AS window RETURN window.from, window.to - window.from",
        rows: [[470n, 30n]],
    },
];

export const CLIENT_CHECKS: Check[] = [
    {
        name: "rejects a parameter a query cannot hold before sending it",
        run: async (client) => {
            // A JavaScript caller has no declarations to turn the Date away
            const parameters = { when: new Date(0) } as unknown as Parameters;

            await assert.rejects(client.query("RETURN 1", { parameters }), (raised) => {
                assert.ok(raised instanceof TuringQueryError);
                assert.equal(raised.status, "PARAMETER_ERROR");
                assert.ok(raised.message.includes("$when"));
                return true;
            });
        },
    },
    {
        name: "returns plain numbers from queryData",
        run: async (client) => {
            const data = await client.queryData("MATCH (s:STATION) RETURN s.crs, s.minInterchange, s.lat ORDER BY s.crs");

            assert.deepEqual(data, [[
                ["DID", "OXF", "PAD", "RDG", "SWI"],
                [4, 5, 10, 6, 5],
                [51.611, 51.7535, 51.5154, 51.4589, 51.5655],
            ]]);
        },
    },
    {
        name: "times the query",
        run: async (client) => {
            const { execTimeMs } = await client.query("MATCH (n) RETURN count(n)");

            assert.equal(typeof execTimeMs, "number");
            assert.ok((execTimeMs as number) >= 0);
        },
    },
    {
        name: "exposes a vector column's values as a typed array",
        run: async (client) => {
            const { chunks } = await client.query("MATCH (m:MODE) WHERE m.depart IS NOT NULL RETURN m.depart ORDER BY m.depart LIMIT 3");
            const column: Column = chunks[0].columns[0];

            assert.equal(column.typeCode, ColumnType.INT64);
            assert.equal(column.length, 3);
            assert.deepEqual(column.toArray(), [470n, 475n, 480n]);
            assert.deepEqual(column.toNumberArray(), [470, 475, 480]);
        },
    },
    {
        name: "marks the missing rows of an optional column",
        run: async (client) => {
            const { chunks } = await client.query("MATCH (s:STATION) RETURN s.sign ORDER BY s.crs");
            const column: Column = chunks[0].columns[0];

            assert.ok(column.isOptional);
            assert.deepEqual(column.toArray(), [null, "Oxford 🚆", null, null, null]);
        },
    },
    {
        name: "sends one value for a constant column",
        run: async (client) => {
            const { chunks } = await client.query("MATCH (s:STATION) RETURN s.crs, 'UK' AS country");
            const column: Column = chunks[0].columns[1];

            assert.ok(column.isConstant);
            assert.equal(column.encoding, ColumnEncoding.CONSTANT);
            assert.deepEqual(column.toArray(), ["UK", "UK", "UK", "UK", "UK"]);
        },
    },
];

// db::ValueType's ordinals, which are append-only on disk
const VALUE_TYPE = { INT64: 1, DOUBLE: 3, STRING: 4, BOOL: 5, EMBEDDING: 6, LIST: 7, DATE_TIME: 8, MAP: 9, DURATION: 10 };

export const SYSTEM_CASES: Case[] = [
    {
        name: "lists the graphs",
        query: "LIST GRAPH",
        rows: [["timetable"], ["default"]],
    },
    {
        name: "lists the labels",
        query: "CALL db.labels() YIELD label RETURN label",
        rows: [["STATION"], ["MODE"], ["FIXED"]],
    },
    {
        name: "lists the edge types",
        query: "CALL db.edgeTypes() YIELD edgeType RETURN edgeType",
        rows: [["ARRIVE"], ["NEXT"], ["WED"], ["THU"]],
    },
    {
        name: "types every property of the timetable",
        query: "CALL db.propertyTypes() YIELD propertyType, valueType RETURN propertyType, valueType",
        types: ["STRING", "VALUE_TYPE"],
        rows: [
            ["arrive", VALUE_TYPE.INT64],
            ["crs", VALUE_TYPE.STRING],
            ["day", VALUE_TYPE.STRING],
            ["depart", VALUE_TYPE.INT64],
            ["end", VALUE_TYPE.INT64],
            ["facilities", VALUE_TYPE.MAP],
            ["lat", VALUE_TYPE.DOUBLE],
            ["lon", VALUE_TYPE.DOUBLE],
            ["maxInterchange", VALUE_TYPE.INT64],
            ["minConnection", VALUE_TYPE.DURATION],
            ["minInterchange", VALUE_TYPE.INT64],
            ["minutes", VALUE_TYPE.INT64],
            ["name", VALUE_TYPE.STRING],
            ["nlc", VALUE_TYPE.STRING],
            ["opened", VALUE_TYPE.DATE_TIME],
            ["platforms", VALUE_TYPE.LIST],
            ["profile", VALUE_TYPE.EMBEDDING],
            ["seq", VALUE_TYPE.INT64],
            ["service", VALUE_TYPE.STRING],
            ["sign", VALUE_TYPE.STRING],
            ["start", VALUE_TYPE.INT64],
            ["stepFree", VALUE_TYPE.BOOL],
        ],
    },
];
