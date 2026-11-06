import assert from "node:assert/strict";

import type { Value } from "turingdb";

import { at, minutes } from "./harness.js";
import type { Case } from "./harness.js";

// The customer's journey planner of test/query/ir/journey, sent through the npm client.
// The leg search, with its two parameters read from variables: a leg boards at o between
// depart_from and depart_before and alights at one of interchanges, where its service does
// not run on to the destination d.
const FIND_LEGS =
    "MATCH leg1=(o)-[:WED]->(firstStop:MODE) -[:NEXT * 0..99]-> (lastStop:MODE)-[:ARRIVE]-(inter:STATION) " +
    "WHERE inter.crs IN interchanges " +
    "AND NOT EXISTS {(lastStop)-[:NEXT|ARRIVE]->{2,20}(d)} " +
    "AND CASE WHEN firstStop:FIXED " +
    "THEN (depart_before > firstStop.start AND depart_from < firstStop.end) " +
    "ELSE depart_before >= firstStop.depart >= depart_from " +
    "END ";

const LEG_ROW =
    "firstStop.service AS service, " +
    "inter.crs AS stop, " +
    "interchanges[coll.indexOf(interchanges, inter.crs) + 1..] AS interchanges, " +
    "lastStop.nlc AS nlc, " +
    "firstStop.depart AS departure, " +
    "coalesce(lastStop.arrive, depart_from) AS arrival, " +
    "coalesce(lastStop.arrive, depart_from) + inter.minInterchange AS departAfter, " +
    "coalesce(lastStop.arrive, depart_before) + inter.maxInterchange AS departBefore, " +
    "inter = d AS arrived ";

// OTTlist sorted the way the customer's apoc.coll.sortMulti(OTTlist, ['nlc', '^arrival', 'departure']) sorts it
const LEGS_AS_A_SORTED_LIST =
    "WITH service, stop, interchanges, nlc, departure, arrival, departAfter, departBefore, arrived " +
    "ORDER BY nlc DESC, arrival, departure DESC " +
    "WITH collect([service, stop, interchanges, nlc, departure, arrival, departAfter, departBefore, arrived]) AS rows " +
    "WITH [r IN rows | {service: r[0], stop: r[1], interchanges: r[2], nlc: r[3], departure: r[4], " +
    "arrival: r[5], departAfter: r[6], departBefore: r[7], arrived: r[8]}] AS OTTlist ";

// The customer's reduce, verbatim: at each location (nlc), a leg is dropped when the leg
// kept before it leaves no earlier and arrives no later
const KEEP_UNBEATEN_LEGS =
    "reduce(retain = [OTTlist[0]], i IN range(1, size(OTTlist) - 1) | " +
    "CASE " +
    "WHEN OTTlist[i].nlc = retain[-1].nlc THEN " +
    "CASE " +
    "WHEN (OTTlist[i].departure <= retain[-1].departure AND OTTlist[i].arrival >= retain[-1].arrival) THEN retain " +
    "WHEN (OTTlist[i].departure = retain[-1].departure AND OTTlist[i].arrival < retain[-1].arrival) THEN retain[..-1] + OTTlist[i] " +
    "ELSE retain + OTTlist[i] " +
    "END " +
    "ELSE retain + OTTlist[i] " +
    "END)";

function findLegsFromOxford(interchanges: string, departFrom: number, departBefore: number): string {
    return "MATCH (o:STATION {crs: 'OXF'}), (d:STATION {crs: 'PAD'}) " +
        `WITH o, d, ${interchanges} AS interchanges, ${departFrom} AS depart_from, ${departBefore} AS depart_before ` +
        FIND_LEGS;
}

const LEGS_FROM_OXFORD = findLegsFromOxford("['DID', 'RDG', 'PAD']", 470, 500);

const UNBEATEN_LEGS_FROM_OXFORD =
    LEGS_FROM_OXFORD + "WITH " + LEG_ROW + LEGS_AS_A_SORTED_LIST + "WITH " + KEEP_UNBEATEN_LEGS + " AS OTTlist ";

// Each kept leg not yet at the destination searches on from its stop, inside the window a
// connection there leaves in and toward the interchanges it has left
const SEARCH_ON_FROM_THE_KEPT_LEGS =
    "UNWIND OTTlist AS leg WITH leg WHERE NOT leg.arrived " +
    "MATCH (o:STATION {crs: leg.stop}), (d:STATION {crs: 'PAD'}) " +
    "WITH leg, o, d, leg.interchanges AS interchanges, " +
    "leg.departAfter AS depart_from, leg.departBefore AS depart_before " +
    FIND_LEGS;

const ONWARD_LEGS = [
    ["F1", "S1", "PAD", 545n],
    ["F1", "S11", "PAD", 556n],
    ["F1", "S12", "PAD", 570n],
    ["F1", "S13", "RDG", 520n],
    ["S10", "S11", "PAD", 556n],
    ["S10", "S12", "PAD", 570n],
    ["S10", "S13", "RDG", 520n],
    ["S3", "S14", "PAD", 560n],
    ["S7", "S11", "PAD", 556n],
    ["S7", "S12", "PAD", 570n],
];

function keepsUnbeaten(name: string, legs: string, keptServices: (string | null)[]): Case {
    return {
        name,
        query: `WITH ${legs} AS OTTlist RETURN [leg IN ${KEEP_UNBEATEN_LEGS} | leg.service]`,
        rows: [[keptServices]],
    };
}

// The legs from Oxford toward Paddington that board between 470 and 500 and alight at an
// interchange, with the window a connection there leaves in
export const LEG_CASES: Case[] = [
    {
        name: "finds the legs of a round",
        query: LEGS_FROM_OXFORD + "RETURN " + LEG_ROW + ", length(leg1)",
        names: ["service", "stop", "interchanges", "nlc", "departure", "arrival", "departAfter", "departBefore", "arrived", "length(leg1)"],
        rows: [
            ["S1", "PAD", [], "3087", 480n, 545n, 555n, 635n, true, 5n],
            ["S2", "DID", ["RDG", "PAD"], "3128", 490n, 505n, 509n, 550n, false, 3n],
            ["S3", "RDG", ["PAD"], "3149", 500n, 525n, 531n, 575n, false, 3n],
            ["S5", "DID", ["RDG", "PAD"], "3128", 470n, 500n, 504n, 545n, false, 3n],
            ["S6", "DID", ["RDG", "PAD"], "3128", 475n, 510n, 514n, 555n, false, 3n],
            ["S7", "DID", ["RDG", "PAD"], "3128", 490n, 502n, 506n, 547n, false, 3n],
            ["S10", "DID", ["RDG", "PAD"], "3128", 482n, 497n, 501n, 542n, false, 3n],
            ["F1", "DID", ["RDG", "PAD"], "3128", null, 470n, 474n, 545n, false, 3n],
        ],
    },
    {
        name: "orders the legs by departure with the fixed link last",
        query: LEGS_FROM_OXFORD + "RETURN firstStop.depart AS departure ORDER BY departure",
        ordered: true,
        rows: [[470n], [475n], [480n], [482n], [490n], [490n], [500n], [null]],
    },
    {
        name: "boards at both ends of the window",
        query: findLegsFromOxford("['DID', 'RDG', 'PAD', 'SWI']", 470, 500) + "RETURN DISTINCT firstStop.service",
        rows: [["S1"], ["S2"], ["S3"], ["S4"], ["S5"], ["S6"], ["S7"], ["S10"], ["F1"]],
    },
    {
        name: "boards inside a narrower window",
        query: findLegsFromOxford("['DID', 'RDG', 'PAD', 'SWI']", 471, 499) + "RETURN DISTINCT firstStop.service",
        rows: [["S1"], ["S2"], ["S4"], ["S6"], ["S7"], ["S10"], ["F1"]],
    },
    {
        name: "opens both fixed links while they run",
        query: findLegsFromOxford("['DID', 'RDG', 'PAD']", 600, 650) + "RETURN firstStop.service, inter.crs",
        rows: [["F1", "DID"], ["F2", "RDG"]],
    },
    {
        name: "opens only the first fixed link after the second closes",
        query: findLegsFromOxford("['DID', 'RDG', 'PAD']", 700, 760) + "RETURN firstStop.service, inter.crs",
        rows: [["F1", "DID"]],
    },
    {
        name: "opens no fixed link before the first opens",
        query: findLegsFromOxford("['DID', 'RDG', 'PAD']", 380, 420) + "RETURN firstStop.service, inter.crs",
        rows: [],
    },
    {
        name: "alights only where the service does not run on to the destination",
        query: findLegsFromOxford("['DID', 'RDG', 'PAD', 'SWI']", 470, 500) +
            "AND firstStop.service IN ['S1', 'S10'] RETURN firstStop.service, inter.crs, inter = d",
        rows: [["S1", "PAD", true], ["S10", "DID", false], ["S10", "SWI", false]],
    },
    {
        name: "spells the run-on as a quantified relationship",
        query: "MATCH (d:STATION {crs: 'PAD'}) MATCH (l:MODE) WHERE NOT EXISTS {(l)-[:NEXT|ARRIVE]->{2,20}(d)} RETURN count(l)",
        rows: [[27n]],
    },
    {
        name: "spells the run-on as a variable-length relationship",
        query: "MATCH (d:STATION {crs: 'PAD'}) MATCH (l:MODE) WHERE NOT EXISTS {(l)-[:NEXT|ARRIVE*2..20]->(d)} RETURN count(l)",
        rows: [[27n]],
    },
    {
        name: "takes a leg that never leaves its first stop",
        query: findLegsFromOxford("['OXF']", 470, 500) + "RETURN firstStop.service, length(leg1), firstStop = lastStop",
        rows: [
            ["S2", 2n, true],
            ["S3", 2n, true],
            ["S4", 2n, true],
            ["S5", 2n, true],
            ["S6", 2n, true],
            ["S7", 2n, true],
            ["S10", 2n, true],
            ["F1", 2n, true],
        ],
    },
    {
        name: "carries the legs into the next query",
        query: LEGS_FROM_OXFORD +
            "RETURN o, d, inter AS prevCalling, leg1 AS path, firstStop.depart AS departure ORDER BY departure " +
            "NEXT " +
            "RETURN o.crs, d.crs, prevCalling.crs, [n IN nodes(path) | coalesce(n.crs, n.service)], departure",
        rows: [
            ["OXF", "PAD", "DID", ["OXF", "S5", "S5", "DID"], 470n],
            ["OXF", "PAD", "DID", ["OXF", "S6", "S6", "DID"], 475n],
            ["OXF", "PAD", "PAD", ["OXF", "S1", "S1", "S1", "S1", "PAD"], 480n],
            ["OXF", "PAD", "DID", ["OXF", "S10", "S10", "DID"], 482n],
            ["OXF", "PAD", "DID", ["OXF", "S2", "S2", "DID"], 490n],
            ["OXF", "PAD", "DID", ["OXF", "S7", "S7", "DID"], 490n],
            ["OXF", "PAD", "RDG", ["OXF", "S3", "S3", "RDG"], 500n],
            ["OXF", "PAD", "DID", ["OXF", "F1", "F1", "DID"], null],
        ],
    },
    {
        name: "declares the window before the first match",
        query: "WITH 470 AS depart_from, 500 AS depart_before " +
            "MATCH (o:STATION {crs: 'OXF'}), (d:STATION {crs: 'PAD'}) " +
            "WITH o, d, depart_from, depart_before, ['DID', 'RDG', 'PAD'] AS interchanges " +
            FIND_LEGS +
            "RETURN firstStop.service, " +
            "coalesce(lastStop.arrive, depart_from) + inter.minInterchange AS departAfter, " +
            "coalesce(lastStop.arrive, depart_before) + inter.maxInterchange AS departBefore",
        rows: [
            ["S1", 555n, 635n],
            ["S2", 509n, 550n],
            ["S3", 531n, 575n],
            ["S5", 504n, 545n],
            ["S6", 514n, 555n],
            ["S7", 506n, 547n],
            ["S10", 501n, 542n],
            ["F1", 474n, 545n],
        ],
    },
    {
        name: "reads the window from parameters",
        query: "MATCH (o:STATION {crs: 'OXF'}), (d:STATION {crs: 'PAD'}) " +
            "WITH o, d, ['DID', 'RDG', 'PAD'] AS interchanges " +
            "MATCH leg1=(o)-[:WED]->(firstStop:MODE) -[:NEXT * 0..99]-> (lastStop:MODE)-[:ARRIVE]-(inter:STATION) " +
            "WHERE inter.crs IN interchanges " +
            "AND NOT EXISTS {(lastStop)-[:NEXT|ARRIVE]->{2,20}(d)} " +
            "AND CASE WHEN firstStop:FIXED " +
            "THEN ($depart_before > firstStop.start AND $depart_from < firstStop.end) " +
            "ELSE $depart_before >= firstStop.depart >= $depart_from " +
            "END " +
            "RETURN firstStop.service, " +
            "coalesce(lastStop.arrive, $depart_from) + inter.minInterchange AS departAfter, " +
            "coalesce(lastStop.arrive, $depart_before) + inter.maxInterchange AS departBefore",
        options: { parameters: { depart_from: 470, depart_before: 500 } },
        rows: [
            ["S1", 555n, 635n],
            ["S2", 509n, 550n],
            ["S3", 531n, 575n],
            ["S5", 504n, 545n],
            ["S6", 514n, 555n],
            ["S7", 506n, 547n],
            ["S10", 501n, 542n],
            ["F1", 474n, 545n],
        ],
    },
    {
        name: "slices the interchanges at coll.indexOf",
        query: LEGS_FROM_OXFORD + "RETURN firstStop.service, interchanges[coll.indexOf(interchanges, inter.crs)+1..]",
        rows: [
            ["S1", []],
            ["S2", ["RDG", "PAD"]],
            ["S3", ["PAD"]],
            ["S5", ["RDG", "PAD"]],
            ["S6", ["RDG", "PAD"]],
            ["S7", ["RDG", "PAD"]],
            ["S10", ["RDG", "PAD"]],
            ["F1", ["RDG", "PAD"]],
        ],
    },
];

// The legs of a round collected into OTTlist, sorted by location then arrival, and folded by
// a reduce that keeps, at each location, only the legs no other leg beats
export const PARETO_CASES: Case[] = [
    {
        name: "reads the last leg and appends one",
        query: "WITH [{nlc: 'A', departure: 480, arrival: 500}, {nlc: 'A', departure: 470, arrival: 510}] AS l " +
            "RETURN l[-1].nlc, l + {nlc: 'B'}, [l[0]], size(l)",
        rows: [[
            "A",
            [{ arrival: 500n, departure: 480n, nlc: "A" }, { arrival: 510n, departure: 470n, nlc: "A" }, { nlc: "B" }],
            [{ arrival: 500n, departure: 480n, nlc: "A" }],
            2n,
        ]],
    },
    {
        name: "slices the last leg off",
        query: "WITH [{nlc: 'A', departure: 480, arrival: 500}, {nlc: 'A', departure: 470, arrival: 510}] AS l RETURN l[..-1]",
        rows: [[[{ arrival: 500n, departure: 480n, nlc: "A" }]]],
    },
    {
        name: "collects the services latest first, the fixed links leading",
        query: "MATCH (:STATION {crs: 'OXF'})-[:WED]->(f:MODE) WITH f ORDER BY f.depart DESC, f.service RETURN collect(f.service)",
        rows: [[["F1", "F2", "S8", "S3", "S2", "S7", "S4", "S10", "S1", "S6", "S5"]]],
    },
    {
        name: "builds the legs from collected calling points",
        query: "MATCH (:STATION {crs: 'OXF'})-[:WED]->(f:MODE) WITH f ORDER BY f.depart WITH collect(f) AS fs " +
            "RETURN [x IN fs WHERE x.depart IS NOT NULL | {service: x.service, departure: x.depart}][0..2]",
        rows: [[[{ departure: 470n, service: "S5" }, { departure: 475n, service: "S6" }]]],
    },
    keepsUnbeaten("drops a leg that leaves no later and arrives no earlier",
                  "[{service: 'a', nlc: 'A', departure: 482, arrival: 497}, {service: 'b', nlc: 'A', departure: 470, arrival: 500}, " +
                  "{service: 'c', nlc: 'A', departure: 482, arrival: 497}]",
                  ["a"]),
    keepsUnbeaten("keeps a leg that leaves later and arrives later",
                  "[{service: 'a', nlc: 'A', departure: 482, arrival: 497}, {service: 'b', nlc: 'A', departure: 490, arrival: 502}]",
                  ["a", "b"]),
    keepsUnbeaten("replaces the last leg with one leaving with it but arriving earlier",
                  "[{service: 'a', nlc: 'A', departure: 482, arrival: 497}, {service: 'b', nlc: 'A', departure: 490, arrival: 502}, " +
                  "{service: 'c', nlc: 'A', departure: 490, arrival: 501}]",
                  ["a", "c"]),
    keepsUnbeaten("starts again at each location",
                  "[{service: 'a', nlc: 'A', departure: 482, arrival: 497}, {service: 'b', nlc: 'B', departure: 470, arrival: 500}]",
                  ["a", "b"]),
    keepsUnbeaten("keeps a leg after a fixed link that has no departure",
                  "[{service: 'f', nlc: 'A', departure: null, arrival: 470}, {service: 'a', nlc: 'A', departure: 482, arrival: 497}, " +
                  "{service: 'b', nlc: 'A', departure: 470, arrival: 500}]",
                  ["f", "a"]),
    keepsUnbeaten("keeps a null when there is no leg", "[]", [null]),
    {
        name: "sorts the legs by location then arrival",
        query: LEGS_FROM_OXFORD + "WITH " + LEG_ROW + LEGS_AS_A_SORTED_LIST + "RETURN [leg IN OTTlist | leg.service]",
        rows: [[["S3", "F1", "S10", "S5", "S7", "S2", "S6", "S1"]]],
    },
    {
        name: "keeps the legs of a round no other beats",
        query: UNBEATEN_LEGS_FROM_OXFORD + "RETURN [leg IN OTTlist | leg.service]",
        rows: [[["S3", "F1", "S10", "S7", "S1"]]],
    },
    {
        name: "orders the kept legs by departure then arrival",
        query: UNBEATEN_LEGS_FROM_OXFORD +
            "UNWIND OTTlist AS leg " +
            "RETURN leg.service, leg.departure, leg.arrival, leg.arrived " +
            "ORDER BY leg.departure, leg.arrived DESC, leg.arrival",
        ordered: true,
        rows: [
            ["S1", 480n, 545n, true],
            ["S10", 482n, 497n, false],
            ["S7", 490n, 502n, false],
            ["S3", 500n, 525n, false],
            ["F1", null, 470n, false],
        ],
    },
    {
        name: "keeps the legs the query before returned",
        query: LEGS_FROM_OXFORD + "RETURN " + LEG_ROW + "ORDER BY departure " +
            "NEXT " + LEGS_AS_A_SORTED_LIST + "WITH " + KEEP_UNBEATEN_LEGS + " AS OTTlist " +
            "RETURN [leg IN OTTlist | leg.service]",
        rows: [[["S3", "F1", "S10", "S7", "S1"]]],
    },
    {
        name: "collects the legs as maps",
        query: LEGS_FROM_OXFORD + "WITH " + LEG_ROW + "ORDER BY nlc DESC, arrival, departure DESC " +
            "WITH collect({service: service, nlc: nlc, departure: departure, arrival: arrival}) AS OTTlist " +
            "WITH " + KEEP_UNBEATEN_LEGS + " AS OTTlist " +
            "RETURN [leg IN OTTlist | leg.service]",
        rows: [[["S3", "F1", "S10", "S7", "S1"]]],
    },
    {
        name: "keeps the path of a leg",
        query: LEGS_FROM_OXFORD + "WITH {service: firstStop.service, path: leg1} AS leg RETURN leg.service, length(leg.path)",
        rows: [["S1", 5n], ["S2", 3n], ["S3", 3n], ["S5", 3n], ["S6", 3n], ["S7", 3n], ["S10", 3n], ["F1", 3n]],
    },
];

// The round after the pruning: F1 reaches Didcot at any time, so it catches S1 there as well
// as the trains S10 and S7 connect into. S3 reaches Reading after S1 and S11 have left it.
export const ROUND_CASES: Case[] = [
    {
        name: "searches on from every kept leg not yet arrived",
        query: UNBEATEN_LEGS_FROM_OXFORD + SEARCH_ON_FROM_THE_KEPT_LEGS +
            "RETURN leg.service, firstStop.service, inter.crs, lastStop.arrive, inter = d",
        rows: ONWARD_LEGS.map((row) => [...row, row[2] === "PAD"]),
    },
    {
        name: "chains the rounds with NEXT",
        query: LEGS_FROM_OXFORD + "RETURN " + LEG_ROW + "ORDER BY departure " +
            "NEXT " + LEGS_AS_A_SORTED_LIST + "WITH " + KEEP_UNBEATEN_LEGS + " AS OTTlist RETURN OTTlist " +
            "NEXT " + SEARCH_ON_FROM_THE_KEPT_LEGS +
            "RETURN leg.service, firstStop.service, inter.crs, lastStop.arrive",
        rows: ONWARD_LEGS,
    },
    {
        name: "ranks the journeys that reach the destination",
        query: UNBEATEN_LEGS_FROM_OXFORD + SEARCH_ON_FROM_THE_KEPT_LEGS + "AND inter = d " +
            "RETURN leg.service AS first, firstStop.service AS second, lastStop.arrive AS arrival " +
            "ORDER BY arrival, first, second",
        ordered: true,
        rows: [
            ["F1", "S1", 545n],
            ["F1", "S11", 556n],
            ["S10", "S11", 556n],
            ["S7", "S11", 556n],
            ["S3", "S14", 560n],
            ["F1", "S12", 570n],
            ["S10", "S12", 570n],
            ["S7", "S12", 570n],
        ],
    },
    {
        name: "boards the next round at the end of the leg's path",
        query: LEGS_FROM_OXFORD + "WITH " + LEG_ROW + ", d AS destination, leg1 AS path " +
            "ORDER BY nlc DESC, arrival, departure DESC " +
            "WITH collect({service: service, interchanges: interchanges, nlc: nlc, departure: departure, " +
            "arrival: arrival, departAfter: departAfter, departBefore: departBefore, arrived: arrived, " +
            "destination: destination, path: path}) AS OTTlist " +
            "WITH " + KEEP_UNBEATEN_LEGS + " AS OTTlist " +
            "UNWIND OTTlist AS leg WITH leg WHERE NOT leg.arrived " +
            "WITH leg, last(nodes(leg.path)) AS o, leg.destination AS d, leg.interchanges AS interchanges, " +
            "leg.departAfter AS depart_from, leg.departBefore AS depart_before " +
            FIND_LEGS +
            "RETURN leg.service, firstStop.service, inter.crs, lastStop.arrive",
        rows: ONWARD_LEGS,
    },
];

const COLLECTED_LEGS = LEGS_FROM_OXFORD + "WITH collect(leg1) AS legs ";

const LEG_IDS = "[n IN nodes(leg1) | id(n)], [r IN relationships(leg1) | id(r)]";

const LEG_SERVICES = ["F1", "S1", "S10", "S2", "S3", "S5", "S6", "S7"];

// A path comes back as its IDs in order: its first node, then each edge and the node it reaches
function pathOf(nodes: Value, edges: Value): bigint[] {
    const nodeIds = nodes as bigint[];
    const edgeIds = edges as bigint[];

    const ids = [nodeIds[0]];
    for (let index = 0; index < edgeIds.length; index++) {
        ids.push(edgeIds[index], nodeIds[index + 1]);
    }

    return ids;
}

// S1 runs 5 hops to Paddington, S3 3 hops to Reading, and S2, S5, S6, S7, S10 and F1 3 hops
// to Didcot. They arrive at 545, 525, 505, 500, 510, 502 and 497, and F1 at no set time.
export const LEG_PATH_CASES: Case[] = [
    {
        name: "folds over the collected legs",
        query: COLLECTED_LEGS +
            "RETURN size(legs), " +
            "reduce(longest = 0, l IN legs | CASE WHEN length(l) > longest THEN length(l) ELSE longest END), " +
            "reduce(hops = 0, l IN legs | hops + length(l)), " +
            "reduce(callingPoints = 0, l IN legs | callingPoints + size([n IN nodes(l) WHERE n:MODE]))",
        rows: [[8n, 5n, 26n, 18n]],
    },
    {
        name: "keeps the earliest arriving leg as the accumulator",
        query: COLLECTED_LEGS +
            "WITH reduce(best = null, l IN legs | " +
            "CASE WHEN nodes(l)[-2].arrive IS NULL THEN best " +
            "WHEN best IS NULL OR nodes(l)[-2].arrive < nodes(best)[-2].arrive THEN l " +
            "ELSE best END) AS best " +
            "RETURN nodes(best)[1].service, nodes(best)[-2].arrive, nodes(best)[-1].crs, length(best)",
        rows: [["S10", 497n, "DID", 3n]],
    },
    {
        name: "keeps the legs arriving before 505",
        query: COLLECTED_LEGS +
            "WITH reduce(kept = [], l IN legs | CASE WHEN nodes(l)[-2].arrive < 505 THEN kept + l ELSE kept END) AS kept " +
            "UNWIND kept AS l " +
            "RETURN nodes(l)[1].service, nodes(l)[-2].arrive, length(l)",
        rows: [["S5", 500n, 3n], ["S7", 502n, 3n], ["S10", 497n, 3n]],
    },
    {
        name: "keeps the legs that arrive earlier than the last kept",
        query: LEGS_FROM_OXFORD +
            "WITH leg1, firstStop.depart AS departure, firstStop.service AS service " +
            "ORDER BY departure, service " +
            "WITH collect(leg1) AS legs " +
            "WITH reduce(kept = [], l IN legs | " +
            "CASE WHEN size(kept) = 0 OR nodes(l)[-2].arrive < nodes(kept[-1])[-2].arrive THEN kept + l " +
            "ELSE kept END) AS kept " +
            "RETURN [l IN kept | nodes(l)[1].service]",
        rows: [[["S5", "S10"]]],
    },
    {
        name: "folds over the legs of each interchange",
        query: LEGS_FROM_OXFORD +
            "WITH inter.crs AS stop, collect(leg1) AS legs " +
            "RETURN stop, size(legs), reduce(hops = 0, l IN legs | hops + length(l)), " +
            "reduce(interchange = 0, l IN legs | interchange + nodes(l)[-1].minInterchange)",
        rows: [["DID", 6n, 18n, 24n], ["RDG", 1n, 3n, 6n], ["PAD", 1n, 5n, 10n]],
    },
    {
        name: "returns each leg as its path",
        query: LEGS_FROM_OXFORD + "RETURN firstStop.service, leg1, " + LEG_IDS,
        types: ["STRING", "GRAPH_PATH", "LIST_VIEW", "LIST_VIEW"],
        check: ({ rows }) => {
            assert.deepEqual(rows.map((row) => row[0]).sort(), LEG_SERVICES);

            for (const [service, path, nodes, edges] of rows) {
                assert.deepEqual(path, pathOf(nodes, edges), `the path of ${service}`);
                assert.equal((path as bigint[]).length, service === "S1" ? 11 : 7, `the IDs of ${service}`);
            }
        },
    },
    {
        name: "returns the collected legs as a list of paths",
        query: LEGS_FROM_OXFORD +
            "WITH collect(leg1) AS legs, collect([n IN nodes(leg1) | id(n)]) AS nodeIds, " +
            "collect([r IN relationships(leg1) | id(r)]) AS edgeIds " +
            "RETURN legs, nodeIds, edgeIds",
        types: ["LIST_VIEW", "LIST_VIEW", "LIST_VIEW"],
        check: ({ rows }) => {
            assert.equal(rows.length, 1);
            const [legs, nodeIds, edgeIds] = rows[0] as Value[][];

            assert.equal(legs.length, 8);
            legs.forEach((leg, index) => {
                assert.deepEqual(leg, pathOf(nodeIds[index], edgeIds[index]), `leg ${index}`);
            });
        },
    },
    {
        name: "returns a leg's path inside a map",
        query: LEGS_FROM_OXFORD + "RETURN {service: firstStop.service, path: leg1} AS leg, " + LEG_IDS,
        types: ["MAP_VIEW", "LIST_VIEW", "LIST_VIEW"],
        check: ({ rows }) => {
            const legs = rows.map(([leg, nodes, edges]) => ({ leg: leg as { service: string; path: Value }, nodes, edges }));
            assert.deepEqual(legs.map(({ leg }) => leg.service).sort(), LEG_SERVICES);

            for (const { leg, nodes, edges } of legs) {
                assert.deepEqual(leg.path, pathOf(nodes, edges), `the path of ${leg.service}`);
            }
        },
    },
    {
        name: "returns no path for a station with no Thursday service",
        query: "MATCH (s:STATION) OPTIONAL MATCH p = (s)-[t:THU]->(m:MODE) RETURN s.crs, p, id(s), id(t), id(m)",
        types: ["STRING", "GRAPH_PATH", "INT64", "INT64", "INT64"],
        check: ({ rows }) => {
            assert.deepEqual(rows.map((row) => row[0]).sort(), ["DID", "OXF", "PAD", "RDG", "SWI"]);

            for (const [crs, path, station, edge, callingPoint] of rows) {
                if (crs === "OXF") {
                    assert.deepEqual(path, [station, edge, callingPoint]);
                } else {
                    assert.equal(path, null, `the path of ${crs}`);
                }
            }
        },
    },
    {
        name: "returns a station alone as a path of no hop",
        query: "MATCH p = (s:STATION {crs: 'OXF'}) RETURN p, length(p), [p], {path: p}, id(s)",
        types: ["GRAPH_PATH", "UINT64", "LIST_VIEW", "MAP_VIEW", "INT64"],
        check: ({ rows }) => {
            assert.equal(rows.length, 1);
            const [path, hops, list, map, station] = rows[0];

            assert.deepEqual(path, [station]);
            assert.equal(hops, 0n);
            assert.deepEqual(list, [[station]]);
            assert.deepEqual(map, { path: [station] });
        },
    },
    {
        name: "collects the stations as paths of no hop",
        query: "MATCH p = (s:STATION) RETURN collect(p), collect(id(s))",
        check: ({ rows }) => {
            const [paths, stations] = rows[0] as Value[][];

            assert.equal(paths.length, 5);
            assert.deepEqual(paths, stations.map((station) => [station]));
        },
    },
];

const MIDNIGHT = "datetime('2026-11-04T00:00:00Z')";

// The legs and rounds above, read through the rest of the model: clock times, the minutes on
// each NEXT edge, the stations' own properties, and journeys ranked across both rounds.
// S1 runs OXF 480 - DID 495/497 - RDG 515/517 - PAD 545, so its hops take 15, 18 and 28.
export const JOURNEY_PLUS_CASES: Case[] = [
    {
        name: "times each leg on the clock",
        query: LEGS_FROM_OXFORD +
            "RETURN firstStop.service, " +
            `CASE WHEN firstStop.depart IS NULL THEN null ELSE ${MIDNIGHT} + duration({minutes: firstStop.depart}) END AS departs, ` +
            `CASE WHEN lastStop.arrive IS NULL THEN null ELSE ${MIDNIGHT} + duration({minutes: lastStop.arrive}) END AS arrives`,
        types: ["STRING", "DATE_TIME", "DATE_TIME"],
        rows: [
            ["S1", at("2026-11-04T08:00:00Z"), at("2026-11-04T09:05:00Z")],
            ["S2", at("2026-11-04T08:10:00Z"), at("2026-11-04T08:25:00Z")],
            ["S3", at("2026-11-04T08:20:00Z"), at("2026-11-04T08:45:00Z")],
            ["S5", at("2026-11-04T07:50:00Z"), at("2026-11-04T08:20:00Z")],
            ["S6", at("2026-11-04T07:55:00Z"), at("2026-11-04T08:30:00Z")],
            ["S7", at("2026-11-04T08:10:00Z"), at("2026-11-04T08:22:00Z")],
            ["S10", at("2026-11-04T08:02:00Z"), at("2026-11-04T08:17:00Z")],
            ["F1", null, null],
        ],
    },
    {
        name: "adds up the minutes on the hops of each leg",
        query: LEGS_FROM_OXFORD +
            "WITH firstStop, leg1, reduce(total = 0, r IN relationships(leg1) | total + coalesce(r.minutes, 0)) AS runs " +
            "RETURN firstStop.service, runs, duration({minutes: runs}), [r IN relationships(leg1) | type(r)]",
        types: ["STRING", "INT64", "DURATION", "LIST_VIEW"],
        rows: [
            ["S1", 61n, minutes(61), ["WED", "NEXT", "NEXT", "NEXT", "ARRIVE"]],
            ["S2", 15n, minutes(15), ["WED", "NEXT", "ARRIVE"]],
            ["S3", 25n, minutes(25), ["WED", "NEXT", "ARRIVE"]],
            ["S5", 30n, minutes(30), ["WED", "NEXT", "ARRIVE"]],
            ["S6", 35n, minutes(35), ["WED", "NEXT", "ARRIVE"]],
            ["S7", 12n, minutes(12), ["WED", "NEXT", "ARRIVE"]],
            ["S10", 15n, minutes(15), ["WED", "NEXT", "ARRIVE"]],
            ["F1", 0n, minutes(0), ["WED", "NEXT", "ARRIVE"]],
        ],
    },
    {
        name: "describes a leg as a map of its stations",
        query: LEGS_FROM_OXFORD + "AND firstStop.service IN ['S1', 'F1'] " +
            "RETURN {service: firstStop.service, from: o {.crs, .name}, to: inter {.crs, .name, .stepFree}, " +
            "departs: firstStop.depart, hops: length(leg1)} AS leg",
        types: ["MAP_VIEW"],
        rows: [
            [{ service: "S1", from: { crs: "OXF", name: "Oxford" }, to: { crs: "PAD", name: "London Paddington", stepFree: true }, departs: 480n, hops: 5n }],
            [{ service: "F1", from: { crs: "OXF", name: "Oxford" }, to: { crs: "DID", name: "Didcot Parkway", stepFree: true }, departs: null, hops: 3n }],
        ],
    },
    {
        name: "drops the legs that alight where there is no step-free access",
        query: findLegsFromOxford("['DID', 'RDG', 'PAD', 'SWI']", 470, 500) +
            "AND all(station IN [n IN nodes(leg1) WHERE n:STATION] WHERE station.stepFree) " +
            "RETURN DISTINCT firstStop.service",
        rows: [["S1"], ["S2"], ["S3"], ["S5"], ["S6"], ["S7"], ["S10"], ["F1"]],
    },
    {
        name: "boards S9 only on Thursday",
        query: "MATCH (:STATION {crs: 'OXF'})-[day]->(m:MODE) RETURN type(day), count(m), collect(DISTINCT m.day)",
        rows: [["WED", 11n, ["WED"]], ["THU", 1n, ["THU"]]],
    },
    // A null sorts first descending, so the fixed link leads each arrival
    {
        name: "ranks the direct and the two-leg journeys to Paddington",
        query: UNBEATEN_LEGS_FROM_OXFORD +
            "UNWIND OTTlist AS leg WITH leg WHERE leg.arrived " +
            "RETURN [leg.service] AS services, leg.departure AS departure, leg.arrival AS arrival " +
            "UNION " +
            UNBEATEN_LEGS_FROM_OXFORD + SEARCH_ON_FROM_THE_KEPT_LEGS + "AND inter = d " +
            "RETURN [leg.service, firstStop.service] AS services, leg.departure AS departure, lastStop.arrive AS arrival " +
            "NEXT " +
            "RETURN services, departure, arrival, arrival - departure AS minutes " +
            "ORDER BY arrival, size(services), departure DESC LIMIT 6",
        ordered: true,
        rows: [
            [["S1"], 480n, 545n, 65n],
            [["F1", "S1"], null, 545n, null],
            [["F1", "S11"], null, 556n, null],
            [["S7", "S11"], 490n, 556n, 66n],
            [["S10", "S11"], 482n, 556n, 74n],
            [["S3", "S14"], 500n, 560n, 60n],
        ],
    },
    {
        name: "lists the onward departures at each interchange",
        query: "MATCH (i:STATION) WHERE i.crs IN ['DID', 'RDG', 'SWI'] " +
            "OPTIONAL MATCH (i)-[:WED]->(m:MODE) WHERE m.depart >= 500 " +
            "WITH i, m ORDER BY m.depart " +
            "RETURN i.crs, count(m), collect(m.service)",
        rows: [["DID", 3n, ["S13", "S11", "S12"]], ["RDG", 3n, ["S1", "S11", "S14"]], ["SWI", 0n, []]],
    },
    {
        name: "ranks the stations by how alike their profile is to Reading's",
        query: "MATCH (r:STATION {crs: 'RDG'}), (s:STATION) WHERE s <> r " +
            "WITH s, cosine_similarity(r.profile, s.profile) AS similarity " +
            "RETURN s.crs, round(1000 * similarity) ORDER BY similarity DESC",
        ordered: true,
        rows: [["SWI", 998.0], ["DID", 963.0], ["PAD", 930.0], ["OXF", 875.0]],
    },
    {
        name: "measures each station's distance from Oxford",
        query: "MATCH (o:STATION {crs: 'OXF'}), (s:STATION) WHERE s <> o " +
            "WITH o, s, radians(s.lat - o.lat) AS latitudeDelta, radians(s.lon - o.lon) AS longitudeDelta " +
            "WITH s, 2 * 6371 * asin(sqrt(haversin(latitudeDelta) + cos(radians(o.lat)) * cos(radians(s.lat)) * haversin(longitudeDelta))) AS kilometres " +
            "RETURN s.crs, round(kilometres) ORDER BY kilometres",
        ordered: true,
        rows: [["DID", 16.0], ["RDG", 39.0], ["SWI", 41.0], ["PAD", 80.0]],
    },
    {
        name: "measures each station's distance from Oxford between their locations",
        query: "MATCH (o:STATION {crs: 'OXF'}), (s:STATION) WHERE s <> o " +
            "WITH s, point.distance(o.location, s.location) / 1000 AS kilometres " +
            "RETURN s.crs, round(kilometres) ORDER BY kilometres",
        ordered: true,
        rows: [["DID", 16.0], ["RDG", 39.0], ["SWI", 41.0], ["PAD", 80.0]],
    },
    {
        name: "finds the stations inside a box around Didcot and Reading",
        query: "MATCH (s:STATION) " +
            "WHERE point.withinBBox(s.location, point({longitude: -1.3, latitude: 51.4}), point({longitude: -0.9, latitude: 51.7})) " +
            "RETURN s.crs",
        rows: [["DID"], ["RDG"]],
    },
];
