import assert from "node:assert/strict";

import type { QueryOptions } from "turingdb";

import type { Case, CaseRunner } from "./harness.js";

// A second change rewrites Wednesday's timetable: S8 (OXF 520 - PAD 580) is cancelled and
// S15 (OXF 530 - PAD 585) runs instead. Oxford gains a staffed flag and loses its sign,
// Swindon gains step-free access and a platform, and MERGE adds Hayes once.
const CANCEL_S8 = "MATCH (m:MODE {service: 'S8'}) DETACH DELETE m";

const ADD_S15 =
    "MATCH (o:STATION {crs: 'OXF'}), (p:STATION {crs: 'PAD'}) " +
    "CREATE (o)-[:WED]->(a:MODE {service: 'S15', day: 'WED', seq: 0, nlc: '3115', depart: 530})-[:ARRIVE]->(o), " +
    "(a)-[:NEXT {minutes: 55}]->(b:MODE {service: 'S15', day: 'WED', seq: 1, nlc: '3087', arrive: 585})-[:ARRIVE]->(p)";

const UPDATE_STATIONS = [
    "MATCH (s:STATION {crs: 'OXF'}) SET s += {staffed: true} REMOVE s.sign",
    "MATCH (s:STATION {crs: 'SWI'}) SET s.stepFree = true, s.platforms = s.platforms + [5]",
    "MERGE (s:STATION {crs: 'HAY'}) ON CREATE SET s.name = 'Hayes & Harlington'",
];

const DIRECT_TO_PADDINGTON =
    "MATCH (:STATION {crs: 'OXF'})-[:WED]->(m:MODE)-[:NEXT*]->(:MODE)-[:ARRIVE]->(:STATION {crs: 'PAD'}) " +
    "RETURN m.service, m.depart";

const STATION_CHANGES =
    "MATCH (s:STATION) WHERE s.crs IN ['OXF', 'SWI', 'HAY'] " +
    "RETURN s.crs, s.name, coalesce(s.staffed, false), s.sign, s.stepFree, size(s.platforms)";

const BEFORE = {
    direct: [["S1", 480n], ["S8", 520n]],
    stations: [
        ["OXF", "Oxford", false, "Oxford 🚆", true, 4n],
        ["SWI", "Swindon", false, null, false, 4n],
    ],
};

const AFTER = {
    direct: [["S1", 480n], ["S15", 530n]],
    stations: [
        ["OXF", "Oxford", true, null, true, 4n],
        ["SWI", "Swindon", false, null, true, 5n],
        ["HAY", "Hayes & Harlington", false, null, null, null],
    ],
};

function versionCases(label: string, options: QueryOptions, expected: typeof BEFORE): Case[] {
    return [
        { name: `${label}: the direct services to Paddington`, query: DIRECT_TO_PADDINGTON, options, rows: expected.direct },
        { name: `${label}: the stations`, query: STATION_CHANGES, options, rows: expected.stations },
    ];
}

function headCommit(history: string[]): string {
    return history[0].replace("(HEAD)", "");
}

async function readHistory(runner: CaseRunner): Promise<string[]> {
    const { rows } = await runner.execute("CALL db.history()");
    return rows.map((row) => row[0] as string);
}

// Runs the second change step by step, checking what each version sees on the way
export async function runVersionCases(runner: CaseRunner): Promise<void> {
    const submittedTimetable = headCommit(await readHistory(runner));

    const newChange = await runner.execute("CHANGE NEW");
    const change = newChange.rows[0][0] as bigint;

    await runner.runChecks("versions: a change", [
        {
            name: "numbers the change it opens with a CHANGE_ID",
            run: async () => {
                assert.deepEqual(newChange.types, ["CHANGE_ID"]);
                assert.equal(typeof change, "bigint");
            },
        },
    ]);

    await runner.execute(CANCEL_S8, { change });
    await runner.execute(ADD_S15, { change });
    for (const update of UPDATE_STATIONS) {
        await runner.execute(update, { change });
    }

    await runner.run("versions: written, not committed", [
        {
            name: "the change does not read the nodes it created before COMMIT",
            query: "MATCH (n) WHERE n.service = 'S15' OR n.crs = 'HAY' RETURN count(n)",
            options: { change },
            rows: [[0n]],
        },
    ]);

    await runner.execute("COMMIT", { change });

    await runner.run("versions: committed in the change", [
        ...versionCases("the change", { change }, AFTER),
        ...versionCases("main", {}, BEFORE),
        {
            name: "MERGE matches the station it created rather than adding a second",
            query: "MERGE (s:STATION {crs: 'HAY'}) RETURN s.name",
            options: { change },
            rows: [["Hayes & Harlington"]],
        },
    ]);

    await runner.execute("CHANGE SUBMIT", { change });

    await runner.run("versions: submitted", [
        ...versionCases("main", {}, AFTER),
        ...versionCases("the commit before the change", { commit: submittedTimetable }, BEFORE),
        {
            name: "the history keeps the commit before the change below a new head",
            query: "CALL db.history() YIELD commit RETURN commit",
            check: ({ rows }) => {
                const commits = rows.map((row) => row[0] as string);

                assert.ok(commits[0].endsWith("(HEAD)"));
                assert.notEqual(headCommit(commits), submittedTimetable);
                assert.ok(commits.includes(submittedTimetable));
            },
        },
    ]);
}
