import type { CaseRunner } from "./harness.js";

// The rail timetable of test/query/ir/journey/JourneyTest.cpp, built over the wire: MODE
// calling points chained by NEXT, each ARRIVEs at its STATION, and a WED or THU edge runs from
// a station to each calling point boardable there. Times are minutes after midnight. The
// stations also hold one property of every value type, and each NEXT edge its hop's minutes.
export const TIMETABLE_GRAPH = "timetable";

type Literal = string | number | boolean | null | Literal[] | { [key: string]: Literal | undefined };

type Call = {
    crs: string;
    arrive?: number;
    depart?: number;
};

type Service = {
    service: string;
    day: string;
    calls: Call[];
};

type FixedLink = {
    service: string;
    from: string;
    to: string;
    start: number;
    end: number;
};

const STATIONS =
    "CREATE (:STATION {crs: 'OXF', nlc: '3115', name: 'Oxford', minInterchange: 5, maxInterchange: 60, lat: 51.7535, lon: -1.27, location: point({longitude: -1.27, latitude: 51.7535}), " +
    "stepFree: true, opened: datetime('1852-10-01T00:00:00Z'), minConnection: duration({minutes: 5}), platforms: [1, 2, 3, 4], " +
    "facilities: {parking: true, cycleSpaces: 400, retail: ['café', 'newsagent']}, profile: (0.9, 0.3, 0.5), sign: 'Oxford 🚆'}), " +
    "(:STATION {crs: 'DID', nlc: '3128', name: 'Didcot Parkway', minInterchange: 4, maxInterchange: 45, lat: 51.611, lon: -1.2429, location: point({longitude: -1.2429, latitude: 51.611}), " +
    "stepFree: true, opened: datetime('1844-06-12T00:00:00Z'), minConnection: duration({minutes: 4}), platforms: [1, 2, 3, 4, 5], " +
    "facilities: {parking: true, cycleSpaces: 120, retail: ['café']}, profile: (0.4, 0.8, 0.2)}), " +
    "(:STATION {crs: 'RDG', nlc: '3149', name: 'Reading', minInterchange: 6, maxInterchange: 50, lat: 51.4589, lon: -0.9718, location: point({longitude: -0.9718, latitude: 51.4589}), " +
    "stepFree: true, opened: datetime('1840-03-30T00:00:00Z'), minConnection: duration({minutes: 6}), platforms: [7, 8, 9, 10, 11, 12, 13, 14, 15], " +
    "facilities: {parking: true, cycleSpaces: 800, retail: ['café', 'pharmacy', 'bookshop']}, profile: (0.8, 0.9, 0.4)}), " +
    "(:STATION {crs: 'PAD', nlc: '3087', name: 'London Paddington', minInterchange: 10, maxInterchange: 90, lat: 51.5154, lon: -0.1755, location: point({longitude: -0.1755, latitude: 51.5154}), " +
    "stepFree: true, opened: datetime('1854-01-16T00:00:00Z'), minConnection: duration({minutes: 10}), platforms: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14], " +
    "facilities: {parking: false, cycleSpaces: 250, retail: ['bookshop', 'café', 'florist']}, profile: (1.0, 0.7, 0.9)}), " +
    "(:STATION {crs: 'SWI', nlc: '3138', name: 'Swindon', minInterchange: 5, maxInterchange: 40, lat: 51.5655, lon: -1.7856, location: point({longitude: -1.7856, latitude: 51.5655}), " +
    "stepFree: false, opened: datetime('1842-07-17T00:00:00Z'), minConnection: duration({minutes: 5}), platforms: [1, 2, 3, 4], " +
    "facilities: {parking: true, cycleSpaces: 60, retail: []}, profile: (0.5, 0.6, 0.3)})";

const SERVICES: Service[] = [
    { service: "S1", day: "WED", calls: [{ crs: "OXF", depart: 480 }, { crs: "DID", arrive: 495, depart: 497 }, { crs: "RDG", arrive: 515, depart: 517 }, { crs: "PAD", arrive: 545 }] },
    { service: "S2", day: "WED", calls: [{ crs: "OXF", depart: 490 }, { crs: "DID", arrive: 505 }] },
    { service: "S3", day: "WED", calls: [{ crs: "OXF", depart: 500 }, { crs: "RDG", arrive: 525 }] },
    { service: "S4", day: "WED", calls: [{ crs: "OXF", depart: 485 }, { crs: "SWI", arrive: 520 }] },
    { service: "S5", day: "WED", calls: [{ crs: "OXF", depart: 470 }, { crs: "DID", arrive: 500 }] },
    { service: "S6", day: "WED", calls: [{ crs: "OXF", depart: 475 }, { crs: "DID", arrive: 510 }] },
    { service: "S7", day: "WED", calls: [{ crs: "OXF", depart: 490 }, { crs: "DID", arrive: 502 }] },
    { service: "S8", day: "WED", calls: [{ crs: "OXF", depart: 520 }, { crs: "PAD", arrive: 580 }] },
    { service: "S9", day: "THU", calls: [{ crs: "OXF", depart: 480 }, { crs: "PAD", arrive: 540 }] },
    { service: "S10", day: "WED", calls: [{ crs: "OXF", depart: 482 }, { crs: "DID", arrive: 497, depart: 499 }, { crs: "SWI", arrive: 530 }] },
    { service: "S11", day: "WED", calls: [{ crs: "DID", depart: 510 }, { crs: "RDG", arrive: 524, depart: 526 }, { crs: "PAD", arrive: 556 }] },
    { service: "S12", day: "WED", calls: [{ crs: "DID", depart: 540 }, { crs: "PAD", arrive: 570 }] },
    { service: "S13", day: "WED", calls: [{ crs: "DID", depart: 505 }, { crs: "RDG", arrive: 520 }] },
    { service: "S14", day: "WED", calls: [{ crs: "RDG", depart: 535 }, { crs: "PAD", arrive: 560 }] },
];

const FIXED_LINKS: FixedLink[] = [
    { service: "F1", from: "OXF", to: "DID", start: 420, end: 1380 },
    { service: "F2", from: "OXF", to: "RDG", start: 600, end: 700 },
];

function cypherLiteral(value: Literal | undefined): string {
    if (value === null || value === undefined) {
        return "null";
    } else if (typeof value === "string") {
        return `'${value.replaceAll("\\", "\\\\").replaceAll("'", "\\'")}'`;
    } else if (typeof value === "number" || typeof value === "boolean") {
        return value.toString();
    } else if (Array.isArray(value)) {
        return `[${value.map(cypherLiteral).join(", ")}]`;
    }

    const entries = Object.entries(value).filter(([, entry]) => entry !== undefined);
    return `{${entries.map(([key, entry]) => `${key}: ${cypherLiteral(entry)}`).join(", ")}}`;
}

// The calling points of every service, one row per call. A value read out of a map is cast
// to its type: the engine rejects a property created from a map value as typed only per row.
const CREATE_CALLING_POINTS =
    `UNWIND ${cypherLiteral(SERVICES)} AS service ` +
    "UNWIND range(0, size(service.calls) - 1) AS seq " +
    "WITH service, seq, service.calls[seq] AS call " +
    "MATCH (station:STATION {crs: call.crs}) " +
    "CREATE (station)<-[:ARRIVE]-(:MODE {service: toString(service.service), day: toString(service.day), seq: seq, " +
    "nlc: station.nlc, arrive: toInteger(call.arrive), depart: toInteger(call.depart)})";

const CREATE_FIXED_LINKS =
    `UNWIND ${cypherLiteral(FIXED_LINKS)} AS link ` +
    "MATCH (from:STATION {crs: link.from}), (to:STATION {crs: link.to}) " +
    "CREATE (from)<-[:ARRIVE]-(:MODE:FIXED {service: toString(link.service), day: 'WED', seq: 0, nlc: from.nlc, " +
    "start: toInteger(link.start), end: toInteger(link.end)}), " +
    "(to)<-[:ARRIVE]-(:MODE {service: toString(link.service), day: 'WED', seq: 1, nlc: to.nlc})";

const CREATE_NEXT_EDGES =
    "MATCH (a:MODE), (b:MODE) WHERE a.service = b.service AND b.seq = a.seq + 1 " +
    "CREATE (a)-[:NEXT {minutes: b.arrive - a.depart}]->(b)";

const CREATE_WEDNESDAY_EDGES =
    "MATCH (s:STATION)<-[:ARRIVE]-(m:MODE {day: 'WED'}) WHERE m.depart IS NOT NULL OR m:FIXED " +
    "CREATE (s)-[:WED]->(m)";

const CREATE_THURSDAY_EDGES =
    "MATCH (s:STATION)<-[:ARRIVE]-(m:MODE {day: 'THU'}) WHERE m.depart IS NOT NULL " +
    "CREATE (s)-[:THU]->(m)";

// A MATCH inside a change sees only what the change has committed, so each step commits
// the nodes the next one matches
export async function buildTimetable(runner: CaseRunner): Promise<void> {
    await runner.execute(`CREATE GRAPH ${TIMETABLE_GRAPH}`, { graph: "default" });

    const newChange = await runner.execute("CHANGE NEW");
    const change = newChange.rows[0][0] as bigint;

    await runner.execute(STATIONS, { change });
    await runner.execute("COMMIT", { change });

    await runner.execute(CREATE_CALLING_POINTS, { change });
    await runner.execute(CREATE_FIXED_LINKS, { change });
    await runner.execute("COMMIT", { change });

    await runner.execute(CREATE_NEXT_EDGES, { change });
    await runner.execute(CREATE_WEDNESDAY_EDGES, { change });
    await runner.execute(CREATE_THURSDAY_EDGES, { change });
    await runner.execute("COMMIT", { change });

    await runner.execute("CHANGE SUBMIT", { change });
}
