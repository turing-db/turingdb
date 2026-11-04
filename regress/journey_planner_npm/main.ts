import { TuringClient } from "turingdb";

import { authChecks, clientChecks, ERROR_CASES, MECHANISM_CASES, PARAMETER_CASES, SYSTEM_CASES, VALUE_CASES } from "./cypher.js";
import { CaseRunner } from "./harness.js";
import { JOURNEY_PLUS_CASES, LEG_CASES, LEG_PATH_CASES, PARETO_CASES, ROUND_CASES } from "./journey.js";
import { buildTimetable, TIMETABLE_GRAPH } from "./timetable.js";
import { runVersionCases } from "./versions.js";

const url = process.env.TURINGDB_URL;
const authUrl = process.env.TURINGDB_AUTH_URL;
const authToken = process.env.TURINGDB_AUTH_TOKEN;
if (url === undefined || authUrl === undefined || authToken === undefined) {
    throw new Error("TURINGDB_URL must name the server's query endpoint, such as http://localhost:6686/query, " +
        "and TURINGDB_AUTH_URL and TURINGDB_AUTH_TOKEN those of a server started with -auth-on");
}

const runner = new CaseRunner(new TuringClient({ url, graph: TIMETABLE_GRAPH }));

await buildTimetable(runner);

await runner.run("values", VALUE_CASES);
await runner.run("mechanisms", MECHANISM_CASES);
await runner.run("journey legs", LEG_CASES);
await runner.run("journey pareto", PARETO_CASES);
await runner.run("journey rounds", ROUND_CASES);
await runner.run("journey leg paths", LEG_PATH_CASES);
await runner.run("journey plus", JOURNEY_PLUS_CASES);
await runner.run("parameters", PARAMETER_CASES);
await runner.run("system", SYSTEM_CASES);
await runner.run("errors", ERROR_CASES);
await runner.runChecks("client", clientChecks(url));
await runner.runChecks("authentication", authChecks(authUrl, authToken));

await runVersionCases(runner);

process.exitCode = runner.report() ? 0 : 1;
