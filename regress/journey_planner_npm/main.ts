import { TuringClient } from "turingdb";

import { CLIENT_CHECKS, ERROR_CASES, MECHANISM_CASES, PARAMETER_CASES, SYSTEM_CASES, VALUE_CASES } from "./cypher.js";
import { CaseRunner } from "./harness.js";
import { JOURNEY_PLUS_CASES, LEG_CASES, LEG_PATH_CASES, PARETO_CASES, ROUND_CASES } from "./journey.js";
import { buildTimetable, TIMETABLE_GRAPH } from "./timetable.js";
import { runVersionCases } from "./versions.js";

const url = process.env.TURINGDB_URL;
if (url === undefined) {
    throw new Error("TURINGDB_URL must name the server's query endpoint, such as http://localhost:6686/query");
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
await runner.runChecks("client", CLIENT_CHECKS);

await runVersionCases(runner);

process.exitCode = runner.report() ? 0 : 1;
