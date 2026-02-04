import { join } from "node:path";

import {
	createTestFile,
	duplicateTestFile,
	deleteTestFile,
	errorResponse,
	getChangedTests,
	idToFilename,
	jsonResponse,
	loadExistingNames,
	loadExpectedFromFile,
	loadMainExpected,
	loadMainVersion,
	loadTestJsonFromFile,
	parseJsonFromStdout,
	runCli,
	updateTestFile,
} from "./server-utils";

const PORT = 5566;

const args = process.argv.slice(2);

const turingSrc = process.env.TURING_SRC;
if (!turingSrc) {
	throw new Error(
		"TURING_SRC is not set. Source setup.sh before running the server.",
	);
}

const cliArgIndex = args.findIndex((arg) => arg === "--cli-binary");
const cliBinary =
	cliArgIndex === -1 || !args[cliArgIndex + 1]
		? join(
				turingSrc,
				"build",
				"build_package",
				"dev-bin",
				"query_test_suite_cli",
			)
		: args[cliArgIndex + 1];

console.log(`Using test suite CLI binary ${cliBinary}`);
if (!(await Bun.file(cliBinary).exists())) {
	throw new Error(
		`Test suite CLI binary '${cliBinary}' does not exist. Build it first. ` +
			`If the binary is in a different location, pass it with --cli-binary`,
	);
}

const sourceTestsDir = join(turingSrc, "test", "query-test-suite", "tests");
const testsRelDir = "test/query-test-suite/tests";

Bun.serve({
	port: PORT,
	async fetch(req) {
		const { pathname, searchParams } = new URL(req.url);

		if (req.method === "OPTIONS") {
			return new Response(null, {
				status: 204,
				headers: {
					"access-control-allow-origin": "*",
					"access-control-allow-methods": "GET,POST,OPTIONS",
					"access-control-allow-headers": "content-type",
				},
			});
		}

		if (pathname === "/api/tests") {
			const { stdout, stderr, exitCode } = await runCli(cliBinary, [
				"--list",
			]);
			if (exitCode !== 0) {
				return errorResponse("Failed to list tests", 500, stderr || stdout);
			}
			try {
				const list = parseJsonFromStdout(stdout) as Array<
					Record<string, unknown>
				>;
				const changed = await getChangedTests(turingSrc, testsRelDir);
				const withMain = await Promise.all(
					list.map(async (test) => {
						const name = typeof test.name === "string" ? test.name : "";
						const changeInfo = name ? changed.get(name) : null;
						if (!name || !changeInfo) return test;
						if (changeInfo.isNew) {
							return { ...test, changed: true, isNew: true };
						}
						const mainVersion = await loadMainVersion(
							turingSrc,
							testsRelDir,
							name,
						);
						return mainVersion
							? { ...test, changed: true, mainVersion }
							: { ...test, changed: true };
					}),
				);
				return jsonResponse(withMain);
			} catch (err) {
				return errorResponse("Invalid list response", 500, stdout);
			}
		}

		if (pathname === "/api/expected") {
			const name = searchParams.get("name") ?? searchParams.get("test");
			if (!name) {
				return errorResponse("Missing test parameter", 400);
			}
			const expected = await loadExpectedFromFile(sourceTestsDir, name);
			if (!expected) {
				return errorResponse("Test not found", 404);
			}
			return jsonResponse(expected);
		}

		if (pathname === "/api/main") {
			const name = searchParams.get("name") ?? searchParams.get("test");
			if (!name) {
				return errorResponse("Missing test parameter", 400);
			}
			const expected = await loadMainExpected(turingSrc, testsRelDir, name);
			if (!expected) {
				return errorResponse("Test not found in main", 404);
			}
			return jsonResponse(expected);
		}

		if (pathname === "/api/test-json") {
			const name = searchParams.get("name") ?? searchParams.get("test");
			if (!name) {
				return errorResponse("Missing test parameter", 400);
			}
			const data = await loadTestJsonFromFile(sourceTestsDir, name);
			if (!data) {
				return errorResponse("Test not found", 404);
			}
			return jsonResponse(data);
		}

		if (pathname === "/api/run") {
			const testId = searchParams.get("test");
			if (!testId) {
				return errorResponse("Missing test parameter", 400);
			}
			const { stdout, stderr, exitCode } = await runCli(cliBinary, [
				"--run",
				testId,
			]);
			if (exitCode !== 0) {
				return errorResponse("Failed to run test", 500, stderr || stdout);
			}
			try {
				return jsonResponse(parseJsonFromStdout(stdout));
			} catch (err) {
				return errorResponse("Invalid run response", 500, stdout);
			}
		}

		if (pathname === "/api/run-all") {
			const { stdout, stderr, exitCode } = await runCli(cliBinary, [
				"--run-all",
			]);
			if (exitCode !== 0) {
				return errorResponse("Failed to run all tests", 500, stderr || stdout);
			}
			try {
				return jsonResponse(parseJsonFromStdout(stdout));
			} catch (err) {
				return errorResponse("Invalid run-all response", 500, stdout);
			}
		}

		if (pathname === "/api/update" && req.method === "POST") {
			const body = await req.json().catch(() => null);
			const hasPlan = typeof body?.plan === "string";
			const hasResult = typeof body?.result === "string";
			const hasQuery = typeof body?.query === "string";
			const hasNewName = typeof body?.newName === "string";
			const hasTags = Array.isArray(body?.tags);
			const hasWriteRequired = typeof body?.writeRequired === "boolean";
			const hasEnabled = typeof body?.enabled === "boolean";
			const hasDisabledReason = typeof body?.disabledReason === "string";
			const targetName =
				typeof body?.name === "string" ? idToFilename(body.name.trim()) : "";
			if (
				!targetName ||
				(!hasPlan &&
					!hasResult &&
					!hasQuery &&
					!hasNewName &&
					!hasTags &&
					!hasWriteRequired &&
					!hasEnabled &&
					!hasDisabledReason)
			) {
				return errorResponse("Invalid payload", 400);
			}
			if (hasNewName) {
				const newName = idToFilename(body.newName.trim());
				if (!newName) {
					return errorResponse("Invalid payload", 400);
				}
				const existingNames = await loadExistingNames(sourceTestsDir);
				if (existingNames.has(newName) && newName !== targetName) {
					return errorResponse("Test name already exists", 409);
				}
			}
			const updatedSource = await updateTestFile(sourceTestsDir, targetName, {
				plan: body.plan,
				result: body.result,
				query: body.query,
				newName: body.newName,
				tags: body.tags,
				writeRequired: body.writeRequired,
				enabled: body.enabled,
				disabledReason: body.disabledReason,
			});
			let updatedBuild = false;
			try {
				updatedBuild = await updateTestFile(sourceTestsDir, targetName, {
					plan: body.plan,
					result: body.result,
					query: body.query,
					newName: body.newName,
					tags: body.tags,
					writeRequired: body.writeRequired,
					enabled: body.enabled,
					disabledReason: body.disabledReason,
				});
			} catch {
				updatedBuild = false;
			}
			if (!updatedSource && !updatedBuild) {
				return errorResponse("Test not found", 404);
			}
			return jsonResponse({ ok: true, updatedSource, updatedBuild });
		}

		if (pathname === "/api/create" && req.method === "POST") {
			const body = await req.json().catch(() => null);
			const name = typeof body?.name === "string" ? body.name.trim() : "";
			if (!name) {
				return errorResponse("Invalid payload", 400);
			}
			try {
				const created = await createTestFile(sourceTestsDir, name);
				return jsonResponse({ ok: true, name: created.name });
			} catch (err) {
				return errorResponse(
					"Failed to create test",
					500,
					err instanceof Error ? err.message : undefined,
				);
			}
		}

		if (pathname === "/api/delete" && req.method === "POST") {
			const body = await req.json().catch(() => null);
			const name = typeof body?.name === "string" ? body.name.trim() : "";
			if (!name) {
				return errorResponse("Invalid payload", 400);
			}
			const targetName = idToFilename(name);
			const deletedSource = await deleteTestFile(sourceTestsDir, targetName);
			if (!deletedSource) {
				return errorResponse("Test not found", 404);
			}
			return jsonResponse({ ok: true, deletedSource });
		}

		if (pathname === "/api/duplicate" && req.method === "POST") {
			const body = await req.json().catch(() => null);
			const name = typeof body?.name === "string" ? body.name.trim() : "";
			if (!name) {
				return errorResponse("Invalid payload", 400);
			}
			const duplicated = await duplicateTestFile(sourceTestsDir, name);
			if (!duplicated) {
				return errorResponse("Test not found", 404);
			}
			return jsonResponse({ ok: true, name: duplicated.name });
		}

		return errorResponse("Not found", 404);
	},
});

console.log(`Query test suite API running on http://localhost:${PORT}`);
