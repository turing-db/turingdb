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
				"test",
				"query-test-suite",
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

async function runCliJsonResponse(
    args: string[],
    runErrorMessage: string,
    parseErrorMessage: string,
    ): Promise<Response> {
  const {stdout, stderr, exitCode} = await runCli(cliBinary, args);
  if (exitCode !== 0) {
    return errorResponse(runErrorMessage, 500, stderr || stdout);
  }
  try {
    return jsonResponse(parseJsonFromStdout(stdout));
  } catch {
    return errorResponse(parseErrorMessage, 500, stdout);
  }
}

Bun.serve({
	port: PORT,
	// A full run-all invocation shells out to the C++ CLI and streams nothing back
	// until every test has run, so the connection sits idle far longer than Bun's
	// 10s default. 255 is the maximum idleTimeout Bun accepts.
	idleTimeout: 255,
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
                  const name =
                      searchParams.get("name") ?? searchParams.get("test");
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
                  return runCliJsonResponse(
                      [ "--run", testId ],
                      "Failed to run test",
                      "Invalid run response",
                  );
                }

                if (pathname === "/api/run-remote") {
                  const testId = searchParams.get("test");
                  if (!testId) {
                    return errorResponse("Missing test parameter", 400);
                  }
                  return runCliJsonResponse(
                      [ "--run-remote", testId ],
                      "Failed to run remote test",
                      "Invalid remote run response",
                  );
                }

                if (pathname === "/api/run-v3") {
                  const testId = searchParams.get("test");
                  if (!testId) {
                    return errorResponse("Missing test parameter", 400);
                  }
                  return runCliJsonResponse(
                      [ "--run-v3", testId ],
                      "Failed to run v3 test",
                      "Invalid v3 run response",
                  );
                }

                if (pathname === "/api/run-all") {
			return runCliJsonResponse(
				["--run-all"],
				"Failed to run all tests",
				"Invalid run-all response",
			);
		}

		if (pathname === "/api/run-all-remote") {
			return runCliJsonResponse(
				["--run-all-remote"],
				"Failed to run all remote tests",
				"Invalid remote run-all response",
			);
		}

		if (pathname === "/api/run-all-v3") {
			return runCliJsonResponse(
				["--run-all-v3"],
				"Failed to run all v3 tests",
				"Invalid v3 run-all response",
			);
		}

		if (pathname === "/api/update" && req.method === "POST") {
			const body = await req.json().catch(() => null);
			const hasPlan = typeof body?.plan === "string";
			const hasResult = typeof body?.result === "string";
			const hasResultJson = typeof body?.resultJson === "string";
			const hasMlir = typeof body?.mlir === "string";
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
					!hasResultJson &&
					!hasMlir &&
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
				resultJson: body.resultJson,
				mlir: body.mlir,
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
					resultJson: body.resultJson,
					mlir: body.mlir,
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
