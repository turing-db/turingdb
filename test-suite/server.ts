import { join } from "node:path";

const PORT = 5566;

const sourceTestsDir = join(
	import.meta.dir,
	"..",
	"test",
	"query-test-suite",
	"tests",
);
const buildTestsDir = join(
	import.meta.dir,
	"..",
	"build",
	"build_package",
	"test",
	"query-test-suite",
	"tests",
);

async function runCli(args: string[]) {
	const proc = Bun.spawn(["turingdb-test-cli", ...args], {
		stdout: "pipe",
		stderr: "pipe",
	});

	const stdout = await new Response(proc.stdout).text();
	const stderr = await new Response(proc.stderr).text();
	const exitCode = await proc.exited;

	return { stdout, stderr, exitCode };
}

function parseJsonFromStdout(stdout: string) {
	const trimmed = stdout.trim();
	try {
		return JSON.parse(trimmed);
	} catch {
		// Try to parse the last line that looks like JSON (CLI prints JSON on a single line).
		const lines = trimmed.split("\n");
		for (let i = lines.length - 1; i >= 0; i -= 1) {
			const line = lines[i].trim();
			if (line.startsWith("{") || line.startsWith("[")) {
				return JSON.parse(line);
			}
		}
		throw new Error("No JSON found in output");
	}
}

function jsonResponse(data: unknown, status = 200) {
	return new Response(JSON.stringify(data), {
		status,
		headers: {
			"content-type": "application/json",
			"access-control-allow-origin": "*",
			"access-control-allow-methods": "GET,POST,OPTIONS",
			"access-control-allow-headers": "content-type",
		},
	});
}

function errorResponse(message: string, status = 500, details?: string) {
	return jsonResponse({ error: message, details }, status);
}

async function updateTestFile(
	dir: string,
	name: string,
	plan?: string,
	result?: string,
	query?: string,
	newName?: string,
	tags?: string[],
	writeRequired?: boolean,
	enabled?: boolean,
	disabledReason?: string,
) {
	const entries = await Array.fromAsync(
		new Bun.Glob("*.json").scan({ cwd: dir }),
	);
	for (const entry of entries) {
		const entryName = entry.replace(/\.json$/i, "");
		if (entryName !== name) continue;
		const path = join(dir, entry);
		const file = Bun.file(path);
		if (!(await file.exists())) continue;
		const text = await file.text();
		let data: any;
		try {
			data = JSON.parse(text);
		} catch {
			continue;
		}
		data.expect = data.expect ?? {};
		if (typeof plan === "string") {
			data.expect.plan = plan;
		}
		if (typeof result === "string") {
			data.expect.result = result;
		}
		if (typeof query === "string") {
			data.query = query;
		}
		if (Array.isArray(tags)) {
			data.tags = tags;
		}
		if (typeof writeRequired === "boolean") {
			data["write-required"] = writeRequired;
		}
		if (typeof enabled === "boolean") {
			data.enabled = enabled;
		}
		if (typeof disabledReason === "string") {
			if (disabledReason.trim()) {
				data["disabled-reason"] = disabledReason;
			} else {
				delete data["disabled-reason"];
			}
		}
		if (typeof newName === "string") {
			delete data.name;
			delete data.id;
			const sanitized = idToFilename(newName);
			const targetPath = join(dir, `${sanitized}.json`);
			await Bun.write(targetPath, JSON.stringify(data, null, 2) + "\n");
			if (targetPath !== path) {
				await Bun.remove(path);
			}
			return true;
		}
		delete data.name;
		delete data.id;
		await Bun.write(path, JSON.stringify(data, null, 2) + "\n");
		return true;
	}
	return false;
}

async function loadExistingNames(dir: string) {
	const names = new Set<string>();
	const entries = await Array.fromAsync(
		new Bun.Glob("*.json").scan({ cwd: dir }),
	);
	for (const entry of entries) {
		const name = entry.replace(/\.json$/i, "");
		names.add(name);
	}
	return names;
}

function idToFilename(id: string) {
	return id.replace(/[\\/]/g, "-").replace(/[^a-z0-9._-]+/gi, "-");
}

async function createTestFile(dir: string, name: string) {
	const existingNames = await loadExistingNames(dir);
	const baseName = idToFilename(name.trim()) || "new-test";
	let candidate = baseName;
	let suffix = 2;
	while (existingNames.has(candidate)) {
		candidate = `${baseName}-${suffix}`;
		suffix += 1;
	}
	while (await Bun.file(join(dir, `${candidate}.json`)).exists()) {
		candidate = `${baseName}-${suffix}`;
		suffix += 1;
	}
	const filePath = join(dir, `${candidate}.json`);
	const payload = {
		enabled: true,
		graph: "simpledb",
		query: "MATCH (n) RETURN n",
		expect: {
			plan: "",
			result: "",
		},
		tags: [] as string[],
		"write-required": false,
	};
	await Bun.write(filePath, JSON.stringify(payload, null, 2) + "\n");
	return { name: candidate, path: filePath };
}

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
			const { stdout, stderr, exitCode } = await runCli(["--list"]);
			if (exitCode !== 0) {
				return errorResponse("Failed to list tests", 500, stderr || stdout);
			}
			try {
				return jsonResponse(parseJsonFromStdout(stdout));
			} catch (err) {
				return errorResponse("Invalid list response", 500, stdout);
			}
		}

		if (pathname === "/api/run") {
			const testId = searchParams.get("test");
			if (!testId) {
				return errorResponse("Missing test parameter", 400);
			}
			const { stdout, stderr, exitCode } = await runCli(["--run", testId]);
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
			const { stdout, stderr, exitCode } = await runCli(["--run-all"]);
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
			const updatedSource = await updateTestFile(
				sourceTestsDir,
				targetName,
				body.plan,
				body.result,
				body.query,
				body.newName,
				body.tags,
				body.writeRequired,
				body.enabled,
				body.disabledReason,
			);
			let updatedBuild = false;
			try {
				updatedBuild = await updateTestFile(
					buildTestsDir,
					targetName,
					body.plan,
					body.result,
					body.query,
					body.newName,
					body.tags,
					body.writeRequired,
					body.enabled,
					body.disabledReason,
				);
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
				try {
					await createTestFile(buildTestsDir, name);
				} catch {
					// optional
				}
				return jsonResponse({ ok: true, name: created.name });
			} catch (err) {
				return errorResponse(
					"Failed to create test",
					500,
					err instanceof Error ? err.message : undefined,
				);
			}
		}

		return errorResponse("Not found", 404);
	},
});

console.log(`Query test suite API running on http://localhost:${PORT}`);
