import { rm } from "node:fs/promises";
import { join } from "node:path";

export type ExecResult = {
	stdout: string;
	stderr: string;
	exitCode: number;
};

export type ChangeStatus = {
	status: string;
	isNew: boolean;
};

export type ExpectedOutput = {
	plan: string;
	result: string;
};

export type UpdateTestOptions = {
	plan?: string;
	result?: string;
	query?: string;
	newName?: string;
	tags?: string[];
	writeRequired?: boolean;
	enabled?: boolean;
	disabledReason?: string;
};

export async function runGit(cwd: string, args: string[]): Promise<ExecResult> {
	const proc = Bun.spawn(["git", ...args], {
		cwd,
		stdout: "pipe",
		stderr: "pipe",
	});
	const stdout = await new Response(proc.stdout).text();
	const stderr = await new Response(proc.stderr).text();
	const exitCode = await proc.exited;
	return { stdout, stderr, exitCode };
}

export async function runCli(
	cliBinary: string,
	args: string[],
): Promise<ExecResult> {
	const proc = Bun.spawn([cliBinary, ...args], {
		stdout: "pipe",
		stderr: "pipe",
	});
	const stdout = await new Response(proc.stdout).text();
	const stderr = await new Response(proc.stderr).text();
	const exitCode = await proc.exited;
	return { stdout, stderr, exitCode };
}

export function parseJsonFromStdout(stdout: string): unknown {
	const trimmed = stdout.trim();
	try {
		return JSON.parse(trimmed);
	} catch {
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

export function jsonResponse(data: unknown, status = 200): Response {
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

export function errorResponse(
	message: string,
	status = 500,
	details?: string,
): Response {
	return jsonResponse({ error: message, details }, status);
}

export function idToFilename(id: string): string {
	return id.replace(/[\\/]/g, "-").replace(/[^a-z0-9._-]+/gi, "-");
}

export async function getChangedTests(
	cwd: string,
	testsRelDir: string,
): Promise<Map<string, ChangeStatus>> {
	const { stdout, exitCode } = await runGit(cwd, [
		"diff",
		"--name-status",
		"main",
		"--",
		testsRelDir,
	]);
	const statusMap = new Map<string, ChangeStatus>();
	if (exitCode === 0) {
		for (const line of stdout.split("\n")) {
			const trimmed = line.trim();
			if (!trimmed) continue;
			const [status, file] = trimmed.split(/\s+/, 2);
			if (!file) continue;
			const base = file.split("/").pop();
			if (!base || !base.endsWith(".json")) continue;
			const name = base.replace(/\.json$/i, "");
			const isNew = status.startsWith("A");
			statusMap.set(name, { status, isNew });
		}
	}

	const untracked = await runGit(cwd, [
		"status",
		"--porcelain",
		"main",
		"--",
		testsRelDir,
	]);
	if (untracked.exitCode === 0) {
		for (const line of untracked.stdout.split("\n")) {
			const trimmed = line.trim();
			if (!trimmed.startsWith("??")) continue;
			const file = trimmed.slice(2).trim();
			const base = file.split("/").pop();
			if (!base || !base.endsWith(".json")) continue;
			const name = base.replace(/\.json$/i, "");
			statusMap.set(name, { status: "??", isNew: true });
		}
	}
	return statusMap;
}

export async function loadMainVersion(
	cwd: string,
	testsRelDir: string,
	name: string,
): Promise<Record<string, unknown> | null> {
	const relPath = `${testsRelDir}/${name}.json`;
	const { stdout, exitCode } = await runGit(cwd, [
		"show",
		`main:${relPath}`,
	]);
	if (exitCode !== 0 || !stdout.trim()) return null;
	try {
		return JSON.parse(stdout) as Record<string, unknown>;
	} catch {
		return null;
	}
}

export async function loadMainExpected(
	cwd: string,
	testsRelDir: string,
	name: string,
): Promise<ExpectedOutput | null> {
	const data = await loadMainVersion(cwd, testsRelDir, name);
	if (!data) return null;
	const expect = (data as { expect?: ExpectedOutput }).expect ?? {};
	return {
		plan: typeof expect.plan === "string" ? expect.plan : "",
		result: typeof expect.result === "string" ? expect.result : "",
	};
}

async function findTestPath(dir: string, name: string): Promise<string | null> {
	const rawName = name.trim();
	const sanitized = idToFilename(rawName);
	if (!sanitized) return null;
	const entries = await Array.fromAsync(
		new Bun.Glob("*.json").scan({ cwd: dir }),
	);
	for (const entry of entries) {
		const entryName = entry.replace(/\.json$/i, "");
		if (entryName === sanitized || entryName === rawName) {
			return join(dir, entry);
		}
	}
	return join(dir, `${sanitized}.json`);
}

export async function loadExpectedFromFile(
	dir: string,
	name: string,
): Promise<ExpectedOutput | null> {
	const path = await findTestPath(dir, name);
	if (!path) return null;
	const file = Bun.file(path);
	if (!(await file.exists())) return null;
	let data: { expect?: ExpectedOutput } | undefined;
	try {
		data = JSON.parse(await file.text()) as { expect?: ExpectedOutput };
	} catch {
		return null;
	}
	const expect = data?.expect ?? {};
	return {
		plan: typeof expect.plan === "string" ? expect.plan : "",
		result: typeof expect.result === "string" ? expect.result : "",
	};
}

export async function loadTestJsonFromFile(
	dir: string,
	name: string,
): Promise<Record<string, unknown> | null> {
	const path = await findTestPath(dir, name);
	if (!path) return null;
	const file = Bun.file(path);
	if (!(await file.exists())) return null;
	try {
		return JSON.parse(await file.text()) as Record<string, unknown>;
	} catch {
		return null;
	}
}

export async function loadExistingNames(dir: string): Promise<Set<string>> {
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

export async function updateTestFile(
	dir: string,
	name: string,
	options: UpdateTestOptions,
): Promise<boolean> {
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
		let data: Record<string, unknown>;
		try {
			data = JSON.parse(text) as Record<string, unknown>;
		} catch {
			continue;
		}
		const expect =
			(data.expect as Record<string, unknown> | undefined) ?? {};
		data.expect = expect;
		if (typeof options.plan === "string") {
			expect.plan = options.plan;
		}
		if (typeof options.result === "string") {
			expect.result = options.result;
		}
		if (typeof options.query === "string") {
			data.query = options.query;
		}
		if (Array.isArray(options.tags)) {
			data.tags = options.tags;
		}
		if (typeof options.writeRequired === "boolean") {
			data["write-required"] = options.writeRequired;
		}
		if (typeof options.enabled === "boolean") {
			data.enabled = options.enabled;
		}
		if (typeof options.disabledReason === "string") {
			if (options.disabledReason.trim()) {
				data["disabled-reason"] = options.disabledReason;
			} else {
				delete data["disabled-reason"];
			}
		}
		if (typeof options.newName === "string") {
			delete data.name;
			delete data.id;
			const sanitized = idToFilename(options.newName);
			const targetPath = join(dir, `${sanitized}.json`);
			await Bun.write(targetPath, JSON.stringify(data, null, 2) + "\n");
			if (targetPath !== path) {
				await rm(path);
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

export async function createTestFile(
	dir: string,
	name: string,
): Promise<{ name: string; path: string }> {
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

export async function deleteTestFile(
	dir: string,
	name: string,
): Promise<boolean> {
	const entries = await Array.fromAsync(
		new Bun.Glob("*.json").scan({ cwd: dir }),
	);
	for (const entry of entries) {
		const entryName = entry.replace(/\.json$/i, "");
		if (entryName !== name) continue;
		const path = join(dir, entry);
		const file = Bun.file(path);
		if (!(await file.exists())) return false;
		await rm(path);
		return true;
	}
	return false;
}
