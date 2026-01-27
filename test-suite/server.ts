import { join } from "node:path";

const PORT = 5566;

const cliPath =
  process.env.QUERY_TEST_SUITE_CLI ??
  join(import.meta.dir, "..", "build", "build_package", "test", "query-test-suite", "Debug", "query_test_suite_cli");
const sourceTestsDir = join(import.meta.dir, "..", "test", "query-test-suite", "tests");
const buildTestsDir = join(import.meta.dir, "..", "build", "build_package", "test", "query-test-suite", "tests");

async function runCli(args: string[]) {
  const proc = Bun.spawn([cliPath, ...args], {
    stdout: "pipe",
    stderr: "pipe"
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
      "access-control-allow-headers": "content-type"
    }
  });
}

function errorResponse(message: string, status = 500, details?: string) {
  return jsonResponse({ error: message, details }, status);
}

async function updateTestFile(dir: string, id: string, plan?: string, result?: string) {
    const entries = await Array.fromAsync(new Bun.Glob("*.json").scan({ cwd: dir }));
    for (const entry of entries) {
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
    if (data?.id !== id) continue;
        data.expect = data.expect ?? {};
        if (typeof plan === "string") {
          data.expect.plan = plan;
        }
        if (typeof result === "string") {
          data.expect.result = result;
        }
        await Bun.write(path, JSON.stringify(data, null, 2) + "\n");
        return true;
    }
    return false;
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
          "access-control-allow-headers": "content-type"
        }
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
      if (!body?.id || (!hasPlan && !hasResult)) {
        return errorResponse("Invalid payload", 400);
      }
      const updatedSource = await updateTestFile(sourceTestsDir, body.id, body.plan, body.result);
      let updatedBuild = false;
      try {
        updatedBuild = await updateTestFile(buildTestsDir, body.id, body.plan, body.result);
      } catch {
        updatedBuild = false;
      }
      if (!updatedSource && !updatedBuild) {
        return errorResponse("Test not found", 404);
      }
      return jsonResponse({ ok: true, updatedSource, updatedBuild });
    }

    return errorResponse("Not found", 404);
  }
});

console.log(`Query test suite API running on http://localhost:${PORT}`);
console.log(`Using CLI: ${cliPath}`);
