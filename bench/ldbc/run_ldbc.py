#!/usr/bin/env python3
"""Runs the official LDBC SNB queries against the TuringDB v3 (MLIR) query engine.

The v3 engine is reachable through the `#v3 ` prefix of the turingdb shell in local
mode, so the runner drives one shell process over stdin: a `cd <graph>`, then each
query on a single line, with a marker line between them to segment the output. It
reports, per query, whether the engine ran it and how long it took.

The engine takes no query parameters, so each `$name` is replaced by its literal from
params/turingdb.json before the query is sent.
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time

QUERY_DIRECTORIES = ["interactive", "bi"]
MARKER = "@@@"

ROW_COUNT_PATTERN = re.compile(r"Query returned (\d+) rows")
QUERY_TIME_PATTERN = re.compile(r"Query executed in ([0-9.]+) ms")
ERROR_PATTERN = re.compile(r"\[error\] (.*)")
REASON_PREFIX = "-------*"
PARAMETER_PATTERN = re.compile(r"\$([a-zA-Z][a-zA-Z0-9_]*)")
WRITE_PATTERN = re.compile(r"\b(CREATE|MERGE|DELETE|SET|REMOVE)\b", re.IGNORECASE)

# Queries calling these reference a Neo4j library rather than the Cypher language, so
# no engine is expected to run them as written
VENDOR_PATTERNS = [("neo4j-gds", re.compile(r"\bgds\.")), ("neo4j-apoc", re.compile(r"\bapoc\."))]


def stripComments(text):
    """Removes Cypher line and block comments, leaving string literals untouched."""
    out = []
    index = 0
    quote = None

    while index < len(text):
        char = text[index]

        if quote is not None:
            out.append(char)
            if char == "\\" and index + 1 < len(text):
                out.append(text[index + 1])
                index += 2
                continue
            if char == quote:
                quote = None
            index += 1
            continue

        if char in "'\"":
            quote = char
            out.append(char)
            index += 1
            continue

        if text.startswith("//", index):
            end = text.find("\n", index)
            index = len(text) if end == -1 else end
            continue

        if text.startswith("/*", index):
            end = text.find("*/", index + 2)
            index = len(text) if end == -1 else end + 2
            continue

        out.append(char)
        index += 1

    return "".join(out)


def substituteParameters(query, parameters):
    """Replaces every $name with its literal, returning the query and any left unbound."""
    missing = set()

    def replace(match):
        name = match.group(1)
        if name not in parameters:
            missing.add(name)
            return match.group(0)
        return str(parameters[name])

    return PARAMETER_PATTERN.sub(replace, query), sorted(missing)


def flatten(query):
    """Collapses a query to the single line the shell reads per statement."""
    return " ".join(query.split())


def loadQueries(queryRoot, parameterFile, selection):
    """Reads every query file, returning name, one-line text and unbound parameters."""
    with open(parameterFile, "r", encoding="utf-8") as handle:
        allParameters = json.load(handle)

    queries = []

    for directory in QUERY_DIRECTORIES:
        path = os.path.join(queryRoot, directory)
        for fileName in sorted(os.listdir(path)):
            if not fileName.endswith(".cypher"):
                continue

            name = fileName[: -len(".cypher")]
            if selection and not any(pattern in name for pattern in selection):
                continue

            with open(os.path.join(path, fileName), "r", encoding="utf-8") as handle:
                text = handle.read()

            parameters = allParameters.get(name, {})
            substituted, missing = substituteParameters(stripComments(text), parameters)

            oneLine = flatten(substituted)

            vendor = ""
            for library, pattern in VENDOR_PATTERNS:
                if pattern.search(oneLine):
                    vendor = library
                    break

            queries.append({
                "name": name,
                "suite": directory,
                "query": oneLine,
                "missing": missing,
                "writes": WRITE_PATTERN.search(oneLine) is not None,
                "vendor": vendor,
            })

    return queries


def sortKey(query):
    """Orders queries by suite, then by the number in their name rather than by text."""
    digits = re.findall(r"\d+", query["name"])
    number = int(digits[0]) if digits else 0
    family = re.sub(r"\d+", "", query["name"])
    return (query["suite"], family, number)


def buildInput(queries, graphName, repeat):
    """Builds the shell's stdin: a cd, then each query repeated, marker-separated.

    A write needs an open change or it fails at execution, and a fresh process always
    numbers its first change 0, so the update queries run after one is checked out.
    Opening it is marked off as its own segment, or the shell prints its row count and
    timing into the preceding query's chunk and they are read as that query's result.
    """
    lines = [f"cd {graphName}"]
    changeOpened = False

    for index, query in enumerate(queries):
        if query["writes"] and not changeOpened:
            lines.append(f"sh echo {MARKER}setup:0")
            lines.append("CHANGE NEW")
            lines.append("checkout change-0")
            changeOpened = True

        for run in range(repeat):
            lines.append(f"sh echo {MARKER}{index}:{run}")
            lines.append("#v3 " + query["query"])

    lines.append(f"sh echo {MARKER}end:0")
    lines.append("quit")

    return "\n".join(lines) + "\n"


def runShell(binary, turingDir, graphName, port, text, timeout):
    """Runs one shell process over the prepared stdin and returns its combined output."""
    command = [
        "stdbuf", "-oL", "-eL",
        binary,
        "-turing-dir", turingDir,
        "-load", graphName,
        "-p", str(port),
    ]

    started = time.time()
    completed = subprocess.run(command,
                               input=text,
                               capture_output=True,
                               text=True,
                               timeout=timeout)
    elapsed = time.time() - started

    return completed.stdout + completed.stderr, elapsed


def splitSegments(output):
    """Splits the shell output into one chunk per (query, run), keyed by the markers."""
    segments = {}
    current = None
    collected = []

    for line in output.splitlines():
        marker = line.strip()
        if marker.startswith(MARKER) and ":" in marker:
            if current is not None:
                segments[current] = collected
            key = marker[len(MARKER):]
            current = None if key.startswith("end") else key
            collected = []
            continue

        if current is not None:
            collected.append(line)

    if current is not None:
        segments[current] = collected

    return segments


def summarize(lines):
    """Reads one query's output chunk into a status, a row count and a duration.

    An engine error spans several lines: the status code, the query with the offending
    span underlined, then the reason on a trailing `-------*` line. The reason is the
    part worth reporting, so it is preferred over the code that opened the block.
    """
    rowCount = None
    duration = None
    code = None
    reason = None

    for line in lines:
        rows = ROW_COUNT_PATTERN.search(line)
        if rows:
            rowCount = int(rows.group(1))

        timing = QUERY_TIME_PATTERN.search(line)
        if timing:
            duration = float(timing.group(1))

        failure = ERROR_PATTERN.search(line)
        if failure and code is None:
            code = failure.group(1).strip().split(":")[0]
            continue

        if code is not None and line.startswith(REASON_PREFIX):
            reason = line[len(REASON_PREFIX):].strip()

    if code is not None:
        error = f"{code}: {reason}" if reason else code
        return {"status": "error", "error": error, "rows": rowCount, "ms": duration}

    if duration is None:
        return {"status": "no-result", "error": None, "rows": rowCount, "ms": None}

    return {"status": "ok", "error": None, "rows": rowCount, "ms": duration}


def classify(error):
    """Buckets an engine error message by the stage that rejected the query."""
    if error is None:
        return ""

    lowered = error.lower()
    if "parser" in lowered or "syntax" in lowered:
        return "parser"
    if "analyze" in lowered or "analysis" in lowered:
        return "analyzer"
    if "not supported" in lowered or "unsupported" in lowered or "not implemented" in lowered:
        return "unsupported"
    if "assertion" in lowered or "internal error" in lowered:
        return "internal"
    return "runtime"


def report(results, output):
    """Prints the per-query table and the totals."""
    nameWidth = max(len(result["name"]) for result in results)

    print(f"{'query'.ljust(nameWidth)}  {'status':<11} {'rows':>6}  {'ms':>9}  detail")
    print("-" * (nameWidth + 45))

    for result in results:
        milliseconds = f"{result['ms']:.2f}" if result["ms"] is not None else "-"
        rows = str(result["rows"]) if result["rows"] is not None else "-"
        detail = result["error"] or ""
        if result["vendor"]:
            detail = f"[{result['vendor']}] {detail}"
        if result["missing"]:
            detail = "unbound parameters: " + ", ".join(result["missing"])
        print(f"{result['name'].ljust(nameWidth)}  {result['status']:<11} {rows:>6}  {milliseconds:>9}  {detail[:90]}")

    passed = [result for result in results if result["status"] == "ok"]
    portable = [result for result in results if not result["vendor"]]
    portablePassed = [result for result in portable if result["status"] == "ok"]

    print()
    print(f"{len(passed)}/{len(results)} queries ran on the v3 engine")
    print(f"{len(portablePassed)}/{len(portable)} once the queries calling a Neo4j library are set aside")

    blockers = {}
    for result in portable:
        if result["status"] == "ok":
            continue
        blockers.setdefault(result["error"], []).append(result["name"])

    if blockers:
        print()
        print("what stopped the rest:")
        for error, names in sorted(blockers.items(), key=lambda entry: -len(entry[1])):
            print(f"  {len(names):2d}  {error}")
            print(f"      {', '.join(names)}")

    if output:
        with open(output, "w", encoding="utf-8") as handle:
            json.dump(results, handle, indent=2)
        print(f"results written to {output}")


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    repoRoot = os.path.abspath(os.path.join(here, "..", ".."))

    parser = argparse.ArgumentParser(description="Run the LDBC SNB queries on TuringDB v3")
    parser.add_argument("--binary", default=os.path.join(repoRoot, "build/tools/turingdb/turingdb"))
    parser.add_argument("--turing-dir", required=True, help="TuringDB root directory holding the graph")
    parser.add_argument("--graph", default="ldbc", help="graph name to query")
    parser.add_argument("--queries", default=os.path.join(here, "queries"))
    parser.add_argument("--params", default=os.path.join(here, "params/turingdb.json"))
    parser.add_argument("--port", type=int, default=7799)
    parser.add_argument("--repeat", type=int, default=3, help="runs per query; the fastest is reported")
    parser.add_argument("--timeout", type=int, default=3600, help="seconds for the whole run")
    parser.add_argument("--only", nargs="*", default=[], help="substrings selecting which queries to run")
    parser.add_argument("--output", default="", help="JSON file to write the results to")
    args = parser.parse_args()

    queries = sorted(loadQueries(args.queries, args.params, args.only), key=sortKey)
    if not queries:
        print("no queries selected", file=sys.stderr)
        return 1

    print(f"running {len(queries)} queries, {args.repeat} times each, on graph '{args.graph}'")

    text = buildInput(queries, args.graph, args.repeat)
    output, elapsed = runShell(args.binary, args.turing_dir, args.graph, args.port, text, args.timeout)
    segments = splitSegments(output)

    results = []
    for index, query in enumerate(queries):
        runs = [summarize(segments.get(f"{index}:{run}", [])) for run in range(args.repeat)]
        succeeded = [run for run in runs if run["status"] == "ok"]

        if succeeded:
            best = min(succeeded, key=lambda run: run["ms"])
        else:
            best = runs[0] if runs else {"status": "no-result", "error": None, "rows": None, "ms": None}

        results.append({
            "name": query["name"],
            "suite": query["suite"],
            "status": best["status"],
            "rows": best["rows"],
            "ms": best["ms"],
            "error": best["error"],
            "stage": classify(best["error"]),
            "missing": query["missing"],
            "writes": query["writes"],
            "vendor": query["vendor"],
            "query": query["query"],
        })

    report(results, args.output)
    print(f"total wall clock: {elapsed:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
