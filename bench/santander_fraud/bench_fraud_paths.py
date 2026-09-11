#!/usr/bin/env python3
"""
Benchmark variable-length path queries on a synthetic bank-fraud graph, v2 against v3.

The graph comes from turing-db/gen-fraud-graph, Santander's open-source fraud graph
generator: `:Account` nodes joined by `:TRANSFER` edges, with cyclic laundering rings
injected as the `is_fraud` edges. Its own benchmark (benchmark/benchmark.py in that
repo) writes every chain as an explicit hop sequence; this one asks the same questions
with quantifiers and adds the shapes no fixed-hop query can express - an unbounded
fraud closure, and a ring detected by returning to the account it started from.

Queries run through both engines in the turingdb shell - v2 directly, v3 behind the
`#v3` prefix - and the median of the timed runs is reported, the first discarded as a
warmup.

Prerequisites:
  - turingdb built (build/tools/turingdb/turingdb)
  - the graph loaded in the turing dir passed to -turing-dir:

      pip install -e .                    # in a gen-fraud-graph checkout
      gen-fraud-graph --scale 0.1 --format parquet --output ./fraud_1m_parquet
      mkdir -p ~/.turing-fraud/data/fraud_1m
      cp ./fraud_1m_parquet/{nodes,edges}.parquet ~/.turing-fraud/data/fraud_1m/
      echo "LOAD PARQUET 'fraud_1m' AS fraud_1m" | turingdb -turing-dir ~/.turing-fraud

Examples:
  ./bench_fraud_paths.py
  ./bench_fraud_paths.py -reps 9 -groups C
  ./bench_fraud_paths.py -verify
"""

import argparse
import os
import re
import statistics
import subprocess
import sys

RING_SEED = 796580
RING_DEPTH = 7
HIGH_RISK = 0.99
HIGH_AMOUNT = 9000

MARKER = "MARKERBEGIN"

GROUPS = {
    "A": "Seeded fan-out - the published benchmark's shape, hops against a quantifier",
    "B": "Whole-graph fraud chains - the published benchmark's queries, and past them",
    "C": "Laundering rings - the typology the generator injects",
    "D": "Untyped variable-length - the only path shapes v2 will run",
    "E": "Search against walk, by hop bound - count(DISTINCT) searches, count() walks",
}


def transferChain(depth):
    hops = "".join(f"-[:TRANSFER]->(x{level})" for level in range(1, depth))
    return f"{hops}-[:TRANSFER]->(b)"


# The fraud chains of the generator's own benchmark: one named edge per hop, every one
# of them constrained to a ring edge in a single WHERE
def fraudChain(depth):
    hops = "".join(f"-[t{level}:TRANSFER]->(x{level})" for level in range(1, depth))
    predicates = " AND ".join(f"t{level}.is_fraud = true" for level in range(1, depth + 1))
    return (f"MATCH (a:Account){hops}-[t{depth}:TRANSFER]->(b) "
            f"WHERE {predicates} RETURN count(b)")


def quantifier(bound):
    return "+" if bound is None else f"{{1,{bound}}}"


# The same pattern counted as trails (the walk) and as distinct ends (the search), one
# query per bound; the walk's bounds stop where its trails stop being enumerable in seconds
def sweep(prefix, question, pattern, walkBounds, searchBounds):
    queries = []
    for mode, bounds in (("walk", walkBounds), ("search", searchBounds)):
        for bound in bounds:
            aggregate = "count(DISTINCT b)" if mode == "search" else "count(b)"
            label = "unbounded" if bound is None else f"{{1,{bound}}}"
            queries.append(("E", f"{prefix}_{mode}_{bound or 'inf'}",
                            f"{question}, {label}, {mode}",
                            None,
                            f"MATCH {pattern.format(hops=quantifier(bound))} RETURN {aggregate}"))
    return queries


# group, id, question, v2 query (None to ask v2 the v3 query and record how it
# refuses it), v3 query
QUERIES = [
    ("A", "seed",
     "Find the seed account by id",
     f"MATCH (a:Account {{account_id: {RING_SEED}}}) RETURN a.risk_score",
     f"MATCH (a:Account {{account_id: {RING_SEED}}}) RETURN a.risk_score"),
]

for depth in (1, 2, 3, 4):
    QUERIES += [
        ("A", f"fanout_hops_{depth}",
         f"Accounts exactly {depth} transfers out, as {depth} written hops",
         f"MATCH (a:Account {{account_id: {RING_SEED}}}){transferChain(depth)} RETURN count(b)",
         f"MATCH (a:Account {{account_id: {RING_SEED}}}){transferChain(depth)} RETURN count(b)"),

        ("A", f"fanout_exact_{depth}",
         f"Accounts exactly {depth} transfers out, as a quantifier",
         None,
         f"MATCH (a:Account {{account_id: {RING_SEED}}})-[:TRANSFER]->{{{depth},{depth}}}(b) RETURN count(b)"),

        ("A", f"fanout_upto_{depth}",
         f"Accounts up to {depth} transfers out",
         f"MATCH (a:Account {{account_id: {RING_SEED}}})-[]->{{1,{depth}}}(b) RETURN count(b)",
         f"MATCH (a:Account {{account_id: {RING_SEED}}})-[:TRANSFER]->{{1,{depth}}}(b) RETURN count(b)"),
    ]

for depth in (2, 3, 4):
    QUERIES += [
        ("B", f"fraud_hops_{depth}",
         f"{depth}-hop fraud chain, as {depth} written hops",
         fraudChain(depth),
         fraudChain(depth)),

        ("B", f"fraud_exact_{depth}",
         f"{depth}-hop fraud chain, as a quantifier",
         None,
         f"MATCH (a:Account)-[t:TRANSFER WHERE t.is_fraud = true]->{{{depth},{depth}}}(b) RETURN count(b)"),
    ]

QUERIES += [
    ("B", "fraud_upto_4",
     "Fraud chains of up to 4 transfers",
     None,
     "MATCH (a:Account)-[t:TRANSFER WHERE t.is_fraud = true]->{1,4}(b) RETURN count(b)"),

    ("B", "fraud_closure",
     "Every fraud chain there is, any length",
     None,
     "MATCH (a:Account)-[t:TRANSFER WHERE t.is_fraud = true]->+(b) RETURN count(b)"),

    ("B", "fraud_closure_ends",
     "Every account a fraud chain reaches",
     None,
     "MATCH (a:Account)-[t:TRANSFER WHERE t.is_fraud = true]->+(b) RETURN count(DISTINCT b)"),

    ("B", "high_amount_2",
     "2 hops of high-value transfers into a high-risk account",
     f"MATCH (a:Account)-[t1:TRANSFER]->(x:Account)-[t2:TRANSFER]->(b:Account) WHERE t1.amount > {HIGH_AMOUNT} AND t2.amount > {HIGH_AMOUNT} AND b.risk_score > 0.9 RETURN count(b)",
     f"MATCH (a:Account)((x)-[t:TRANSFER]->(y) WHERE t.amount > {HIGH_AMOUNT}){{2,2}}(b:Account) WHERE b.risk_score > 0.9 RETURN count(b)"),

    ("C", "ring_seed",
     f"Does account {RING_SEED} sit on a laundering ring",
     None,
     f"MATCH (a:Account {{account_id: {RING_SEED}}})-[t:TRANSFER WHERE t.is_fraud = true]->{{1,{RING_DEPTH}}}(a) RETURN count(a)"),

    ("C", "ring_all",
     "Every account on a laundering ring, whole graph",
     None,
     f"MATCH (a:Account)-[t:TRANSFER WHERE t.is_fraud = true]->{{1,{RING_DEPTH}}}(a) RETURN count(a)"),

    ("C", "ring_all_unbounded",
     "Every account on a laundering ring, no depth bound",
     None,
     "MATCH (a:Account)-[t:TRANSFER WHERE t.is_fraud = true]->+(a) RETURN count(a)"),

    ("C", "ring_untyped",
     "Every account on a 3-transfer ring, fraudulent or not",
     "MATCH (a:Account)-[]->{1,3}(a) RETURN count(a)",
     "MATCH (a:Account)-[:TRANSFER]->{1,3}(a) RETURN count(a)"),

    ("C", "ring_seed_depth",
     f"How many accounts {RING_SEED} launders through",
     None,
     f"MATCH (a:Account {{account_id: {RING_SEED}}})-[t:TRANSFER WHERE t.is_fraud = true]->{{1,{RING_DEPTH}}}(a) RETURN size(t)"),

    ("C", "ring_depths",
     "Every ring's length, whole graph",
     None,
     f"MATCH (a:Account)-[t:TRANSFER WHERE t.is_fraud = true]->{{1,{RING_DEPTH}}}(a) RETURN size(t), count(a)"),

    ("D", "ball_2",
     "Within 2 transfers of every account",
     "MATCH (a:Account)-[]->{1,2}(b) RETURN count(b)",
     "MATCH (a:Account)-[:TRANSFER]->{1,2}(b) RETURN count(b)"),

    ("D", "ball_3",
     "Within 3 transfers of the 550 ring accounts",
     f"MATCH (a:Account)-[t:TRANSFER]->(s) WHERE t.is_fraud = true MATCH (s)-[]->{{1,3}}(b) RETURN count(b)",
     f"MATCH (a:Account)-[t:TRANSFER]->(s) WHERE t.is_fraud = true MATCH (s)-[:TRANSFER]->{{1,3}}(b) RETURN count(b)"),

    ("D", "ball_seed_5",
     "Within 5 transfers of the seed account",
     f"MATCH (a:Account {{account_id: {RING_SEED}}})-[]->{{1,5}}(b) RETURN count(b)",
     f"MATCH (a:Account {{account_id: {RING_SEED}}})-[:TRANSFER]->{{1,5}}(b) RETURN count(b)"),

    ("D", "ball_seed_6_ends",
     "Accounts within 6 transfers of the seed account",
     f"MATCH (a:Account {{account_id: {RING_SEED}}})-[]->{{1,6}}(b) RETURN count(DISTINCT b)",
     f"MATCH (a:Account {{account_id: {RING_SEED}}})-[:TRANSFER]->{{1,6}}(b) RETURN count(DISTINCT b)"),
]

QUERIES += sweep("seed", "Downstream of one account",
                 f"(a:Account {{{{account_id: {RING_SEED}}}}})-[:TRANSFER]->{{hops}}(b)",
                 [2, 4, 6, 7, 8],
                 [2, 4, 6, 7, 8, 10, 14, None])

QUERIES += sweep("ring", "Downstream of the 550 ring accounts",
                 "(a:Account)-[t0:TRANSFER WHERE t0.is_fraud = true]->(s)-[:TRANSFER]->{hops}(b)",
                 [2, 3, 4, 5],
                 [2, 3, 4, 5, 6])


def selectQueries(groups, only):
    selected = []
    for group, queryID, question, v2, v3 in QUERIES:
        if group not in groups:
            continue
        if only and queryID not in only:
            continue
        selected.append((group, queryID, question, v2, v3))
    return selected


def buildScript(engine, queries, graph, reps, quiet):
    lines = [f"load graph {graph}", f"cd {graph}"]
    if quiet:
        lines.append("quiet")

    for _, queryID, _, v2, v3 in queries:
        query = v3 if engine == "v3" or v2 is None else v2

        for rep in range(reps):
            lines.append(f"sh echo {MARKER} {queryID} {rep}")
            lines.append(query if engine == "v2" else "#v3 " + query)

    lines.append("exit")

    return "\n".join(lines) + "\n"


def runShell(binary, turingDir, port, script, timeout):
    command = ["stdbuf", "-oL", binary, "-turing-dir", turingDir, "-p", str(port)]
    process = subprocess.run(command, input=script, capture_output=True, text=True, timeout=timeout)

    return process.stdout + "\n" + process.stderr


def parseShellOutput(output):
    results = {}
    current = None

    for line in output.splitlines():
        marker = re.match(rf"{MARKER} (\S+) (\d+)", line)
        if marker:
            current = marker.group(1)
            results.setdefault(current, {"times": [], "rows": None, "value": None, "error": None})
            continue

        if current is None:
            continue

        result = results[current]

        rows = re.match(r"Query returned (\d+) rows\.", line)
        if rows:
            result["rows"] = int(rows.group(1))
            continue

        time = re.match(r"Query executed in ([0-9.]+) ms\.", line)
        if time:
            result["times"].append(float(time.group(1)))
            continue

        value = re.match(r"^\|\s*(-?\d+)\s*\|$", line.strip())
        if value and result["value"] is None:
            result["value"] = value.group(1)
            continue

        if "[error]" in line and result["error"] is None:
            result["error"] = line.split("[error]", 1)[1].strip()[:120]

    return results


def median(results, queryID):
    result = results.get(queryID)
    if not result or len(result["times"]) < 2:
        return None

    return statistics.median(result["times"][1:])


# An aggregate's value when -verify captured it, else the rows the query returned
def countOrRows(result):
    if not result:
        return None

    return result["value"] if result["value"] is not None else result["rows"]


def report(queries, v2Results, v3Results):
    for group, title in GROUPS.items():
        inGroup = [q for q in queries if q[0] == group]
        if not inGroup:
            continue

        print(f"\n{group}. {title}")
        print("-" * 112)
        print(f"  {'question':58} {'v2':>14} {'v3 ms':>9} {'speedup':>8} {'rows':>11}")

        for _, queryID, question, _, _ in inGroup:
            v2Time = median(v2Results, queryID)
            v3Time = median(v3Results, queryID)
            rows = countOrRows(v3Results.get(queryID))

            if v3Time is None:
                print(f"  {question:58} {'':>14} {'FAILED':>9}")
                continue

            if v2Time is None:
                error = (v2Results.get(queryID) or {}).get("error") or "no result"
                print(f"  {question:58} {error.split(':')[0]:>14} {v3Time:9.2f} {'--':>8} {rows!s:>11}")
            else:
                print(f"  {question:58} {v2Time:14.2f} {v3Time:9.2f} {v2Time / v3Time:7.1f}x {rows!s:>11}")

    print()
    reportDisagreements(queries, v2Results, v3Results)


def reportDisagreements(queries, v2Results, v3Results):
    disagreements = []

    for _, queryID, question, _, _ in queries:
        v2Result = v2Results.get(queryID)
        v3Result = v3Results.get(queryID)
        if not v2Result or not v3Result:
            continue

        for field in ("rows", "value"):
            if v2Result[field] is not None and v3Result[field] is not None and v2Result[field] != v3Result[field]:
                disagreements.append((question, field, v2Result[field], v3Result[field]))

    if not disagreements:
        print("Both engines agree on every result they both produced.")
        return

    print("DISAGREEMENTS")
    for question, field, v2Value, v3Value in disagreements:
        print(f"  {question}: {field} v2={v2Value} v3={v3Value}")


def main():
    repoRoot = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("-binary", default=os.path.join(repoRoot, "build/tools/turingdb/turingdb"))
    parser.add_argument("-turing-dir", dest="turingDir", default=os.path.expanduser("~/.turing-fraud"))
    parser.add_argument("-graph", default="fraud_1m")
    parser.add_argument("-port", type=int, default=6821)
    parser.add_argument("-reps", type=int, default=5, help="runs per query; the first is a warmup")
    parser.add_argument("-timeout", type=float, default=5400, help="seconds per engine")
    parser.add_argument("-groups", default="ABCDE", help="query groups to run")
    parser.add_argument("-only", default="", help="comma-separated query ids")
    parser.add_argument("-verify", action="store_true", help="re-run the counting queries unquiet and compare their values")
    args = parser.parse_args()

    queries = selectQueries(set(args.groups), set(filter(None, args.only.split(","))))
    if not queries:
        print("No query selected", file=sys.stderr)
        return 1

    aggregates = [q for q in queries if "count(" in q[4]]

    results = {}
    for index, engine in enumerate(("v2", "v3")):
        script = buildScript(engine, queries, args.graph, args.reps, True)
        output = runShell(args.binary, args.turingDir, args.port + index, script, args.timeout)
        results[engine] = parseShellOutput(output)

        if not args.verify or not aggregates:
            continue

        valueScript = buildScript(engine, aggregates, args.graph, 1, False)
        valueOutput = runShell(args.binary, args.turingDir, args.port + 2 + index, valueScript, args.timeout)

        for queryID, result in parseShellOutput(valueOutput).items():
            if queryID in results[engine]:
                results[engine][queryID]["value"] = result["value"]

    report(queries, results["v2"], results["v3"])

    return 0


if __name__ == "__main__":
    sys.exit(main())
