#!/usr/bin/env python3
"""
Benchmark variable-length path and traversal queries on reactome, v2 against v3.

Runs a set of biologically meaningful Cypher queries through both engines in the
turingdb shell - v2 directly, v3 behind the `#v3` prefix - and reports the median
time of each, the queries only v3 can express, and any result the two engines
disagree on.

Prerequisites:
  - turingdb built (build/tools/turingdb/turingdb)
  - a reactome graph in the turing dir passed to -turing-dir. Copy it out of
    ~/.turing first: a single lock guards a turing dir and other sessions hold it.

      mkdir -p ~/.turing-bench/graphs
      cp -r ~/.turing/graphs/reactome ~/.turing-bench/graphs/

Examples:
  ./bench_paths.py
  ./bench_paths.py -reps 9 -groups C
  ./bench_paths.py -verify
"""

import argparse
import os
import re
import statistics
import subprocess
import sys

SIGNAL_TRANSDUCTION = "R-HSA-162582"
HUB_REACTION = "R-HSA-2993780"
DEEPEST_HUMAN_COMPLEX = "R-HSA-6814275"

MARKER = "MARKERBEGIN"

GROUPS = {
    "A": "Property-seeded typed queries - what a curator actually writes",
    "B": "Label-seeded typed traversal - the engine, with no scan in the way",
    "C": "Untyped variable-length - the only path shapes v2 will run",
    "D": "Queries v2 cannot express at all",
}


def hasEventChain(depth):
    hops = "".join(f"-[:hasEvent]->(x{level})" for level in range(1, depth))
    return f"MATCH (p:TopLevelPathway){hops}-[:hasEvent]->(z:Event) RETURN count(z)"


# group, id, question, v2 query (None to ask v2 the v3 query and record how it
# refuses it), v3 query
QUERIES = [
    ("A", "seed",
     "Find Signal Transduction by accession",
     f"MATCH (p:TopLevelPathway {{stId:'{SIGNAL_TRANSDUCTION}'}}) RETURN p.displayName",
     f"MATCH (p:TopLevelPathway {{stId:'{SIGNAL_TRANSDUCTION}'}}) RETURN p.displayName"),

    ("A", "st_d1",
     "Its direct sub-events (depth 1)",
     f"MATCH (p:TopLevelPathway {{stId:'{SIGNAL_TRANSDUCTION}'}})-[:hasEvent]->(a:Event) RETURN a.stId",
     f"MATCH (p:TopLevelPathway {{stId:'{SIGNAL_TRANSDUCTION}'}})-[:hasEvent]->{{1,1}}(a:Event) RETURN a.stId"),

    ("A", "st_d3",
     "Its sub-events exactly 3 levels down",
     f"MATCH (p:TopLevelPathway {{stId:'{SIGNAL_TRANSDUCTION}'}})-[:hasEvent]->(a)-[:hasEvent]->(b)-[:hasEvent]->(c:Event) RETURN c.stId",
     f"MATCH (p:TopLevelPathway {{stId:'{SIGNAL_TRANSDUCTION}'}})-[:hasEvent]->{{3,3}}(c:Event) RETURN c.stId"),

    ("A", "st_d4",
     "Its sub-events exactly 4 levels down",
     f"MATCH (p:TopLevelPathway {{stId:'{SIGNAL_TRANSDUCTION}'}})-[:hasEvent]->(a)-[:hasEvent]->(b)-[:hasEvent]->(c)-[:hasEvent]->(d:Event) RETURN d.stId",
     f"MATCH (p:TopLevelPathway {{stId:'{SIGNAL_TRANSDUCTION}'}})-[:hasEvent]->{{4,4}}(d:Event) RETURN d.stId"),

    ("A", "cascade_d4",
     f"Reactions exactly 4 steps downstream of hub {HUB_REACTION}",
     f"MATCH (r:Reaction {{stId:'{HUB_REACTION}'}})<-[:precedingEvent]-(a)<-[:precedingEvent]-(b)<-[:precedingEvent]-(c)<-[:precedingEvent]-(d:Reaction) RETURN d.stId",
     f"MATCH (r:Reaction {{stId:'{HUB_REACTION}'}})<-[:precedingEvent]-{{4,4}}(d:Reaction) RETURN d.stId"),

    ("A", "complex_d2",
     f"Sub-units of complex {DEEPEST_HUMAN_COMPLEX}, exactly 2 levels in",
     f"MATCH (c:Complex {{stId:'{DEEPEST_HUMAN_COMPLEX}'}})-[:hasComponent]->(a)-[:hasComponent]->(b) RETURN b.stId",
     f"MATCH (c:Complex {{stId:'{DEEPEST_HUMAN_COMPLEX}'}})-[:hasComponent]->{{2,2}}(b) RETURN b.stId"),

    ("B", "tlp_typed_d2_all",
     "Events two hasEvent levels under all 415 top-level pathways",
     hasEventChain(2),
     "MATCH (p:TopLevelPathway)-[:hasEvent]->{2,2}(z:Event) RETURN count(z)"),

    ("B", "tlp_typed_d3_all",
     "Events three hasEvent levels under all of them",
     hasEventChain(3),
     "MATCH (p:TopLevelPathway)-[:hasEvent]->{3,3}(z:Event) RETURN count(z)"),

    ("B", "tlp_typed_d4_all",
     "Events four hasEvent levels under all of them",
     hasEventChain(4),
     "MATCH (p:TopLevelPathway)-[:hasEvent]->{4,4}(z:Event) RETURN count(z)"),

    ("B", "tlp_typed_d5_all",
     "Events five hasEvent levels under all of them",
     hasEventChain(5),
     "MATCH (p:TopLevelPathway)-[:hasEvent]->{5,5}(z:Event) RETURN count(z)"),

    ("B", "tlp_typed_d6_all",
     "Events six hasEvent levels under all of them",
     hasEventChain(6),
     "MATCH (p:TopLevelPathway)-[:hasEvent]->{6,6}(z:Event) RETURN count(z)"),

    ("B", "complex_tree_all",
     "Every sub-unit two levels inside every complex",
     "MATCH (c:Complex)-[:hasComponent]->(a)-[:hasComponent]->(b) RETURN count(b)",
     "MATCH (c:Complex)-[:hasComponent]->{2,2}(b) RETURN count(b)"),

    ("B", "preceding_d3",
     "Reaction triples three precedingEvent steps apart",
     "MATCH (r:Reaction)<-[:precedingEvent]-(a)<-[:precedingEvent]-(b)<-[:precedingEvent]-(c:Reaction) RETURN count(c)",
     "MATCH (r:Reaction)<-[:precedingEvent]-{3,3}(c:Reaction) RETURN count(c)"),

    ("B", "tlp_two_hop",
     "Reactions two hasEvent levels under every top-level pathway",
     "MATCH (tlp:TopLevelPathway)-[:hasEvent]->(p:Pathway)-[:hasEvent]->(r:ReactionLikeEvent) RETURN count(r)",
     "MATCH (tlp:TopLevelPathway)-[:hasEvent]->(p:Pathway)-[:hasEvent]->(r:ReactionLikeEvent) RETURN count(r)"),

    ("B", "inputs",
     "Every reaction's input entities",
     "MATCH (r:ReactionLikeEvent)-[:input]->(p:PhysicalEntity) RETURN count(p)",
     "MATCH (r:ReactionLikeEvent)-[:input]->(p:PhysicalEntity) RETURN count(p)"),

    ("C", "untyped_1_2",
     "Everything within 2 hops of the 415 top-level pathways",
     "MATCH (p:TopLevelPathway)-[e]->{1,2}(m) RETURN count(m)",
     "MATCH (p:TopLevelPathway)-[e]->{1,2}(m) RETURN count(m)"),

    ("C", "untyped_1_3",
     "Everything within 3 hops of them",
     "MATCH (p:TopLevelPathway)-[e]->{1,3}(m) RETURN count(m)",
     "MATCH (p:TopLevelPathway)-[e]->{1,3}(m) RETURN count(m)"),

    ("C", "untyped_1_3_event",
     "Events within 3 hops of them",
     "MATCH (p:TopLevelPathway)-[e]->{1,3}(m:Event) RETURN count(m)",
     "MATCH (p:TopLevelPathway)-[e]->{1,3}(m:Event) RETURN count(m)"),

    ("C", "reaction_2hop",
     "Everything within 2 hops of all 83,459 reactions",
     "MATCH (r:Reaction)-[e]->{1,2}(m) RETURN count(m)",
     "MATCH (r:Reaction)-[e]->{1,2}(m) RETURN count(m)"),

    ("C", "untyped_undirected",
     "Everything within 2 hops in either direction",
     "MATCH (p:TopLevelPathway)-[e]-{1,2}(m) RETURN count(m)",
     "MATCH (p:TopLevelPathway)-[e]-{1,2}(m) RETURN count(m)"),

    ("D", "st_all",
     "Signal Transduction's entire sub-event tree, any depth",
     None,
     f"MATCH (p:TopLevelPathway {{stId:'{SIGNAL_TRANSDUCTION}'}})-[:hasEvent]->+(a:Event) RETURN a.stId"),

    ("D", "st_all_reactions",
     "Every reaction it eventually decomposes into",
     None,
     f"MATCH (p:TopLevelPathway {{stId:'{SIGNAL_TRANSDUCTION}'}})-[:hasEvent]->+(r:Reaction) RETURN r.stId"),

    ("D", "all_tlp_reactions",
     "Every reaction under every top-level pathway",
     None,
     "MATCH (p:TopLevelPathway)-[:hasEvent]->+(r:Reaction) RETURN p.stId, r.stId"),

    ("D", "ancestor",
     f"Which top-level pathway contains reaction {HUB_REACTION}",
     None,
     f"MATCH (r:Reaction {{stId:'{HUB_REACTION}'}})<-[:hasEvent]-+(p:TopLevelPathway) RETURN p.displayName"),

    ("D", "cascade_1_4",
     "Reactions up to 4 steps downstream of that hub",
     None,
     f"MATCH (r:Reaction {{stId:'{HUB_REACTION}'}})<-[:precedingEvent]-{{1,4}}(d:Reaction) RETURN d.stId"),

    ("D", "cascade_distinct",
     "All reactions downstream of that hub, any distance",
     None,
     f"MATCH (r:Reaction {{stId:'{HUB_REACTION}'}})<-[:precedingEvent]-+(d:Reaction) RETURN DISTINCT d.stId"),

    ("D", "complex_all",
     "That complex's whole sub-unit tree",
     None,
     f"MATCH (c:Complex {{stId:'{DEEPEST_HUMAN_COMPLEX}'}})-[:hasComponent]->+(b) RETURN b.stId"),

    ("D", "shared_input",
     "Reaction pairs of one pathway sharing an input entity",
     None,
     "MATCH (a:Pathway)-[:hasEvent]->(r1:ReactionLikeEvent), (a)-[:hasEvent]->(r2:ReactionLikeEvent), (r1)-[:input]->(e:PhysicalEntity), (r2)-[:input]->(e) RETURN count(e)"),
]


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


def report(queries, v2Results, v3Results):
    for group, title in GROUPS.items():
        inGroup = [q for q in queries if q[0] == group]
        if not inGroup:
            continue

        print(f"\n{group}. {title}")
        print("-" * 112)
        print(f"  {'question':58} {'v2':>14} {'v3 ms':>9} {'speedup':>8} {'rows':>9}")

        for _, queryID, question, _, _ in inGroup:
            v2Time = median(v2Results, queryID)
            v3Time = median(v3Results, queryID)
            rows = (v3Results.get(queryID) or {}).get("rows")

            if v3Time is None:
                print(f"  {question:58} {'':>14} {'FAILED':>9}")
                continue

            if v2Time is None:
                error = (v2Results.get(queryID) or {}).get("error") or "no result"
                print(f"  {question:58} {error.split(':')[0]:>14} {v3Time:9.2f} {'--':>8} {rows!s:>9}")
            else:
                print(f"  {question:58} {v2Time:14.2f} {v3Time:9.2f} {v2Time / v3Time:7.1f}x {rows!s:>9}")

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
    repoRoot = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("-binary", default=os.path.join(repoRoot, "build/tools/turingdb/turingdb"))
    parser.add_argument("-turing-dir", dest="turingDir", default=os.path.expanduser("~/.turing-bench"))
    parser.add_argument("-graph", default="reactome")
    parser.add_argument("-port", type=int, default=6811)
    parser.add_argument("-reps", type=int, default=5, help="runs per query; the first is a warmup")
    parser.add_argument("-timeout", type=float, default=1800, help="seconds per engine")
    parser.add_argument("-groups", default="ABCD", help="query groups to run")
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
