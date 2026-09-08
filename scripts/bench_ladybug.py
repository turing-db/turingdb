#!/usr/bin/env python3
"""
Time the docs/path_bench.md variable-length path queries on ladybug.

Mirrors bench_paths.py: the same 71 queries in five groups, five runs each with the
first discarded, and reports the median of the rest against the v2 and v3 times
docs/path_bench.md records. Load the graph first with ladybug_load_reactome.py.

Reactome nodes carry six labels and ladybug is one label per node table, so `(a:Event)`
is written `(a:Node) ... a.isEvent` against the loader's boolean columns. Quantifiers
become recursive rels under TRAIL, whose semantics match v3's; ladybug's default is WALK,
which reproduces the repeated-edge overcount v2 has.

Seeded queries run twice: as written, where the stId predicate scans all 2.98M nodes for
want of a secondary index, and re-seeded on the primary key, which isolates the walk.

  ./bench_ladybug.py
  ./bench_ladybug.py -groups E -reps 9
  ./bench_ladybug.py -report
"""

import argparse
import json
import os
import statistics
import sys
import time

import ladybug

SIGNAL_TRANSDUCTION = "R-HSA-162582"
HUB_REACTION = "R-HSA-2993780"
DEEPEST_HUMAN_COMPLEX = "R-HSA-6814275"

# turingdb's `+` is unbounded; ladybug's recursion needs a literal ceiling
UNBOUNDED = 200

GROUPS = {
    "A": "Property-seeded typed queries - what a curator actually writes",
    "B": "Label-seeded typed traversal - the engine, with no scan in the way",
    "C": "Untyped variable-length - the only path shapes v2 will run",
    "D": "Queries v2 cannot express at all",
    "E": "Search against walk, by hop bound - count(DISTINCT) searches, count() walks",
}


def seed(variable, label, accession):
    return f"{variable}.is{label} AND {variable}.stId = '{accession}'"


def hasEventChain(depth):
    hops = "".join(f"-[:hasEvent]->(x{level}:Node)" for level in range(1, depth))
    return (f"MATCH (p:Node){hops}-[:hasEvent]->(z:Node) "
            f"WHERE p.isTopLevelPathway AND z.isEvent RETURN count(z)")


def bound(hops):
    return f"1..{UNBOUNDED}" if hops is None else f"1..{hops}"


def sweep(prefix, question, pattern, filters, walkBounds, searchBounds):
    queries = []
    for mode, bounds in (("walk", walkBounds), ("search", searchBounds)):
        for hops in bounds:
            aggregate = "count(DISTINCT z)" if mode == "search" else "count(z)"
            label = "unbounded" if hops is None else f"{{1,{hops}}}"
            queries.append(("E", f"{prefix}_{mode}_{hops or 'inf'}",
                            f"{question}, {label}, {mode}",
                            f"MATCH {pattern.format(hops=bound(hops))} "
                            f"WHERE {filters} RETURN {aggregate}"))
    return queries


QUERIES = [
    ("A", "seed", "Find Signal Transduction by accession",
     f"MATCH (p:Node) WHERE {seed('p', 'TopLevelPathway', SIGNAL_TRANSDUCTION)} RETURN p.displayName"),

    ("A", "st_d1", "Its direct sub-events (depth 1)",
     f"MATCH (p:Node)-[:hasEvent* TRAIL 1..1]->(a:Node) "
     f"WHERE {seed('p', 'TopLevelPathway', SIGNAL_TRANSDUCTION)} AND a.isEvent RETURN a.stId"),

    ("A", "st_d3", "Its sub-events exactly 3 levels down",
     f"MATCH (p:Node)-[:hasEvent* TRAIL 3..3]->(c:Node) "
     f"WHERE {seed('p', 'TopLevelPathway', SIGNAL_TRANSDUCTION)} AND c.isEvent RETURN c.stId"),

    ("A", "st_d4", "Its sub-events exactly 4 levels down",
     f"MATCH (p:Node)-[:hasEvent* TRAIL 4..4]->(d:Node) "
     f"WHERE {seed('p', 'TopLevelPathway', SIGNAL_TRANSDUCTION)} AND d.isEvent RETURN d.stId"),

    ("A", "cascade_d4", f"Reactions exactly 4 steps downstream of hub {HUB_REACTION}",
     f"MATCH (r:Node)<-[:precedingEvent* TRAIL 4..4]-(d:Node) "
     f"WHERE {seed('r', 'Reaction', HUB_REACTION)} AND d.isReaction RETURN d.stId"),

    ("A", "complex_d2", f"Sub-units of complex {DEEPEST_HUMAN_COMPLEX}, exactly 2 levels in",
     f"MATCH (c:Node)-[:hasComponent* TRAIL 2..2]->(b:Node) "
     f"WHERE {seed('c', 'Complex', DEEPEST_HUMAN_COMPLEX)} RETURN b.stId"),

    ("B", "tlp_typed_d2_all", "Events two hasEvent levels under all 415 top-level pathways",
     "MATCH (p:Node)-[:hasEvent* TRAIL 2..2]->(z:Node) WHERE p.isTopLevelPathway AND z.isEvent RETURN count(z)"),

    ("B", "tlp_typed_d3_all", "Events three hasEvent levels under all of them",
     "MATCH (p:Node)-[:hasEvent* TRAIL 3..3]->(z:Node) WHERE p.isTopLevelPathway AND z.isEvent RETURN count(z)"),

    ("B", "tlp_typed_d4_all", "Events four hasEvent levels under all of them",
     "MATCH (p:Node)-[:hasEvent* TRAIL 4..4]->(z:Node) WHERE p.isTopLevelPathway AND z.isEvent RETURN count(z)"),

    ("B", "tlp_typed_d5_all", "Events five hasEvent levels under all of them",
     "MATCH (p:Node)-[:hasEvent* TRAIL 5..5]->(z:Node) WHERE p.isTopLevelPathway AND z.isEvent RETURN count(z)"),

    ("B", "tlp_typed_d6_all", "Events six hasEvent levels under all of them",
     "MATCH (p:Node)-[:hasEvent* TRAIL 6..6]->(z:Node) WHERE p.isTopLevelPathway AND z.isEvent RETURN count(z)"),

    ("B", "complex_tree_all", "Every sub-unit two levels inside every complex",
     "MATCH (c:Node)-[:hasComponent* TRAIL 2..2]->(b:Node) WHERE c.isComplex RETURN count(b)"),

    ("B", "preceding_d3", "Reaction triples three precedingEvent steps apart",
     "MATCH (r:Node)<-[:precedingEvent* TRAIL 3..3]-(c:Node) WHERE r.isReaction AND c.isReaction RETURN count(c)"),

    ("B", "tlp_two_hop", "Reactions two hasEvent levels under every top-level pathway",
     "MATCH (tlp:Node)-[:hasEvent]->(p:Node)-[:hasEvent]->(r:Node) "
     "WHERE tlp.isTopLevelPathway AND p.isPathway AND r.isReactionLikeEvent RETURN count(r)"),

    ("B", "inputs", "Every reaction's input entities",
     "MATCH (r:Node)-[:input]->(p:Node) WHERE r.isReactionLikeEvent AND p.isPhysicalEntity RETURN count(p)"),

    ("C", "untyped_1_2", "Everything within 2 hops of the 415 top-level pathways",
     "MATCH (p:Node)-[e* TRAIL 1..2]->(m:Node) WHERE p.isTopLevelPathway RETURN count(m)"),

    ("C", "untyped_1_3", "Everything within 3 hops of them",
     "MATCH (p:Node)-[e* TRAIL 1..3]->(m:Node) WHERE p.isTopLevelPathway RETURN count(m)"),

    ("C", "untyped_1_3_event", "Events within 3 hops of them",
     "MATCH (p:Node)-[e* TRAIL 1..3]->(m:Node) WHERE p.isTopLevelPathway AND m.isEvent RETURN count(m)"),

    ("C", "reaction_2hop", "Everything within 2 hops of all 83,459 reactions",
     "MATCH (r:Node)-[e* TRAIL 1..2]->(m:Node) WHERE r.isReaction RETURN count(m)"),

    ("C", "untyped_undirected", "Everything within 2 hops in either direction",
     "MATCH (p:Node)-[e* TRAIL 1..2]-(m:Node) WHERE p.isTopLevelPathway RETURN count(m)"),

    ("D", "st_all", "Signal Transduction's entire sub-event tree, any depth",
     f"MATCH (p:Node)-[:hasEvent* TRAIL 1..{UNBOUNDED}]->(a:Node) "
     f"WHERE {seed('p', 'TopLevelPathway', SIGNAL_TRANSDUCTION)} AND a.isEvent RETURN a.stId"),

    ("D", "st_all_reactions", "Every reaction it eventually decomposes into",
     f"MATCH (p:Node)-[:hasEvent* TRAIL 1..{UNBOUNDED}]->(r:Node) "
     f"WHERE {seed('p', 'TopLevelPathway', SIGNAL_TRANSDUCTION)} AND r.isReaction RETURN r.stId"),

    ("D", "all_tlp_reactions", "Every reaction under every top-level pathway",
     f"MATCH (p:Node)-[:hasEvent* TRAIL 1..{UNBOUNDED}]->(r:Node) "
     f"WHERE p.isTopLevelPathway AND r.isReaction RETURN p.stId, r.stId"),

    ("D", "ancestor", f"Which top-level pathway contains reaction {HUB_REACTION}",
     f"MATCH (r:Node)<-[:hasEvent* TRAIL 1..{UNBOUNDED}]-(p:Node) "
     f"WHERE {seed('r', 'Reaction', HUB_REACTION)} AND p.isTopLevelPathway RETURN p.displayName"),

    ("D", "cascade_1_4", "Reactions up to 4 steps downstream of that hub",
     f"MATCH (r:Node)<-[:precedingEvent* TRAIL 1..4]-(d:Node) "
     f"WHERE {seed('r', 'Reaction', HUB_REACTION)} AND d.isReaction RETURN d.stId"),

    ("D", "cascade_distinct", "All reactions downstream of that hub, any distance",
     f"MATCH (r:Node)<-[:precedingEvent* TRAIL 1..{UNBOUNDED}]-(d:Node) "
     f"WHERE {seed('r', 'Reaction', HUB_REACTION)} AND d.isReaction RETURN DISTINCT d.stId"),

    ("D", "complex_all", "That complex's whole sub-unit tree",
     f"MATCH (c:Node)-[:hasComponent* TRAIL 1..{UNBOUNDED}]->(b:Node) "
     f"WHERE {seed('c', 'Complex', DEEPEST_HUMAN_COMPLEX)} RETURN b.stId"),

    ("D", "shared_input", "Reaction pairs of one pathway sharing an input entity",
     "MATCH (a:Node)-[:hasEvent]->(r1:Node), (a)-[:hasEvent]->(r2:Node), "
     "(r1)-[:input]->(e:Node), (r2)-[:input]->(e) "
     "WHERE a.isPathway AND r1.isReactionLikeEvent AND r2.isReactionLikeEvent AND e.isPhysicalEntity "
     "RETURN count(e)"),
]

QUERIES += sweep("cascade", f"Reactions downstream of hub {HUB_REACTION}",
                 "(r:Node)<-[:precedingEvent* TRAIL {hops}]-(z:Node)",
                 f"{seed('r', 'Reaction', HUB_REACTION)} AND z.isReaction",
                 [4, 8, 16, 24, 32, 40, 44],
                 [4, 8, 16, 24, 32, 40, 44, 100, None])

QUERIES += sweep("st", "Signal Transduction's sub-events",
                 "(p:Node)-[:hasEvent* TRAIL {hops}]->(z:Node)",
                 f"{seed('p', 'TopLevelPathway', SIGNAL_TRANSDUCTION)} AND z.isEvent",
                 [2, 4, 8, 13, 100, None],
                 [2, 4, 8, 13, 100, None])

QUERIES += sweep("tlp", "Reactions under the 415 top-level pathways",
                 "(p:Node)-[:hasEvent* TRAIL {hops}]->(z:Node)",
                 "p.isTopLevelPathway AND z.isReaction",
                 [3, 6, 100, None],
                 [3, 6, 100, None])

QUERIES += sweep("complex", "Sub-units of every complex",
                 "(c:Node)-[:hasComponent* TRAIL {hops}]->(z:Node)",
                 "c.isComplex",
                 [2, 100],
                 [2, 100])

QUERIES += sweep("reactions", "Reactions downstream of every reaction",
                 "(r:Node)<-[:precedingEvent* TRAIL {hops}]-(z:Node)",
                 "r.isReaction AND z.isReaction",
                 [3],
                 [3, None])


# id -> (v2 ms, v3 ms, expected count) from docs/path_bench.md; None where the engine refused
REFERENCE = {
    "seed": (45.81, 0.55, 1),
    "st_d1": (45.93, 0.85, 17),
    "st_d3": (46.34, 1.02, 480),
    "st_d4": (47.36, 1.29, 1169),
    "cascade_d4": (46.22, 1.20, None),
    "complex_d2": (46.24, 1.22, None),

    "tlp_typed_d2_all": (2.30, 1.46, 11630),
    "tlp_typed_d3_all": (9.86, 3.37, 28799),
    "tlp_typed_d4_all": (24.10, 7.06, 40012),
    "tlp_typed_d5_all": (51.06, 11.40, 28679),
    "tlp_typed_d6_all": (60.62, 13.98, 10260),
    "complex_tree_all": (58.25, 30.12, 213396),
    "preceding_d3": (46.54, 17.53, 86520),
    "tlp_two_hop": (2.45, 1.17, 6371),
    "inputs": (21.25, 7.75, 186017),

    "untyped_1_2": (2.89, 1.42, 34669),
    "untyped_1_3": (16.29, 4.90, 228878),
    "untyped_1_3_event": (25.19, 7.86, 90086),
    "reaction_2hop": (253.96, 85.88, 4788031),
    "untyped_undirected": (7118.42, 836.27, 125690888),

    "st_all": (None, 2.23, 3154),
    "st_all_reactions": (None, 2.15, 2344),
    "all_tlp_reactions": (None, 26.04, 92952),
    "ancestor": (None, 1.20, 1),
    "cascade_1_4": (None, 1.22, 96),
    "cascade_distinct": (None, 2.01, 1520),
    "complex_all": (None, 1.22, 23),
    "shared_input": (None, 60.62, 390348),

    "cascade_walk_4": (None, 1.23, 96),
    "cascade_walk_8": (None, 1.23, 101),
    "cascade_walk_16": (None, 1.25, 179),
    "cascade_walk_24": (None, 1.53, 1412),
    "cascade_walk_32": (None, 5.15, 43489),
    "cascade_walk_40": (None, 1329.0, 16956465),
    "cascade_walk_44": (None, 24275.0, 302221591),
    "cascade_search_4": (None, 1.23, 96),
    "cascade_search_8": (None, 1.24, 101),
    "cascade_search_16": (None, 1.27, 177),
    "cascade_search_24": (None, 1.40, 511),
    "cascade_search_32": (None, 1.53, 902),
    "cascade_search_40": (None, 1.62, 1175),
    "cascade_search_44": (None, 1.68, 1308),
    "cascade_search_100": (None, 1.77, 1520),
    "cascade_search_inf": (None, 1.77, 1520),

    "st_walk_2": (None, 0.92, 157),
    "st_walk_4": (None, 1.25, 1806),
    "st_walk_8": (None, 1.92, 3119),
    "st_walk_13": (None, 1.94, 3154),
    "st_walk_100": (None, 1.92, 3154),
    "st_walk_inf": (None, 1.91, 3154),
    "st_search_2": (None, 0.93, 157),
    "st_search_4": (None, 1.30, 1784),
    "st_search_8": (None, 1.85, 2994),
    "st_search_13": (None, 1.85, 2997),
    "st_search_100": (None, 1.85, 2997),
    "st_search_inf": (None, 1.85, 2997),

    "tlp_walk_3": (None, 3.79, 24834),
    "tlp_walk_6": (None, 16.33, 86616),
    "tlp_walk_100": (None, 18.48, 92952),
    "tlp_walk_inf": (None, 21.33, 92952),
    "tlp_search_3": (None, 5.64, 24127),
    "tlp_search_6": (None, 20.91, 79761),
    "tlp_search_100": (None, 22.39, 81798),
    "tlp_search_inf": (None, 22.42, 81798),

    "complex_walk_2": (None, 30.26, 487883),
    "complex_walk_100": (None, 52.62, 740283),
    "complex_search_2": (None, 44.19, 143187),
    "complex_search_100": (None, 59.07, 143187),

    "reactions_walk_3": (None, 17.94, 208580),
    "reactions_search_3": (None, 16.92, 45252),
    "reactions_search_inf": (None, 138.34, 45331),
}



def seedIDs(connection):
    listed = ", ".join(f"'{accession}'" for accession in
                       (SIGNAL_TRANSDUCTION, HUB_REACTION, DEEPEST_HUMAN_COMPLEX))
    rows = connection.execute(f"MATCH (p:Node) WHERE p.stId IN [{listed}] RETURN p.stId, p.id").get_all()

    return {accession: nodeID for accession, nodeID in rows}


# The stId predicate ladybug can only answer by scanning, rewritten onto the primary key
def onPrimaryKey(query, ids):
    for accession, nodeID in ids.items():
        for variable in ("p", "r", "c"):
            query = query.replace(f"{variable}.stId = '{accession}'", f"{variable}.id = {nodeID}")

    return query


def runOnce(connection, query):
    start = time.perf_counter()
    result = connection.execute(query)
    rows = result.get_num_tuples()
    value = None

    if rows == 1 and len(result.get_column_names()) == 1:
        first = result.get_next()
        value = first[0] if isinstance(first[0], int) else None
    else:
        while result.has_next():
            result.get_next()

    elapsed = (time.perf_counter() - start) * 1000
    result.close()

    return elapsed, rows, value


def timeQuery(connection, query, reps, budget):
    times = []
    rows = value = None

    for _ in range(reps):
        elapsed, rows, value = runOnce(connection, query)
        times.append(elapsed)

        if sum(times) > budget * 1000:
            break

    return {"median": statistics.median(times[1:]) if len(times) > 1 else times[0],
            "times": times, "rows": rows, "value": value}


def measure(args, selected, done):
    database = ladybug.Database(args.db, buffer_pool_size=16 * 1024**3,
                                max_num_threads=args.threads)
    connection = ladybug.Connection(database)
    connection.execute(f"CALL var_length_extend_max_depth={UNBOUNDED}")
    connection.set_query_timeout(int(args.timeout * 1000))

    ids = seedIDs(connection)

    with open(args.results, "a") as handle:
        for group, queryID, question, query in selected:
            if queryID in done:
                continue

            record = {"group": group, "id": queryID, "question": question, "query": query}
            for variant, text in (("written", query), ("pk", onPrimaryKey(query, ids))):
                if variant == "pk" and text == query:
                    continue

                try:
                    record[variant] = timeQuery(connection, text, args.reps, args.budget)
                except Exception as error:
                    record[variant] = {"error": str(error)[:200]}

            handle.write(json.dumps(record) + "\n")
            handle.flush()

            written = record["written"]
            shown = f"{written['median']:10.2f} ms" if "median" in written else "   TIMEOUT"
            print(f"{group} {queryID:24s} {shown}  {written.get('rows', '')}", flush=True)


def median(record, variant):
    result = record.get(variant)

    return result["median"] if result and "median" in result else None


def resultCount(record):
    written = record.get("written") or {}

    return written["value"] if written.get("value") is not None else written.get("rows")


def formatTime(value, width=10, digits=2):
    if value is None:
        return " " * (width - 1) + "-"

    if value >= 100000:
        return f"{value:{width},.0f}"

    return f"{value:{width}.{digits}f}"


def report(results):
    records = [json.loads(line) for line in open(results) if line.strip()]

    for group, title in GROUPS.items():
        inGroup = [record for record in records if record["group"] == group]
        if not inGroup:
            continue

        print(f"\n{group}. {title}")
        print("-" * 126)
        print(f"  {'question':52} {'v2':>10} {'v3':>10} {'ladybug':>10} {'lb (pk)':>10} "
              f"{'lb/v3':>8} {'count':>13} {'match':>6}")

        for record in inGroup:
            v2Time, v3Time, expected = REFERENCE.get(record["id"], (None, None, None))
            asWritten, onKey = median(record, "written"), median(record, "pk")
            observed = resultCount(record)

            best = onKey if onKey is not None else asWritten
            ratio = best / v3Time if best is not None and v3Time else None
            agrees = "" if expected is None or observed is None else ("yes" if expected == observed else "NO")
            error = (record.get("written") or {}).get("error")

            print(f"  {record['question'][:52]:52} {formatTime(v2Time)} {formatTime(v3Time)} "
                  f"{formatTime(asWritten)} {formatTime(onKey)} {formatTime(ratio, 8, 1)} "
                  f"{observed if observed is not None else '-':>13} {agrees:>6}"
                  f"{'' if not error else '  [' + error.split(':')[0][:44] + ']'}")

    disagreements = [(record["id"], REFERENCE[record["id"]][2], resultCount(record))
                     for record in records
                     if record["id"] in REFERENCE and REFERENCE[record["id"]][2] is not None
                     and resultCount(record) is not None
                     and REFERENCE[record["id"]][2] != resultCount(record)]

    print()
    if not disagreements:
        print("Every count ladybug produced matches the value docs/path_bench.md records for v3.")
        return

    print("COUNT DISAGREEMENTS with docs/path_bench.md")
    for queryID, expected, observed in disagreements:
        print(f"  {queryID}: v3={expected} ladybug={observed}")


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("-db", default=os.path.expanduser("~/.ladybug-bench/reactome"))
    parser.add_argument("-results", default=os.path.expanduser("~/.ladybug-bench/results.jsonl"))
    parser.add_argument("-reps", type=int, default=5, help="runs per query; the first is a warmup")
    parser.add_argument("-timeout", type=float, default=180, help="seconds per query")
    parser.add_argument("-budget", type=float, default=400, help="seconds of repetitions per query")
    parser.add_argument("-groups", default="ABCDE")
    parser.add_argument("-only", default="", help="comma-separated query ids")
    parser.add_argument("-threads", type=int, default=0, help="0 leaves ladybug's default")
    parser.add_argument("-resume", action="store_true", help="skip query ids already measured")
    parser.add_argument("-report", action="store_true", help="re-render the results file, measuring nothing")
    args = parser.parse_args()

    if args.report:
        report(args.results)
        return 0

    only = set(filter(None, args.only.split(",")))
    selected = [q for q in QUERIES if q[0] in set(args.groups) and (not only or q[1] in only)]

    done = set()
    if args.resume and os.path.exists(args.results):
        with open(args.results) as handle:
            done = {json.loads(line)["id"] for line in handle if line.strip()}
    elif os.path.exists(args.results):
        os.remove(args.results)

    measure(args, selected, done)
    report(args.results)

    return 0


if __name__ == "__main__":
    sys.exit(main())
