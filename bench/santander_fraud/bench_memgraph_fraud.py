#!/usr/bin/env python3
"""
Time this directory's variable-length path queries on memgraph.

Mirrors bench_fraud_paths.py: the same queries in five groups, five runs each with the
first discarded. It reports the median of the rest and checks each count against the one
the README records. Load the graph first with memgraph_load_fraud.py.

Every query below is the v3 query with the quantifier rewritten and nothing else:
`-[:TRANSFER]->{1,4}` becomes `-[:TRANSFER *1..4]->`, `+` becomes `*1..`, and an inline
hop predicate becomes memgraph's filter lambda, `-[t:TRANSFER *1..4 (e, n | e.is_fraud)]->`.
Memgraph's variable-length expansion enforces relationship uniqueness, which is v3's trail
semantics.

Memgraph also has a breadth-first expansion, `*BFS`, which yields every reachable node once
instead of every trail; that is the same question v3's distinct-end search answers, so the
distinct queries are timed a second time in that form. `*BFS` excludes a node from its own
ball, so the ring queries, which ask a walk to come back to where it started, have no BFS
form and are not given one.

  ./bench_memgraph_fraud.py
  ./bench_memgraph_fraud.py -groups E -reps 9
  ./bench_memgraph_fraud.py -report
"""

import argparse
import json
import os
import statistics
import sys
import time

import neo4j

RING_SEED = 796580
RING_DEPTH = 7
HIGH_AMOUNT = 9000

GROUPS = {
    "A": "Seeded fan-out - the published benchmark's shape, hops against a quantifier",
    "B": "Whole-graph fraud chains - the published benchmark's queries, and past them",
    "C": "Laundering rings - the typology the generator injects",
    "D": "Variable-length balls around the accounts",
    "E": "Search against walk, by hop bound - count(DISTINCT) searches, count() walks",
}

FRAUD_HOP = "(e, n | e.is_fraud = true)"


def transferChain(depth):
    hops = "".join(f"-[:TRANSFER]->(x{level})" for level in range(1, depth))
    return f"{hops}-[:TRANSFER]->(b)"


def fraudChain(depth):
    hops = "".join(f"-[t{level}:TRANSFER]->(x{level})" for level in range(1, depth))
    predicates = " AND ".join(f"t{level}.is_fraud = true" for level in range(1, depth + 1))
    return (f"MATCH (a:Account){hops}-[t{depth}:TRANSFER]->(b) "
            f"WHERE {predicates} RETURN count(b)")


# turingdb's `+` is unbounded, which memgraph writes as a variable-length with no ceiling
def quantifier(bound):
    return "*1.." if bound is None else f"*1..{bound}"


def sweep(prefix, question, pattern, walkBounds, searchBounds):
    queries = []
    for mode, bounds in (("walk", walkBounds), ("search", searchBounds)):
        for bound in bounds:
            aggregate = "count(DISTINCT b)" if mode == "search" else "count(b)"
            label = "unbounded" if bound is None else f"{{1,{bound}}}"
            queries.append(("E", f"{prefix}_{mode}_{bound or 'inf'}",
                            f"{question}, {label}, {mode}",
                            f"MATCH {pattern.format(hops=quantifier(bound))} RETURN {aggregate}"))
    return queries


QUERIES = [
    ("A", "seed", "Find the seed account by id",
     f"MATCH (a:Account {{account_id: {RING_SEED}}}) RETURN a.risk_score"),
]

for depth in (1, 2, 3, 4):
    QUERIES += [
        ("A", f"fanout_hops_{depth}", f"Accounts exactly {depth} transfers out, as {depth} written hops",
         f"MATCH (a:Account {{account_id: {RING_SEED}}}){transferChain(depth)} RETURN count(b)"),

        ("A", f"fanout_exact_{depth}", f"Accounts exactly {depth} transfers out, as a quantifier",
         f"MATCH (a:Account {{account_id: {RING_SEED}}})-[:TRANSFER*{depth}..{depth}]->(b) RETURN count(b)"),

        ("A", f"fanout_upto_{depth}", f"Accounts up to {depth} transfers out",
         f"MATCH (a:Account {{account_id: {RING_SEED}}})-[:TRANSFER*1..{depth}]->(b) RETURN count(b)"),
    ]

for depth in (2, 3, 4):
    QUERIES += [
        ("B", f"fraud_hops_{depth}", f"{depth}-hop fraud chain, as {depth} written hops",
         fraudChain(depth)),

        ("B", f"fraud_exact_{depth}", f"{depth}-hop fraud chain, as a quantifier",
         f"MATCH (a:Account)-[t:TRANSFER *{depth}..{depth} {FRAUD_HOP}]->(b) RETURN count(b)"),
    ]

QUERIES += [
    ("B", "fraud_upto_4", "Fraud chains of up to 4 transfers",
     f"MATCH (a:Account)-[t:TRANSFER *1..4 {FRAUD_HOP}]->(b) RETURN count(b)"),

    ("B", "fraud_closure", "Every fraud chain there is, any length",
     f"MATCH (a:Account)-[t:TRANSFER *1.. {FRAUD_HOP}]->(b) RETURN count(b)"),

    ("B", "fraud_closure_ends", "Every account a fraud chain reaches",
     f"MATCH (a:Account)-[t:TRANSFER *1.. {FRAUD_HOP}]->(b) RETURN count(DISTINCT b)"),

    ("B", "high_amount_2", "2 hops of high-value transfers into a high-risk account",
     f"MATCH (a:Account)-[t:TRANSFER *2..2 (e, n | e.amount > {HIGH_AMOUNT})]->(b:Account) "
     f"WHERE b.risk_score > 0.9 RETURN count(b)"),

    ("C", "ring_seed", f"Does account {RING_SEED} sit on a laundering ring",
     f"MATCH (a:Account {{account_id: {RING_SEED}}})-[t:TRANSFER *1..{RING_DEPTH} {FRAUD_HOP}]->(a) "
     f"RETURN count(a)"),

    ("C", "ring_all", "Every account on a laundering ring, whole graph",
     f"MATCH (a:Account)-[t:TRANSFER *1..{RING_DEPTH} {FRAUD_HOP}]->(a) RETURN count(a)"),

    ("C", "ring_all_unbounded", "Every account on a laundering ring, no depth bound",
     f"MATCH (a:Account)-[t:TRANSFER *1.. {FRAUD_HOP}]->(a) RETURN count(a)"),

    ("C", "ring_untyped", "Every account on a 3-transfer ring, fraudulent or not",
     "MATCH (a:Account)-[:TRANSFER*1..3]->(a) RETURN count(a)"),

    ("C", "ring_seed_depth", f"How many accounts {RING_SEED} launders through",
     f"MATCH (a:Account {{account_id: {RING_SEED}}})-[t:TRANSFER *1..{RING_DEPTH} {FRAUD_HOP}]->(a) "
     f"RETURN size(t)"),

    ("C", "ring_depths", "Every ring's length, whole graph",
     f"MATCH (a:Account)-[t:TRANSFER *1..{RING_DEPTH} {FRAUD_HOP}]->(a) RETURN size(t), count(a)"),

    ("D", "ball_2", "Within 2 transfers of every account",
     "MATCH (a:Account)-[:TRANSFER*1..2]->(b) RETURN count(b)"),

    ("D", "ball_3", "Within 3 transfers of the 550 ring accounts",
     "MATCH (a:Account)-[t:TRANSFER]->(s) WHERE t.is_fraud = true "
     "MATCH (s)-[:TRANSFER*1..3]->(b) RETURN count(b)"),

    ("D", "ball_seed_5", "Within 5 transfers of the seed account",
     f"MATCH (a:Account {{account_id: {RING_SEED}}})-[:TRANSFER*1..5]->(b) RETURN count(b)"),

    ("D", "ball_seed_6_ends", "Accounts within 6 transfers of the seed account",
     f"MATCH (a:Account {{account_id: {RING_SEED}}})-[:TRANSFER*1..6]->(b) RETURN count(DISTINCT b)"),
]

QUERIES += sweep("seed", "Downstream of one account",
                 f"(a:Account {{{{account_id: {RING_SEED}}}}})-[:TRANSFER{{hops}}]->(b)",
                 [2, 4, 6, 7, 8],
                 [2, 4, 6, 7, 8, 10, 14, None])

QUERIES += sweep("ring", "Downstream of the 550 ring accounts",
                 "(a:Account)-[t0:TRANSFER]->(s) WHERE t0.is_fraud = true "
                 "MATCH (s)-[:TRANSFER{hops}]->(b)",
                 [2, 3, 4, 5],
                 [2, 3, 4, 5, 6])


# id -> the count the README records for it
EXPECTED_COUNTS = {
    "seed": 1,
    "fanout_hops_1": 9,
    "fanout_exact_1": 9,
    "fanout_hops_2": 91,
    "fanout_exact_2": 91,
    "fanout_upto_2": 100,
    "fanout_hops_3": 844,
    "fanout_exact_3": 844,
    "fanout_hops_4": 7554,
    "fanout_exact_4": 7554,
    "fanout_upto_4": 8498,

    "fraud_hops_2": 550,
    "fraud_exact_2": 550,
    "fraud_hops_3": 550,
    "fraud_exact_3": 550,
    "fraud_hops_4": 550,
    "fraud_exact_4": 550,
    "fraud_upto_4": 2200,
    "fraud_closure": 3166,
    "fraud_closure_ends": 550,
    "high_amount_2": 62,

    "ring_seed": 1,
    "ring_all": 550,
    "ring_all_unbounded": 550,
    "ring_untyped": 827,
    "ring_seed_depth": 5,
    "ring_depths": 4,

    "ball_2": 90019597,
    "ball_3": 500111,
    "ball_seed_5": 76009,
    "ball_seed_6_ends": 480764,

    "seed_walk_2": 100,
    "seed_walk_4": 8498,
    "seed_walk_6": 683167,
    "seed_walk_7": 6151203,
    "seed_walk_8": 55375613,
    "seed_search_2": 100,
    "seed_search_4": 8465,
    "seed_search_6": 480764,
    "seed_search_7": 986834,
    "seed_search_8": 999871,
    "seed_search_10": 999888,
    "seed_search_14": 999888,
    "seed_search_inf": 999888,

    "ring_walk_2": 55001,
    "ring_walk_3": 500111,
    "ring_walk_4": 4506417,
    "ring_walk_5": 40573269,
    "ring_search_2": 48142,
    "ring_search_3": 351083,
    "ring_search_4": 957553,
    "ring_search_5": 999836,
    "ring_search_6": 999888,
}


def breadthFirst(query):
    if "*1.." not in query or "]->(a) " in query:
        return None

    return query.replace("*1..]", " *BFS]").replace("*1..", " *BFS 1..")


# The row-returning queries as an aggregate the server computes, so their time excludes
# serializing every row over bolt and hydrating it into python
def serverSideCount(query):
    head, _, projection = query.rpartition(" RETURN ")
    if not head or projection.startswith("count("):
        return None

    if projection.startswith("DISTINCT "):
        return f"{head} RETURN count(DISTINCT {projection[len('DISTINCT '):]})"

    if "," in projection:
        return f"{head} RETURN count(*)"

    return f"{head} RETURN count({projection})"


def runOnce(session, query):
    start = time.perf_counter()
    result = session.run(query)
    records = list(result)
    elapsed = (time.perf_counter() - start) * 1000

    value = None
    if len(records) == 1 and len(records[0]) == 1:
        only = records[0][0]
        if isinstance(only, int):
            value = only

    return elapsed, len(records), value


def timeQuery(session, query, reps, budget):
    times = []
    rows = value = None

    for _ in range(reps):
        elapsed, rows, value = runOnce(session, query)
        times.append(elapsed)

        if sum(times) > budget * 1000:
            break

    return {"median": statistics.median(times[1:]) if len(times) > 1 else times[0],
            "times": times, "rows": rows, "value": value}


def measure(args, selected, done):
    driver = neo4j.GraphDatabase.driver(args.uri, auth=("", ""))

    with driver.session() as session:
        session.run(f'SET DATABASE SETTING "query.timeout" TO "{int(args.timeout)}"').consume()

        with open(args.results, "a") as handle:
            for group, queryID, question, query in selected:
                if queryID in done:
                    continue

                record = {"group": group, "id": queryID, "question": question, "query": query}
                variants = [("written", query), ("counted", serverSideCount(query))]

                searching = "count(DISTINCT" in query or "RETURN DISTINCT" in query
                if args.bfs and searching:
                    variants.append(("bfs", breadthFirst(query)))

                for variant, text in variants:
                    if text is None:
                        continue

                    try:
                        record[variant] = timeQuery(session, text, args.reps, args.budget)
                    except Exception as error:
                        record[variant] = {"error": str(error)[:200]}

                handle.write(json.dumps(record) + "\n")
                handle.flush()

                written = record["written"]
                shown = f"{written['median']:10.2f} ms" if "median" in written else "   TIMEOUT"
                print(f"{group} {queryID:24s} {shown}  {written.get('rows', '')}", flush=True)

    driver.close()


def median(record, variant):
    result = record.get(variant)

    return result["median"] if result and "median" in result else None


def resultCount(record):
    written = record.get("written") or {}
    if written.get("value") is not None:
        return written["value"]

    if written.get("rows") is not None:
        return written["rows"]

    for variant in ("counted", "bfs"):
        result = record.get(variant) or {}
        if result.get("value") is not None:
            return result["value"]

    return None


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
        print("-" * 118)
        print(f"  {'question':52} {'memgraph':>10} {'mg count':>10} {'mg BFS':>10} "
              f"{'count':>13} {'expected':>13} {'match':>6}")

        for record in inGroup:
            expected = EXPECTED_COUNTS.get(record["id"])
            asWritten, withBFS = median(record, "written"), median(record, "bfs")
            counted = median(record, "counted")
            observed = resultCount(record)

            agrees = "" if expected is None or observed is None else ("yes" if expected == observed else "NO")
            error = (record.get("written") or {}).get("error")

            print(f"  {record['question'][:52]:52} {formatTime(asWritten)} {formatTime(counted)} {formatTime(withBFS)} "
                  f"{observed if observed is not None else '-':>13} {expected if expected is not None else '-':>13} {agrees:>6}"
                  f"{'' if not error else '  [' + error.split(':')[0][:44] + ']'}")

    disagreements = [(record["id"], EXPECTED_COUNTS[record["id"]], resultCount(record))
                     for record in records
                     if record["id"] in EXPECTED_COUNTS
                     and resultCount(record) is not None
                     and EXPECTED_COUNTS[record["id"]] != resultCount(record)]

    print()
    if not disagreements:
        print("Every count memgraph produced matches the value the README records.")
        return

    print("COUNT DISAGREEMENTS with the README")
    for queryID, expected, observed in disagreements:
        print(f"  {queryID}: README={expected} memgraph={observed}")


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("-uri", default="bolt://127.0.0.1:7688")
    parser.add_argument("-results", default=os.path.expanduser("~/.memgraph-fraud/results.jsonl"))
    parser.add_argument("-reps", type=int, default=5, help="runs per query; the first is a warmup")
    parser.add_argument("-timeout", type=float, default=300, help="seconds per query")
    parser.add_argument("-budget", type=float, default=600, help="seconds of repetitions per query")
    parser.add_argument("-groups", default="ABCDE")
    parser.add_argument("-only", default="", help="comma-separated query ids")
    parser.add_argument("-bfs", action="store_true", default=True,
                        help="also time the distinct queries as a *BFS expansion")
    parser.add_argument("-no-bfs", dest="bfs", action="store_false")
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
