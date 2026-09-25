#!/usr/bin/env python3
"""
Benchmark bounded and unbounded variable-length paths on reactome and the Santander
fraud graph, across graph databases. workloads.py holds every question: `reactome` and
`fraud` sweep hop bounds, `reactome-paths` asks the 71 questions of docs/path_bench.md,
and `reactome-hops` times one expansion at each exact hop count.

One question is written once, in plain openCypher, and sent to every database verbatim:
`-[e:hasEvent*1..4]->` for a bound, `-[e:hasEvent*]->` for none, `all(e IN t WHERE ...)`
for a predicate on every hop. Nothing is rewritten for the engine it is sent to, so a
database that cannot run a shape reports its error in the column where its time would be.

ladybug is the one exception: its dialect is not openCypher, so its client translates -
`clients.toLadybug` writes the quantifier as `* TRAIL 1..k` and turns a label into a
predicate where the fixture packs several labels into one table. That is a dialect, not
a rewrite of the question, and a translation that changed the question would show up as
a disagreeing count.

Each query is asked `-reps` times, the first run discarded as a warmup, and the median of
the rest reported. A query that runs past `-timeout` is killed and reported as TIMEOUT; a
query whose runs add up past `-budget` stops repeating. The queries of one sweep run in
order of their bound, and a sweep stops at its first failure, since the next bound is
deeper still.

turingdb and falkor report their own execution time and that is what is shown; driving
the turingdb shell over a pipe costs another 50 ms per query that its number excludes.
turingdb-embedded runs the engine in this process through its python bindings and is
timed around the call.
memgraph, neo4j and ladybug are timed at the client, since none of them reports a
server-side time - which for embedded ladybug is the same thing, and for bolt adds the
round trip.

Prerequisites:
  - turingdb built (build/tools/turingdb/turingdb)
  - the graphs loaded in their own turing dirs. A single lock guards a turing dir, so
    copy reactome out of ~/.turing rather than benchmarking in it:

      mkdir -p ~/.turing-bench/graphs && cp -r ~/.turing/graphs/reactome ~/.turing-bench/graphs/
      echo "LOAD PARQUET 'fraud_1m' AS fraud_1m" | turingdb -turing-dir ~/.turing-fraud

  - for turingdb-embedded, the python bindings built (python/turingdb/_embedded)
  - for the other clients, the same graph loaded where each of them reads it:
      memgraph, neo4j   `pip install neo4j`, bench/santander_fraud/memgraph_load_fraud.py
                        or scripts/memgraph_load_reactome_vlp.py, and
                        scripts/neo4j_load_reactome_vlp.py for neo4j
      ladybug           `pip install ladybug`, scripts/ladybug_load_reactome.py for
                        reactome, ./load_ladybug.py for fraud
      falkor            `pip install falkordb`, a falkordb server, ./load_falkor.py

The indexes a database carries decide what a seeded query pays before it walks anything:
the memgraph and falkor copies of reactome index TopLevelPathway(stId) and Reaction(stId),
their copies of the fraud graph carry no index at all, and ladybug has a primary key on
the node id and nothing else. Group S times that lookup on its own.

Examples:
  ./bench_vlp.py
  ./bench_vlp.py reactome -clients turingdb,memgraph,ladybug,falkor
  ./bench_vlp.py fraud -groups SAC -reps 9 -out fraud.json
  ./bench_vlp.py reactome-paths -clients turingdb,memgraph,ladybug -groups E
  ./bench_vlp.py reactome-hops -clients turingdb-embedded,memgraph -groups CE
"""

import argparse
import json
import os
import statistics
import sys

from dataclasses import dataclass, field

from clients import BoltClient, EmbeddedTuringDBClient, FalkorClient, LadybugClient, TuringDBClient
from workloads import WORKLOADS

CLIENTS = ("turingdb", "turingdb-embedded", "memgraph", "neo4j", "ladybug", "falkor")


@dataclass
class Measurement:
    times: list = field(default_factory=list)
    wallTimes: list = field(default_factory=list)
    rows: int = None
    value: int = None
    error: str = None

    def record(self, result):
        self.times.append(result.milliseconds)
        self.wallTimes.append(result.wallMilliseconds)
        self.error = result.error

        if result.rows is not None:
            self.rows = result.rows

        if result.value is not None:
            self.value = result.value

    @property
    def seconds(self):
        return sum(self.times) / 1000

    @property
    def median(self):
        if not self.times:
            return None
        elif len(self.times) == 1:
            return self.times[0]

        return statistics.median(self.times[1:])


def createClient(name, workload, args):
    fixture = workload.fixture

    if name == "turingdb":
        turingDir = os.path.expanduser(args.turingDir or fixture.turingDir)
        graph = args.turingGraph or fixture.graph

        return TuringDBClient(name, args.binary, turingDir, graph, args.port, args.timeout)
    elif name == "turingdb-embedded":
        turingDir = os.path.expanduser(args.turingDir or fixture.turingDir)
        graph = args.turingGraph or fixture.graph

        return EmbeddedTuringDBClient(name, turingDir, graph, args.timeout, args.sdkPath)
    elif name == "memgraph":
        return BoltClient("memgraph", args.boltURI or fixture.memgraphURI, args.timeout)
    elif name == "ladybug":
        database = os.path.expanduser(args.ladybugDB or fixture.ladybugDB)

        return LadybugClient("ladybug", database, fixture.ladybugNodeTable, args.timeout)
    elif name == "falkor":
        host, _, port = args.falkorURI.partition(":")

        return FalkorClient("falkor", host, int(port), fixture.falkorGraph, args.timeout)
    elif name == "neo4j":
        if not args.boltURI:
            raise ValueError("the neo4j client needs -bolt-uri: no fixture records where neo4j holds these graphs")

        user, _, password = args.boltAuth.partition(":")

        return BoltClient("neo4j", args.boltURI, args.timeout, auth=(user, password))
    else:
        raise ValueError(f"unknown client {name}")


def selectQueries(workload, groups, only):
    selected = []

    for query in workload.queries:
        if groups and query.group not in groups:
            continue
        elif only and query.queryID not in only:
            continue

        selected.append(query)

    return selected


def measure(client, queries, reps, budget):
    measurements = {}
    failedSweeps = set()

    for query in queries:
        if query.sweep in failedSweeps:
            print(f"  {client.name:14} {query.queryID:24} skipped", flush=True)
            continue

        measurement = Measurement()

        for _ in range(reps):
            try:
                measurement.record(client.run(query.cypher))
            except Exception as error:
                measurement.error = str(error)[:120]

            if measurement.error is not None or measurement.seconds > budget:
                break

        measurements[query.queryID] = measurement
        print(f"  {client.name:14} {query.queryID:24} {describe(measurement)}", flush=True)

        if measurement.error is not None and query.sweep:
            failedSweeps.add(query.sweep)

    return measurements


def describe(measurement):
    if measurement.error is not None:
        return measurement.error

    return f"{measurement.median:10.2f} ms  {measurement.value}"


def formatCell(measurement, width):
    if measurement is None:
        return f"{'-':>{width}}"
    elif measurement.error is not None:
        return f"{measurement.error.split(':')[0][:width - 1]:>{width}}"

    return f"{measurement.median:{width}.2f}"


def answeredValues(measurements):
    return {measurement.value for measurement in measurements if measurement is not None and measurement.value is not None}


def formatResult(values):
    if not values:
        return "-"
    elif len(values) > 1:
        return "differs"

    return str(next(iter(values)))


def report(workload, queries, results, clientNames):
    questionWidth = max(len(query.question) for query in queries) + 2
    clientWidth = 13

    print(f"\n{workload.name}: {workload.description}")

    for group, title in workload.groups.items():
        inGroup = [query for query in queries if query.group == group]
        if not inGroup:
            continue

        print(f"\n{group}. {title}")
        print("-" * (questionWidth + clientWidth * len(clientNames) + 14))

        header = f"  {'question':{questionWidth}}" + "".join(f"{name:>{clientWidth}}" for name in clientNames)
        print(header + f"{'result':>14}")

        for query in inGroup:
            measurements = [results[name].get(query.queryID) for name in clientNames]
            cells = "".join(formatCell(measurement, clientWidth) for measurement in measurements)

            print(f"  {query.question:{questionWidth}}{cells}{formatResult(answeredValues(measurements)):>14}")

    reportDisagreements(queries, results, clientNames)


def reportDisagreements(queries, results, clientNames):
    disagreements = []

    for query in queries:
        answers = {name: results[name][query.queryID].value for name in clientNames
                   if results[name].get(query.queryID) is not None and results[name][query.queryID].value is not None}

        if len(set(answers.values())) > 1:
            disagreements.append((query.question, answers))

    if not disagreements:
        print("\nEvery database that answered agreed on the result.")
        return

    print("\nDISAGREEMENTS")
    for question, answers in disagreements:
        print(f"  {question}: " + ", ".join(f"{name}={value}" for name, value in answers.items()))


def writeResults(path, workload, queries, results):
    records = []

    for query in queries:
        record = {"group": query.group, "id": query.queryID, "question": query.question, "cypher": query.cypher}

        for name, measurements in results.items():
            measurement = measurements.get(query.queryID)
            if measurement is None:
                continue

            record[name] = {"median": measurement.median,
                            "times": measurement.times,
                            "wallTimes": measurement.wallTimes,
                            "rows": measurement.rows,
                            "value": measurement.value,
                            "error": measurement.error}

        records.append(record)

    with open(path, "w") as handle:
        json.dump({"workload": workload.name, "queries": records}, handle, indent=2)


def runWorkload(workload, args, out):
    queries = selectQueries(workload, set(args.groups), set(filter(None, args.only.split(","))))
    if not queries:
        print(f"{workload.name}: no query selected", file=sys.stderr)
        return

    results = {}
    clientNames = []

    for name in args.clients.split(","):
        try:
            client = createClient(name, workload, args)

            print(f"\n{workload.name}: {len(queries)} queries on {client.name}", flush=True)
            with client:
                results[client.name] = measure(client, queries, args.reps, args.budget)
        except Exception as error:
            print(f"  {name} is unavailable: {error}", file=sys.stderr)
            continue

        clientNames.append(client.name)

    if clientNames:
        report(workload, queries, results, clientNames)

    if out:
        writeResults(out, workload, queries, results)


def main():
    repoRoot = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("workloads", nargs="*", default=list(WORKLOADS), help=f"one or more of {','.join(WORKLOADS)}")
    parser.add_argument("-clients", default="turingdb,memgraph", help=f"comma-separated, from {','.join(CLIENTS)}")
    parser.add_argument("-reps", type=int, default=5, help="runs per query; the first is a warmup")
    parser.add_argument("-timeout", type=float, default=60, help="seconds a single run may take")
    parser.add_argument("-budget", type=float, default=120, help="seconds a query may spend across its runs")
    parser.add_argument("-groups", default="", help="query groups to run; every group when empty")
    parser.add_argument("-only", default="", help="comma-separated query ids")
    parser.add_argument("-binary", default=os.path.join(repoRoot, "build/tools/turingdb/turingdb"))
    parser.add_argument("-port", type=int, default=6941, help="port for the turingdb client")
    parser.add_argument("-sdk", dest="sdkPath", default=os.path.join(repoRoot, "python"), help="where the turingdb python package lives")
    parser.add_argument("-turing-dir", dest="turingDir", default="", help="overrides the workload's turing dir")
    parser.add_argument("-turing-graph", dest="turingGraph", default="", help="overrides the workload's graph name")
    parser.add_argument("-bolt-uri", dest="boltURI", default="", help="overrides the workload's memgraph uri; required by the neo4j client")
    parser.add_argument("-bolt-auth", dest="boltAuth", default=":", help="user:password for the neo4j client")
    parser.add_argument("-ladybug-db", dest="ladybugDB", default="", help="overrides the workload's ladybug database")
    parser.add_argument("-falkor-uri", dest="falkorURI", default="127.0.0.1:6379", help="host:port of the falkordb server")
    parser.add_argument("-out", default="", help="write every measurement to this json file")
    args = parser.parse_args()

    for name in args.workloads:
        if name not in WORKLOADS:
            parser.error(f"unknown workload {name}")

    for name in args.clients.split(","):
        if name not in CLIENTS:
            parser.error(f"unknown client {name}")

    for name in args.workloads:
        out = args.out

        if out and len(args.workloads) > 1:
            stem, extension = os.path.splitext(out)
            out = f"{stem}-{name}{extension}"

        runWorkload(WORKLOADS[name], args, out)

    return 0


if __name__ == "__main__":
    sys.exit(main())
