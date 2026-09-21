#!/usr/bin/env python3
"""
Latency of a fixed-length expansion from one seed, one hop count at a time.

A sweep is one pattern written as a function of the hop count: the same query at
`*1..1`, then `*2..2`, out to `-hops`. Every query this benchmark asks is one of the
one-line functions below, and the report prints the ones it timed above the table.

Each expansion is timed twice, once as a quantified path and once as the same hops
written out as k relationship patterns. The two do not answer quite the same question -
a quantified path walks trails, k patterns walk anything - so the pair reads as the
price of the quantifier, not as one query run two ways.

The report is latency and nothing else - the median of `-reps` runs of each query, in
ms, one row per hop count. turingdb runs in this process and is timed around the
`TuringDB::query` call; falkor reports its own execution time; bolt and ladybug are
timed at the client. A sweep stops at the first hop count that errors or outruns
`-timeout`, since the next one is deeper still.

`turingdb-shell` drives the shell over a pipe instead. Its sentinel forks a process
holding the whole graph, which costs ~90 ms per query on reactome, so it is there to
read the engine's own reported time, not to be raced against the others.

Another database plugs in as a client class: anything with `open()`, `close()` and a
`run(query)` returning a result that carries `milliseconds`. clients.py holds the ones
that exist - the turingdb shell, bolt for memgraph and neo4j, falkor, ladybug - and
`createClient` is where a new one is named.

The seed and the graph belong together: the default queries expand the reactome
TopLevelPathway R-HSA-162582, loaded as `reactome` in ~/.turing-bench.

Examples:
  ./bench_hops.py
  ./bench_hops.py -hops 12 -reps 3 -only untyped,chain
  ./bench_hops.py -client memgraph -bolt-uri bolt://127.0.0.1:7687
"""

import argparse
import os
import statistics
import sys

from dataclasses import dataclass

from clients import BoltClient, EmbeddedTuringDBClient, FalkorClient, LadybugClient, TuringDBClient

CLIENTS = ("turingdb", "turingdb-shell", "memgraph", "neo4j", "falkor", "ladybug")

SEED = "R-HSA-162582"
MATCH_SEED = f"MATCH (p:TopLevelPathway {{stId:'{SEED}'}})"


def untypedTrails(hops):
    return f"{MATCH_SEED}-[e*{hops}..{hops}]->(m) RETURN count(m)"


def untypedEnds(hops):
    return f"{MATCH_SEED}-[e*{hops}..{hops}]->(m) RETURN count(DISTINCT m)"


def hasEventTrails(hops):
    return f"{MATCH_SEED}-[e:hasEvent*{hops}..{hops}]->(a:Event) RETURN count(a)"


def chainPattern(hops, relationship, endLabel):
    pattern = MATCH_SEED

    for index in range(hops):
        label = endLabel if index == hops - 1 else ""
        pattern += f"-[{relationship}]->(n{index}{label})"

    return pattern


def untypedTrailsChain(hops):
    return f"{chainPattern(hops, '', '')} RETURN count(n{hops - 1})"


def untypedEndsChain(hops):
    return f"{chainPattern(hops, '', '')} RETURN count(DISTINCT n{hops - 1})"


def hasEventTrailsChain(hops):
    return f"{chainPattern(hops, ':hasEvent', ':Event')} RETURN count(n{hops - 1})"


@dataclass(frozen=True)
class Sweep:
    name: str
    question: str
    build: object


@dataclass(frozen=True)
class Timing:
    milliseconds: float = None
    error: str = None


SWEEPS = (Sweep("untyped", "every trail of exactly k hops, any edge type", untypedTrails),
          Sweep("untyped-chain", "the same expansion as k relationship patterns", untypedTrailsChain),
          Sweep("distinct", "the nodes those trails end on", untypedEnds),
          Sweep("distinct-chain", "the nodes those k patterns end on", untypedEndsChain),
          Sweep("hasEvent", "every hasEvent trail of exactly k hops", hasEventTrails),
          Sweep("hasEvent-chain", "the same hasEvent expansion as k patterns", hasEventTrailsChain))


def createClient(name, args):
    if name == "turingdb":
        return EmbeddedTuringDBClient(name, os.path.expanduser(args.turingDir), args.graph, args.timeout, args.sdkPath)
    elif name == "turingdb-shell":
        return TuringDBClient(name, args.binary, os.path.expanduser(args.turingDir), args.graph, args.port, args.timeout)
    elif name in ("memgraph", "neo4j"):
        user, _, password = args.boltAuth.partition(":")

        return BoltClient(name, args.boltURI, args.timeout, auth=(user, password))
    elif name == "falkor":
        host, _, port = args.falkorURI.partition(":")

        return FalkorClient(name, host, int(port), args.graph, args.timeout)
    elif name == "ladybug":
        return LadybugClient(name, os.path.expanduser(args.ladybugDB), args.ladybugNodeTable, args.timeout)

    raise ValueError(f"unknown client {name}")


def measure(client, query, reps, budget):
    times = []

    for _ in range(reps):
        result = client.run(query)

        if result.error is not None:
            return Timing(error=result.error)

        times.append(result.milliseconds)

        if sum(times) > budget * 1000:
            break

    if len(times) == 1:
        return Timing(milliseconds=times[0])

    return Timing(milliseconds=statistics.median(times[1:]))


def runSweep(client, sweep, hops, reps, budget):
    timings = {}

    for hopCount in hops:
        timing = measure(client, sweep.build(hopCount), reps, budget)
        timings[hopCount] = timing

        print(f"  {sweep.name:10} {hopCount:3} hops  {describe(timing)}", flush=True)

        if timing.error is not None:
            break

    return timings


def describe(timing):
    if timing.error is not None:
        return timing.error

    return f"{timing.milliseconds:12.2f} ms"


def formatCell(timing):
    if timing is None:
        return "-"
    elif timing.error is not None:
        return timing.error.split(":")[0][:12]

    return f"{timing.milliseconds:.2f}"


def formatRow(cells, widths):
    return "| " + " | ".join(f"{cell:>{width}}" for cell, width in zip(cells, widths)) + " |"


def renderTable(headers, rows):
    widths = [max(len(cell) for cell in column) for column in zip(headers, *rows)]
    rule = "+" + "+".join("-" * (width + 2) for width in widths) + "+"

    lines = [rule, formatRow(headers, widths), rule]
    lines += [formatRow(row, widths) for row in rows]
    lines.append(rule)

    return "\n".join(lines)


def report(client, sweeps, results, hops):
    sample = hops[-1]
    nameWidth = max(len(sweep.name) for sweep in sweeps)

    print(f"\n{client.name}: median latency in ms as the hop count grows")
    print(f"\nthe queries, at {sample} hop{'' if sample == 1 else 's'}:")

    for sweep in sweeps:
        print(f"  {sweep.name:{nameWidth}}  {sweep.build(sample)}")

    headers = ["hops"] + [sweep.name for sweep in sweeps]
    rows = [[str(hopCount)] + [formatCell(results[sweep.name].get(hopCount)) for sweep in sweeps] for hopCount in hops]

    print()
    print(renderTable(headers, rows))


def main():
    repoRoot = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("-client", default="turingdb", help=f"one of {','.join(CLIENTS)}")
    parser.add_argument("-hops", type=int, default=10, help="deepest hop count to time")
    parser.add_argument("-reps", type=int, default=5, help="runs per query; the first is a warmup")
    parser.add_argument("-timeout", type=float, default=400, help="seconds a single run may take")
    parser.add_argument("-budget", type=float, default=150, help="seconds a query may spend across its runs")
    parser.add_argument("-only", default="", help=f"comma-separated sweeps, from {','.join(sweep.name for sweep in SWEEPS)}")
    parser.add_argument("-binary", default=os.path.join(repoRoot, "build/tools/turingdb/turingdb"))
    parser.add_argument("-turing-dir", dest="turingDir", default="~/.turing-bench")
    parser.add_argument("-graph", default="reactome", help="the graph turingdb loads and falkor selects")
    parser.add_argument("-port", type=int, default=6941, help="port for the turingdb-shell client")
    parser.add_argument("-sdk", dest="sdkPath", default=os.path.join(repoRoot, "python"), help="where the turingdb python package lives")
    parser.add_argument("-bolt-uri", dest="boltURI", default="bolt://127.0.0.1:7687")
    parser.add_argument("-bolt-auth", dest="boltAuth", default=":", help="user:password for the bolt clients")
    parser.add_argument("-falkor-uri", dest="falkorURI", default="127.0.0.1:6379")
    parser.add_argument("-ladybug-db", dest="ladybugDB", default="~/lbbench/reactome.ladybug")
    parser.add_argument("-ladybug-node-table", dest="ladybugNodeTable", default="Node")
    args = parser.parse_args()

    if args.client not in CLIENTS:
        parser.error(f"unknown client {args.client}")

    selected = set(filter(None, args.only.split(",")))
    sweeps = [sweep for sweep in SWEEPS if not selected or sweep.name in selected]
    if not sweeps:
        parser.error(f"no sweep named {args.only}")

    hops = list(range(1, args.hops + 1))
    client = createClient(args.client, args)
    results = {}

    print(f"{client.name}: {len(sweeps)} sweeps over {len(hops)} hop counts", flush=True)

    with client:
        for sweep in sweeps:
            results[sweep.name] = runSweep(client, sweep, hops, args.reps, args.budget)

    report(client, sweeps, results, hops)

    return 0


if __name__ == "__main__":
    sys.exit(main())
