#!/usr/bin/env python3
"""
Load a benchmark graph into falkordb, from the parquet each fixture already keeps.

fraud is gen-fraud-graph's export: one nodes.parquet of :Account rows keyed by `__id`,
one edges.parquet of :TRANSFER rows keyed by `__source` and `__target`. reactome is the
dump the ladybug loader reads: one nodes.parquet whose labels are boolean columns, and
one edge_<type>.parquet of src and dst per edge type.

Both copies carry what memgraph's copy of the same graph carries, so the two are
comparable: the same properties, and the same indexes - stId on the seeded labels for
reactome, none at all for fraud, where the published methodology is index-free. Every
reactome node also takes the DatabaseObject label, which the id index hangs on.

  ./load_falkor.py fraud
  ./load_falkor.py reactome -parquet ~/lbbench/data

Rows go in through UNWIND batches over the redis protocol.
"""

import argparse
import glob
import os
import sys
import time

import numpy as np
import pyarrow.parquet as pq

from falkordb import FalkorDB

LABELS = ["Event", "Pathway", "TopLevelPathway", "Reaction", "ReactionLikeEvent",
          "Complex", "PhysicalEntity", "Species"]

REACTOME_PROPERTIES = ["stId", "displayName", "schemaClass", "speciesName"]
REACTOME_INDEXED = ["TopLevelPathway", "Reaction", "Complex"]

ACCOUNT_PROPERTIES = ["account_id", "balance", "risk_score"]
TRANSFER_PROPERTIES = ["amount", "is_fraud"]


def batches(table, size):
    for offset in range(0, table.num_rows, size):
        yield table.slice(offset, size).to_pylist()


def propertyMap(names):
    return "{" + ", ".join(f"{name}: row.{name}" for name in names) + "}"


def insertNodes(graph, labels, properties, table, batchSize):
    query = f"UNWIND $rows AS row CREATE (n:{':'.join(labels)} {propertyMap(['id'] + properties)})"

    for rows in batches(table, batchSize):
        graph.query(query, params={"rows": rows})


def insertEdges(graph, label, edgeType, properties, table, batchSize):
    query = (f"UNWIND $rows AS row MATCH (a:{label} {{id: row.src}}), (b:{label} {{id: row.dst}}) "
             f"CREATE (a)-[:{edgeType} {propertyMap(properties)}]->(b)")

    for rows in batches(table, batchSize):
        graph.query(query, params={"rows": rows})


def loadFraud(graph, parquet, batchSize):
    nodes = pq.read_table(parquet + "/nodes.parquet", columns=["__id"] + ACCOUNT_PROPERTIES)
    nodes = nodes.rename_columns(["id"] + ACCOUNT_PROPERTIES)

    graph.query("CREATE INDEX FOR (n:Account) ON (n.id)")

    start = time.perf_counter()
    insertNodes(graph, ["Account"], ACCOUNT_PROPERTIES, nodes, batchSize)
    print(f"  {nodes.num_rows:,} accounts in {time.perf_counter() - start:.1f}s", flush=True)

    edges = pq.read_table(parquet + "/edges.parquet",
                          columns=["__source", "__target"] + TRANSFER_PROPERTIES)
    edges = edges.rename_columns(["src", "dst"] + TRANSFER_PROPERTIES)

    start = time.perf_counter()
    insertEdges(graph, "Account", "TRANSFER", TRANSFER_PROPERTIES, edges, batchSize)
    print(f"  {edges.num_rows:,} transfers in {time.perf_counter() - start:.1f}s", flush=True)

    graph.query("DROP INDEX ON :Account(id)")


# The labels of one node, read off the boolean columns the dump writes them as
def labelSets(nodes):
    flags = np.stack([nodes.column(f"is{label}").to_numpy(zero_copy_only=False) for label in LABELS])
    codes = np.zeros(nodes.num_rows, dtype=np.int64)

    for index in range(len(LABELS)):
        codes |= flags[index].astype(np.int64) << index

    for code in np.unique(codes):
        labels = [LABELS[index] for index in range(len(LABELS)) if code >> index & 1]

        yield ["DatabaseObject"] + labels, np.flatnonzero(codes == code)


def loadReactome(graph, parquet, batchSize):
    nodes = pq.read_table(parquet + "/nodes.parquet",
                          columns=["id"] + REACTOME_PROPERTIES + [f"is{label}" for label in LABELS])

    graph.query("CREATE INDEX FOR (n:DatabaseObject) ON (n.id)")

    start = time.perf_counter()
    for labels, indices in labelSets(nodes):
        insertNodes(graph, labels, REACTOME_PROPERTIES, nodes.take(indices), batchSize)
    print(f"  {nodes.num_rows:,} nodes in {time.perf_counter() - start:.1f}s", flush=True)

    total = 0
    start = time.perf_counter()
    for path in sorted(glob.glob(parquet + "/edge_*.parquet")):
        edgeType = os.path.basename(path)[len("edge_"):-len(".parquet")]
        edges = pq.read_table(path)
        total += edges.num_rows

        insertEdges(graph, "DatabaseObject", edgeType, [], edges, batchSize)
    print(f"  {total:,} edges in {time.perf_counter() - start:.1f}s", flush=True)

    for label in REACTOME_INDEXED:
        graph.query(f"CREATE INDEX FOR (n:{label}) ON (n.stId)")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("workload", choices=("fraud", "reactome"))
    parser.add_argument("-parquet", default="", help="the directory the graph's parquet is in")
    parser.add_argument("-falkor-uri", dest="falkorURI", default="127.0.0.1:6379")
    parser.add_argument("-graph", default="", help="the graph key to write; defaults to the workload's fixture")
    parser.add_argument("-batch", type=int, default=5000, help="rows per UNWIND")
    parser.add_argument("-replace", action="store_true", help="delete the graph key first")
    args = parser.parse_args()

    sources = {"fraud": os.path.expanduser("~/.turing-fraud/data/fraud_1m"),
               "reactome": os.path.expanduser("~/lbbench/data")}
    keys = {"fraud": "fraud_1m", "reactome": "reactome"}

    parquet = args.parquet or sources[args.workload]
    host, _, port = args.falkorURI.partition(":")

    database = FalkorDB(host=host, port=int(port))
    key = args.graph or keys[args.workload]

    if key in database.list_graphs():
        if not args.replace:
            print(f"{key} is already loaded; pass -replace to rebuild it", file=sys.stderr)
            return 1

        database.select_graph(key).delete()

    graph = database.select_graph(key)

    start = time.perf_counter()
    if args.workload == "fraud":
        loadFraud(graph, parquet, args.batch)
    else:
        loadReactome(graph, parquet, args.batch)

    nodes = graph.query("MATCH (n) RETURN count(n)").result_set[0][0]
    edges = graph.query("MATCH ()-[r]->() RETURN count(r)").result_set[0][0]
    print(f"{key} holds {nodes:,} nodes and {edges:,} edges, loaded in {time.perf_counter() - start:.1f}s")

    return 0


if __name__ == "__main__":
    sys.exit(main())
