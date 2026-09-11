#!/usr/bin/env python3
"""
Load a gen-fraud-graph parquet export into a memgraph database.

The export is already the shape LOAD PARQUET wants - one `nodes.parquet` of `:Account`
rows keyed by `__id`, one `edges.parquet` of `:TRANSFER` rows keyed by `__source` and
`__target` - so the conversion is one CSV each, and the labels and the type go into the
LOAD CSV statement as literals.

  ./memgraph_load_fraud.py -parquet ~/fraud_1m_parquet -import ~/.memgraph-fraud/import

Takes about 6 min for 1,000,000 accounts and 9,000,550 transfers.
"""

import argparse
import os
import time

import neo4j
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq

NODE_PROPERTIES = ["account_id", "balance", "risk_score"]
EDGE_PROPERTIES = ["amount", "is_fraud"]


def buildNodeFile(parquet, out):
    table = pq.read_table(parquet + "/nodes.parquet", columns=["__id"] + NODE_PROPERTIES)
    columns = {"id": table.column("__id")}
    for name in NODE_PROPERTIES:
        columns[name] = table.column(name)

    path = out + "/accounts.csv"
    csv.write_csv(pa.table(columns), path)

    return path, table.num_rows


def buildEdgeFile(parquet, out):
    table = pq.read_table(parquet + "/edges.parquet",
                          columns=["__source", "__target"] + EDGE_PROPERTIES)
    columns = {"src": table.column("__source"), "dst": table.column("__target"),
               "amount": table.column("amount"),
               "is_fraud": table.column("is_fraud").cast(pa.int8())}

    path = out + "/transfers.csv"
    csv.write_csv(pa.table(columns), path)

    return path, table.num_rows


def loadDatabase(session, nodePath, edgePath, mount):
    session.run("STORAGE MODE IN_MEMORY_ANALYTICAL").consume()
    session.run("CREATE INDEX ON :Account(id)").consume()

    start = time.perf_counter()
    session.run(f'LOAD CSV FROM "{mount}/{os.path.basename(nodePath)}" WITH HEADER AS row '
                f'CREATE (n:Account {{id: ToInteger(row.id), '
                f'account_id: ToInteger(row.account_id), '
                f'balance: ToFloat(row.balance), risk_score: ToFloat(row.risk_score)}})').consume()
    print(f"  accounts loaded in {time.perf_counter() - start:.1f}s", flush=True)

    start = time.perf_counter()
    session.run(f'LOAD CSV FROM "{mount}/{os.path.basename(edgePath)}" WITH HEADER AS row '
                f'MATCH (a:Account {{id: ToInteger(row.src)}}), (b:Account {{id: ToInteger(row.dst)}}) '
                f'CREATE (a)-[:TRANSFER {{amount: ToFloat(row.amount), '
                f'is_fraud: ToInteger(row.is_fraud) = 1}}]->(b)').consume()
    print(f"  transfers loaded in {time.perf_counter() - start:.1f}s", flush=True)

    session.run("CREATE INDEX ON :Account(account_id)").consume()


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("-parquet", default=os.path.expanduser("~/fraud_1m_parquet"))
    parser.add_argument("-import", dest="importDirectory",
                        default=os.path.expanduser("~/.memgraph-fraud/import"))
    parser.add_argument("-mount", default="/import", help="where the import directory is mounted in the container")
    parser.add_argument("-uri", default="bolt://127.0.0.1:7688")
    parser.add_argument("-convert-only", action="store_true", dest="convertOnly")
    parser.add_argument("-load-only", action="store_true", dest="loadOnly")
    args = parser.parse_args()

    os.makedirs(args.importDirectory, exist_ok=True)

    nodePath = args.importDirectory + "/accounts.csv"
    edgePath = args.importDirectory + "/transfers.csv"

    if not args.loadOnly:
        start = time.perf_counter()
        nodePath, nodeCount = buildNodeFile(args.parquet, args.importDirectory)
        edgePath, edgeCount = buildEdgeFile(args.parquet, args.importDirectory)
        print(f"converted {nodeCount:,} nodes and {edgeCount:,} edges "
              f"in {time.perf_counter() - start:.1f}s", flush=True)

    if args.convertOnly:
        return 0

    driver = neo4j.GraphDatabase.driver(args.uri, auth=("", ""))
    with driver.session() as session:
        loadDatabase(session, nodePath, edgePath, args.mount)

        counts = session.run("MATCH (n) RETURN count(n) AS nodes").single()["nodes"]
        edges = session.run("MATCH ()-[r]->() RETURN count(r) AS edges").single()["edges"]
        print(f"memgraph holds {counts:,} nodes and {edges:,} edges", flush=True)

    driver.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
