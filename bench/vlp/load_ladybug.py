#!/usr/bin/env python3
"""
Load the fraud graph into a ladybug database, from the parquet turingdb imports.

One Account table and one TRANSFER table, so a label is a table of its own here and the
benchmark's queries need no boolean label columns. reactome has its own loader,
scripts/ladybug_load_reactome.py, which packs six labels per node into one Node table.

  ./load_ladybug.py
  ./load_ladybug.py -parquet ~/.turing-fraud/data/fraud_1m -db ~/.ladybug-fraud/fraud_1m

COPY reads parquet directly, so the staging step only drops the columns the benchmark
does not read and casts account_id down to the signed integer ladybug stores.
"""

import argparse
import os
import shutil
import sys
import time

import ladybug
import pyarrow as pa
import pyarrow.parquet as pq

ACCOUNT_PROPERTIES = ["account_id", "balance", "risk_score"]
TRANSFER_PROPERTIES = ["amount", "is_fraud"]


def buildStagingFiles(parquet, staging):
    nodes = pq.read_table(parquet + "/nodes.parquet", columns=["__id"] + ACCOUNT_PROPERTIES)
    nodes = nodes.rename_columns(["id"] + ACCOUNT_PROPERTIES)
    nodes = nodes.set_column(1, "account_id", nodes.column("account_id").cast(pa.int64()))
    pq.write_table(nodes, staging + "/accounts.parquet")

    edges = pq.read_table(parquet + "/edges.parquet", columns=["__source", "__target"] + TRANSFER_PROPERTIES)
    edges = edges.rename_columns(["src", "dst"] + TRANSFER_PROPERTIES)
    pq.write_table(edges, staging + "/transfers.parquet")

    return nodes.num_rows, edges.num_rows


def loadFraud(connection, staging):
    connection.execute("CREATE NODE TABLE Account(id INT64, account_id INT64, balance DOUBLE, "
                       "risk_score DOUBLE, PRIMARY KEY(id))")
    connection.execute(f"COPY Account FROM '{staging}/accounts.parquet'")

    connection.execute("CREATE REL TABLE TRANSFER(FROM Account TO Account, amount DOUBLE, is_fraud BOOLEAN)")
    connection.execute(f"COPY TRANSFER FROM '{staging}/transfers.parquet'")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("-parquet", default=os.path.expanduser("~/.turing-fraud/data/fraud_1m"))
    parser.add_argument("-db", default=os.path.expanduser("~/.ladybug-fraud/fraud_1m"))
    parser.add_argument("-staging", default=os.path.expanduser("~/.ladybug-fraud/staging"))
    parser.add_argument("-replace", action="store_true", help="delete the database first")
    args = parser.parse_args()

    if os.path.exists(args.db):
        if not args.replace:
            print(f"{args.db} already exists; pass -replace to rebuild it", file=sys.stderr)
            return 1

        shutil.rmtree(args.db)

    os.makedirs(args.staging, exist_ok=True)

    start = time.perf_counter()
    nodeCount, edgeCount = buildStagingFiles(args.parquet, args.staging)
    print(f"staged {nodeCount:,} accounts and {edgeCount:,} transfers in {time.perf_counter() - start:.1f}s", flush=True)

    start = time.perf_counter()
    database = ladybug.Database(args.db)
    connection = ladybug.Connection(database)
    loadFraud(connection, args.staging)
    print(f"loaded into {args.db} in {time.perf_counter() - start:.1f}s")

    return 0


if __name__ == "__main__":
    sys.exit(main())
