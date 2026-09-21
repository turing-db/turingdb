#!/usr/bin/env python3
"""
Load reactome into a neo4j database, from turingdb's own parquet dump of the graph.

The conversion is the one the memgraph loader does - one CSV per labelset, one per
relationship type - so this script imports it and changes only the dialect: `LOAD CSV
WITH HEADERS`, batched through `CALL { ... } IN TRANSACTIONS`, and neo4j's index syntax.
Labels and relationship types are written into the query text as literals, so nothing
here needs neo4j's dynamic labels.

neo4j reads `file:///` URLs out of its own import directory and nowhere else, so the
CSVs must sit in the directory the server mounts there:

  docker run -d --name neo4j-bench -p 7687:7687 -p 7474:7474 \
    -v ~/.neo4j-bench/import:/var/lib/neo4j/import -v ~/.neo4j-bench/data:/data \
    -e NEO4J_AUTH=neo4j/turingbench -e NEO4J_server_memory_heap_max__size=12G \
    -e NEO4J_server_memory_pagecache_size=16G -e NEO4J_db_transaction_timeout=0 \
    neo4j:5-community

  ./neo4j_load_reactome_vlp.py                 # convert and load
  ./neo4j_load_reactome_vlp.py -load-only      # the CSVs are already written

Every node carries the DatabaseObject label, and the edges are matched on its indexed
id, so the index is created between the nodes and the edges.
"""

import argparse
import os
import sys
import time

import neo4j

from memgraph_load_reactome_vlp import PROPERTIES, buildEdgeFiles, buildNodeFiles, onlyDirectory

BATCH_ROWS = 20000

SEED_LABELS = ["TopLevelPathway", "Reaction", "Complex"]


def loadNodes(session, nodeFiles):
    assignments = ", ".join(["id: toInteger(row.id)"] + [f"{name}: row.{name}" for name in PROPERTIES])

    for path, labels, count in nodeFiles:
        labelList = ":".join(f"`{label}`" for label in labels)
        start = time.perf_counter()

        session.run(f"LOAD CSV WITH HEADERS FROM 'file:///{os.path.basename(path)}' AS row "
                    f"CALL {{ WITH row CREATE (n:{labelList} {{{assignments}}}) }} "
                    f"IN TRANSACTIONS OF {BATCH_ROWS} ROWS").consume()

        print(f"  {count:9,} nodes  :{':'.join(labels)}  {time.perf_counter() - start:.1f}s", flush=True)


def loadEdges(session, edgeFiles):
    for path, name, count in edgeFiles:
        start = time.perf_counter()

        session.run(f"LOAD CSV WITH HEADERS FROM 'file:///{os.path.basename(path)}' AS row "
                    f"CALL {{ WITH row "
                    f"MATCH (a:DatabaseObject {{id: toInteger(row.src)}}), "
                    f"(b:DatabaseObject {{id: toInteger(row.dst)}}) "
                    f"CREATE (a)-[:`{name}`]->(b) }} "
                    f"IN TRANSACTIONS OF {BATCH_ROWS} ROWS").consume()

        print(f"  {count:9,} edges  :{name}  {time.perf_counter() - start:.1f}s", flush=True)


def createIndex(session, label, property):
    name = f"{label}_{property}".lower()

    session.run(f"CREATE INDEX {name} IF NOT EXISTS FOR (n:{label}) ON (n.{property})").consume()
    session.run("CALL db.awaitIndexes()").consume()


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("-dump", default=os.path.expanduser("~/reactome-parquet-dump"))
    parser.add_argument("-import", dest="importDirectory",
                        default=os.path.expanduser("~/.neo4j-bench/import"))
    parser.add_argument("-uri", default="bolt://127.0.0.1:7687")
    parser.add_argument("-auth", default="neo4j:turingbench", help="user:password")
    parser.add_argument("-convert-only", action="store_true", dest="convertOnly")
    parser.add_argument("-load-only", action="store_true", dest="loadOnly")
    args = parser.parse_args()

    part = onlyDirectory(args.dump + "/dataparts/*")
    commit = onlyDirectory(args.dump + "/commits/*/labels.parquet").rsplit("/", 1)[0]
    os.makedirs(args.importDirectory, exist_ok=True)

    start = time.perf_counter()
    write = not args.loadOnly
    nodeFiles, nodeCount = buildNodeFiles(part, commit, args.importDirectory, write)
    edgeFiles = buildEdgeFiles(part, commit, args.importDirectory, write)
    edgeCount = sum(count for _, _, count in edgeFiles)

    if write:
        print(f"converted {nodeCount:,} nodes in {len(nodeFiles)} labelsets and "
              f"{edgeCount:,} edges in {len(edgeFiles)} types "
              f"in {time.perf_counter() - start:.1f}s", flush=True)

    if args.convertOnly:
        return 0

    user, _, password = args.auth.partition(":")
    driver = neo4j.GraphDatabase.driver(args.uri, auth=(user, password))

    with driver.session() as session:
        loadNodes(session, nodeFiles)
        createIndex(session, "DatabaseObject", "id")
        loadEdges(session, edgeFiles)

        for label in SEED_LABELS:
            createIndex(session, label, "stId")

        nodes = session.run("MATCH (n) RETURN count(n)").single()[0]
        edges = session.run("MATCH ()-[e]->() RETURN count(e)").single()[0]
        print(f"loaded {nodes:,} nodes and {edges:,} edges in {time.perf_counter() - start:.1f}s")

    driver.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
