#!/usr/bin/env python3
"""
Load reactome into a memgraph database, from turingdb's own parquet dump of the graph.

Memgraph is schemaless and multi-label, so every node keeps the labels turingdb gives it
and every edge type becomes a relationship type. Nodes are written one CSV per labelset -
the dump stores them as contiguous ranges of one - and edges one CSV per type, which
LOAD CSV then reads with the labels and the type as literals.

  ./memgraph_load_reactome_vlp.py -dump ~/reactome-parquet-dump -import ~/.memgraph-bench/import

Takes about 15 min: 2,978,202 nodes, 11,537,843 edges, 83 labelsets, 88 relationship types.
"""

import argparse
import glob
import os
import sys
import time

import neo4j
import numpy as np
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq

PROPERTIES = {"stId": 14, "displayName": 3, "schemaClass": 12, "speciesName": 13}

# the labels the benchmark queries scan or seed on
INDEXED_LABELS = ["Event", "Pathway", "TopLevelPathway", "Reaction", "ReactionLikeEvent",
                  "Complex", "PhysicalEntity", "Species"]

SEED_LABELS = ["TopLevelPathway", "Reaction", "Complex"]


def onlyDirectory(pattern):
    matches = glob.glob(pattern)
    if len(matches) != 1:
        raise SystemExit(f"expected one directory matching {pattern}, found {len(matches)}")

    return matches[0]


def labelBits(row):
    return int(row[0]) | (int(row[1]) << 64) | (int(row[2]) << 128) | (int(row[3]) << 192)


def readLabelNames(commit):
    labels = pq.read_table(commit + "/labels.parquet").to_pydict()

    return {int(i): name for i, name in zip(labels["label_id"], labels["name"])}


def readLabelSets(commit, names):
    labelsets = pq.read_table(commit + "/labelsets.parquet").to_pydict()

    sets = {}
    for setID, *row in zip(labelsets["labelset_id"], labelsets["integer_0"],
                           labelsets["integer_1"], labelsets["integer_2"],
                           labelsets["integer_3"]):
        mask = labelBits(row)
        sets[int(setID)] = [name for bit, name in names.items() if mask >> bit & 1]

    return sets


def scatterProperty(part, propertyID, nodeCount):
    values = pq.read_table(part + f"/node-props-{propertyID}.parquet")
    scattered = np.full(nodeCount, None, dtype=object)
    scattered[values.column("entity_id").to_numpy()] = values.column("value").to_pylist()

    return scattered


def buildNodeFiles(part, commit, out, write=True):
    sets = readLabelSets(commit, readLabelNames(commit))
    ranges = pq.read_table(part + "/node-ranges.parquet").to_pydict()
    nodeCount = sum(int(count) for count in ranges["count"])

    columns = {}
    if write:
        columns = {name: scatterProperty(part, propertyID, nodeCount)
                   for name, propertyID in PROPERTIES.items()}

    files = []
    for setID, first, count in zip(ranges["labelset_id"], ranges["first_node_id"], ranges["count"]):
        setID, first, count = int(setID), int(first), int(count)
        path = out + f"/nodes_{setID}.csv"

        if write:
            table = {"id": pa.array(np.arange(first, first + count, dtype=np.int64))}
            for name, values in columns.items():
                table[name] = pa.array([sanitize(value) for value in values[first:first + count]],
                                       type=pa.string())

            csv.write_csv(pa.table(table), path)

        files.append((path, sets[setID], count))

    return files, nodeCount


def sanitize(value):
    if value is None:
        return None

    return value.replace("\r", " ").replace("\n", " ")


def buildEdgeFiles(part, commit, out, write=True):
    types = pq.read_table(commit + "/edge-types.parquet").to_pydict()
    names = {int(i): name for i, name in zip(types["edge_type_id"], types["name"])}

    table = pq.read_table(part + "/edges-out.parquet",
                          columns=["node_id", "other_id", "edge_type_id"])
    source = table.column("node_id").to_numpy().astype(np.int64)
    target = table.column("other_id").to_numpy().astype(np.int64)
    kind = table.column("edge_type_id").to_numpy()

    order = np.argsort(kind, kind="stable")
    source, target, kind = source[order], target[order], kind[order]
    boundaries = np.searchsorted(kind, np.arange(len(names) + 1))

    files = []
    for typeID, name in sorted(names.items()):
        start, stop = boundaries[typeID], boundaries[typeID + 1]
        if stop == start:
            continue

        path = out + f"/edges_{name}.csv"
        if write:
            csv.write_csv(pa.table({"src": pa.array(source[start:stop]),
                                    "dst": pa.array(target[start:stop])}), path)

        files.append((path, name, int(stop - start)))

    return files


def containerPath(path, importDirectory, mount):
    return mount + "/" + os.path.basename(path)


def loadDatabase(session, nodeFiles, edgeFiles, importDirectory, mount):
    session.run("STORAGE MODE IN_MEMORY_ANALYTICAL").consume()
    session.run("CREATE INDEX ON :DatabaseObject(id)").consume()

    assignments = ", ".join([f"id: ToInteger(row.id)"] +
                            [f"{name}: row.{name}" for name in PROPERTIES])

    for path, labels, count in nodeFiles:
        start = time.perf_counter()
        session.run(f'LOAD CSV FROM "{containerPath(path, importDirectory, mount)}" '
                    f'WITH HEADER NULLIF "" AS row '
                    f'CREATE (n:{":".join(labels)} {{{assignments}}})').consume()
        print(f"  {count:9,} nodes  :{':'.join(labels)}  "
              f"{time.perf_counter() - start:.1f}s", flush=True)

    for path, name, count in edgeFiles:
        start = time.perf_counter()
        session.run(f'LOAD CSV FROM "{containerPath(path, importDirectory, mount)}" '
                    f'WITH HEADER AS row '
                    f'MATCH (a:DatabaseObject {{id: ToInteger(row.src)}}), '
                    f'(b:DatabaseObject {{id: ToInteger(row.dst)}}) '
                    f'CREATE (a)-[:`{name}`]->(b)').consume()
        print(f"  {count:9,} edges  :{name}  {time.perf_counter() - start:.1f}s", flush=True)

    for label in INDEXED_LABELS:
        session.run(f"CREATE INDEX ON :{label}").consume()

    for label in SEED_LABELS:
        session.run(f"CREATE INDEX ON :{label}(stId)").consume()


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("-dump", default=os.path.expanduser("~/reactome-parquet-dump"))
    parser.add_argument("-import", dest="importDirectory",
                        default=os.path.expanduser("~/.memgraph-bench/import"))
    parser.add_argument("-mount", default="/import", help="where the import directory is mounted in the container")
    parser.add_argument("-uri", default="bolt://127.0.0.1:7687")
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

    driver = neo4j.GraphDatabase.driver(args.uri, auth=("", ""))
    with driver.session() as session:
        loadDatabase(session, nodeFiles, edgeFiles, args.importDirectory, args.mount)

        counts = session.run("MATCH (n) RETURN count(n)").single()[0]
        edges = session.run("MATCH ()-[e]->() RETURN count(e)").single()[0]
        print(f"loaded {counts:,} nodes and {edges:,} edges in {time.perf_counter() - start:.1f}s")

    driver.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
