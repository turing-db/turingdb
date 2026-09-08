#!/usr/bin/env python3
"""
Load reactome into a ladybug database, from turingdb's own parquet dump of the graph.

Reactome nodes carry six labels each and ladybug is one label per node table, so every
node goes into one Node table and the labels the benchmark names become boolean columns.
Each edge type becomes its own rel table, which is what an untyped quantifier walks.

  ./ladybug_load_reactome.py -dump ~/reactome-parquet-dump -db ~/.ladybug-bench/reactome

Takes about 25 s: 2,978,202 nodes, 11,537,843 edges, 88 rel tables.
"""

import argparse
import glob
import os
import shutil
import sys
import time

import ladybug
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

# the labels the benchmark queries name, as `(a:Event)` becomes `(a:Node) ... a.isEvent`
LABELS = ["Event", "Pathway", "TopLevelPathway", "Reaction", "ReactionLikeEvent",
          "Complex", "PhysicalEntity", "Species"]

PROPERTIES = {"stId": 14, "displayName": 3, "schemaClass": 12, "speciesName": 13}


def onlyDirectory(pattern):
    matches = glob.glob(pattern)
    if len(matches) != 1:
        raise SystemExit(f"expected one directory matching {pattern}, found {len(matches)}")

    return matches[0]


def labelBits(row):
    return int(row[0]) | (int(row[1]) << 64) | (int(row[2]) << 128) | (int(row[3]) << 192)


def buildNodes(part, commit, out):
    labels = pq.read_table(commit + "/labels.parquet").to_pydict()
    labelID = {name: int(i) for i, name in zip(labels["label_id"], labels["name"])}

    labelsets = pq.read_table(commit + "/labelsets.parquet").to_pydict()
    masks = {int(setID): labelBits(row) for setID, *row in
             zip(labelsets["labelset_id"], labelsets["integer_0"], labelsets["integer_1"],
                 labelsets["integer_2"], labelsets["integer_3"])}

    ranges = pq.read_table(part + "/node-ranges.parquet").to_pydict()
    nodeCount = sum(int(count) for count in ranges["count"])

    flags = {name: np.zeros(nodeCount, dtype=bool) for name in LABELS}
    for setID, first, count in zip(ranges["labelset_id"], ranges["first_node_id"], ranges["count"]):
        mask = masks[int(setID)]
        first, count = int(first), int(count)

        for name in LABELS:
            if mask >> labelID[name] & 1:
                flags[name][first:first + count] = True

    columns = {"id": pa.array(np.arange(nodeCount, dtype=np.int64))}
    for name, propertyID in PROPERTIES.items():
        values = pq.read_table(part + f"/node-props-{propertyID}.parquet")
        scattered = np.full(nodeCount, None, dtype=object)
        scattered[values.column("entity_id").to_numpy()] = values.column("value").to_pylist()
        columns[name] = pa.array(scattered, type=pa.string())

    for name in LABELS:
        columns["is" + name] = pa.array(flags[name])

    pq.write_table(pa.table(columns), out + "/nodes.parquet", compression="snappy")

    return nodeCount


def buildEdges(part, commit, out):
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

    written = []
    for typeID, name in sorted(names.items()):
        start, stop = boundaries[typeID], boundaries[typeID + 1]
        if stop == start:
            continue

        edges = pa.table({"src": pa.array(source[start:stop]), "dst": pa.array(target[start:stop])})
        pq.write_table(edges, out + f"/edge_{name}.parquet", compression="snappy")
        written.append(name)

    return written


def createDatabase(database, out, edgeNames):
    connection = ladybug.Connection(database)

    columns = ["id INT64"]
    columns += [f"{name} STRING" for name in PROPERTIES]
    columns += [f"is{name} BOOL" for name in LABELS]

    connection.execute(f"CREATE NODE TABLE Node({', '.join(columns)}, PRIMARY KEY(id))")
    connection.execute(f"COPY Node FROM '{out}/nodes.parquet'")

    for name in edgeNames:
        connection.execute(f"CREATE REL TABLE {name}(FROM Node TO Node)")
        connection.execute(f"COPY {name} FROM '{out}/edge_{name}.parquet'")

    return connection


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("-dump", default=os.path.expanduser("~/reactome-parquet-dump"),
                        help="a turingdb parquet dump of the reactome graph")
    parser.add_argument("-db", default=os.path.expanduser("~/.ladybug-bench/reactome"))
    parser.add_argument("-staging", default="", help="where the COPY inputs go; defaults beside -db")
    args = parser.parse_args()

    part = onlyDirectory(args.dump + "/dataparts/*")
    commit = onlyDirectory(args.dump + "/commits/*/labels.parquet").rsplit("/", 1)[0]
    staging = args.staging or args.db + ".copy-inputs"

    os.makedirs(staging, exist_ok=True)
    shutil.rmtree(args.db, ignore_errors=True)

    start = time.time()
    nodeCount = buildNodes(part, commit, staging)
    edgeNames = buildEdges(part, commit, staging)
    print(f"converted {nodeCount} nodes and {len(edgeNames)} edge types "
          f"in {time.time() - start:.1f} s", flush=True)

    start = time.time()
    database = ladybug.Database(args.db, buffer_pool_size=16 * 1024**3, max_db_size=64 * 1024**3)
    connection = createDatabase(database, staging, edgeNames)
    print(f"copied into {args.db} in {time.time() - start:.1f} s", flush=True)

    for query in ["MATCH (n:Node) RETURN count(n)",
                  "MATCH ()-[e:input]->() RETURN count(e)"]:
        print(f"{query} -> {connection.execute(query).get_all()}", flush=True)

    return 0


if __name__ == "__main__":
    sys.exit(main())
