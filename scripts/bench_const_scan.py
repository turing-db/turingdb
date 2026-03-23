#!/usr/bin/env python3

import json
import sys
import time

sys.path.insert(0, "/home/ubuntu/turingdb2/python")

from turingdb import TuringDB

SEED_DBIDS_PATH = "/home/ubuntu/turingdb-dgl/seed_dbids.json"
HOST = "http://localhost:6666"
GRAPH = "reactome"
LIMIT = 209500

def main():
    with open(SEED_DBIDS_PATH) as f:
        db_ids = json.load(f)

    print(f"Loaded {len(db_ids)} seed dbIds")

    client = TuringDB(host=HOST, timeout=300)
    client.set_graph(GRAPH)

    # Step 1: Map dbId -> NodeID
    or_chain = " OR ".join(f"n.dbId = {dbid}" for dbid in db_ids)
    mapping_query = f"MATCH (n) WHERE {or_chain} RETURN n"

    print("Resolving dbIds to NodeIDs...")
    df = client.query(mapping_query)
    node_ids = df["n"].tolist()
    print(f"Resolved {len(node_ids)} NodeIDs (from {len(db_ids)} dbIds)")

    if len(node_ids) == 0:
        print("No NodeIDs found, aborting.")
        return

    # Step 2: Build the 2-hop query with OR chain on NodeIDs
    or_chain = " OR ".join(f"n = {nid}" for nid in node_ids)
    query = f"MATCH (n)-->(m)-->(p) WHERE {or_chain} RETURN n, m, p LIMIT {LIMIT}"

    print(f"Query length: {len(query)} chars")

    # Step 3: Benchmark
    print("Running benchmark query...")
    t0 = time.perf_counter()
    result = client.query(query)
    t1 = time.perf_counter()

    wall_time_ms = (t1 - t0) * 1000
    server_time_ms = client.get_query_exec_time()

    print(f"Rows returned: {len(result)}")
    print(f"Server time:   {server_time_ms:.2f} ms")
    print(f"Wall time:     {wall_time_ms:.2f} ms")

if __name__ == "__main__":
    main()
