import os

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

import turingdb

GRAPH_NAME = "embeddingloaddb"
DIMENSION = 4

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, ".turing", "data")
PARQUET_NAME = "embeddings.parquet"


def write_embedding_parquet(node_ids, embeddings):
    """Write a FIXED_LEN_BYTE_ARRAY embedding Parquet file into the data dir.

    node_ids are internal TuringDB node IDs; embeddings are little-endian
    float32 vectors packed as a fixed-size binary column.
    """
    matrix = np.asarray(embeddings, dtype="<f4")
    blobs = [row.tobytes() for row in matrix]

    table = pa.table({
        "node_id": pa.array(node_ids, pa.int64()),
        "embedding": pa.array(blobs, pa.binary(DIMENSION * 4)),
    })

    os.makedirs(DATA_DIR, exist_ok=True)
    pq.write_table(table, os.path.join(DATA_DIR, PARQUET_NAME))


def load_embeddings(client):
    change = client.new_change()
    client.checkout(change=change)
    result = client.query(f'LOAD EMBEDDING FROM "{PARQUET_NAME}" AS emb')
    client.query("COMMIT")
    client.query("CHANGE SUBMIT")
    client.checkout()
    return result


def read_back(client):
    result = client.query("MATCH (n:Vec) RETURN n.name, n.emb")
    names = list(result["n.name"])
    vecs = list(result["n.emb"])
    return {name: list(vec) for name, vec in zip(names, vecs)}


def assert_close(actual, expected):
    assert len(actual) == len(expected), f"Dimension mismatch: {actual} vs {expected}"
    for a, b in zip(actual, expected):
        assert abs(a - b) < 1e-5, f"Value mismatch: {actual} vs {expected}"


def main():
    client = turingdb.TuringDB(host="http://localhost:6666")
    client.try_reach()

    print("Running LOAD EMBEDDING Test")

    client.query(f"CREATE GRAPH {GRAPH_NAME}")
    client.set_graph(GRAPH_NAME)

    # Create nodes in a fresh graph; internal node IDs are assigned
    # sequentially in creation order, so a=0, b=1, c=2.
    change = client.new_change()
    client.checkout(change=change)
    client.query('CREATE (n:Vec {name: "a"})')
    client.query('CREATE (n:Vec {name: "b"})')
    client.query('CREATE (n:Vec {name: "c"})')
    client.query("COMMIT")
    client.query("CHANGE SUBMIT")
    client.checkout()

    embeddings_by_name = {
        "a": [0.1, 0.2, 0.3, 0.4],
        "b": [0.5, 0.6, 0.7, 0.8],
        "c": [0.9, 1.0, 1.1, 1.2],
    }
    node_ids = [0, 1, 2]
    embeddings = [embeddings_by_name["a"],
                  embeddings_by_name["b"],
                  embeddings_by_name["c"]]

    # --- Test 1: embeddings are created on nodes that had none ---
    write_embedding_parquet(node_ids, embeddings)
    result = load_embeddings(client)
    assert list(result["count"]) == [3], f"Expected count 3, got {list(result['count'])}"

    loaded = read_back(client)
    assert len(loaded) == 3, f"Expected 3 nodes, got {len(loaded)}"
    for name, expected in embeddings_by_name.items():
        assert_close(loaded[name], expected)

    print("  Passed: embeddings created from Parquet")

    # --- Test 2: re-loading overwrites existing embeddings ---
    overwritten_by_name = {
        "a": [9.1, 9.2, 9.3, 9.4],
        "b": [8.5, 8.6, 8.7, 8.8],
        "c": [7.9, 7.0, 7.1, 7.2],
    }
    write_embedding_parquet(node_ids, [overwritten_by_name["a"],
                                       overwritten_by_name["b"],
                                       overwritten_by_name["c"]])
    load_embeddings(client)

    reloaded = read_back(client)
    for name, expected in overwritten_by_name.items():
        assert_close(reloaded[name], expected)

    print("  Passed: re-loading overwrites embeddings")

    print("* load_embedding: done")


if __name__ == "__main__":
    main()
