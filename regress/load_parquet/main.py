import turingdb

# `typed` is a directory under the data dir holding nodes.parquet + edges.parquet
# (see run.sh). It has 4 labelled nodes and 2 edges.
IMPORT_DIR = "typed"
GRAPH_NAME = "typed"
EXPECTED_NODE_COUNT = 4
EXPECTED_EDGE_COUNT = 2


def test_load_parquet_succeeds(client: turingdb.TuringDB) -> None:
    # A failed load raises TuringDBException, which fails the test.
    result = client.query(f"LOAD PARQUET '{IMPORT_DIR}' AS {GRAPH_NAME}")
    print(f"LOAD PARQUET result: {result}")

    print("* test_load_parquet_succeeds: PASSED")


def test_can_query_loaded_graph(client: turingdb.TuringDB) -> None:
    client.set_graph(GRAPH_NAME)

    nodes = client.query("MATCH (n) RETURN n")
    assert len(nodes) == EXPECTED_NODE_COUNT, \
        f"Expected {EXPECTED_NODE_COUNT} nodes, got {len(nodes)}"

    edges = client.query("MATCH ()-[r]->() RETURN r")
    assert len(edges) == EXPECTED_EDGE_COUNT, \
        f"Expected {EXPECTED_EDGE_COUNT} edges, got {len(edges)}"

    print("* test_can_query_loaded_graph: PASSED")


def main() -> None:
    client = turingdb.TuringDB(host="http://localhost:6666")
    client.try_reach()
    print("Connected to TuringDB")

    test_load_parquet_succeeds(client)
    test_can_query_loaded_graph(client)

    print("* load_parquet: ALL PASSED")


if __name__ == "__main__":
    main()
