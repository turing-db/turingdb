import turingdb

GRAPH_NAME = "simpledb"
EXPECTED_NODE_COUNT = 18

def main() -> None:
    client = turingdb.TuringDB(instance_id='', auth_token='', host='http://localhost:6666')
    print("Connected to DB")

    client.query(f"LOAD GRAPH {GRAPH_NAME}")
    print(f"Loaded graph {GRAPH_NAME}")
    client.set_graph(GRAPH_NAME)
    print(f"Set graph {GRAPH_NAME}")

    result = client.query("MATCH (n) RETURN COUNT(n)")
    count = result[0][0]

    assert count == EXPECTED_NODE_COUNT, f"Expected {EXPECTED_NODE_COUNT} nodes, got {count}"
    print(f"COUNT(n) returned {count} as expected")

    print("* count_simpledb: done")

if __name__ == "__main__":
    main()
