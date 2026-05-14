import os
import turingdb

GRAPH_NAME = "simpledb"
EXPECTED_NODE_COUNT = 18

def main() -> None:
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".turing")
    db = turingdb.TuringDB(data_dir=data_dir)
    print(f"In-process TuringDB opened at {data_dir}")

    db.query(f"LOAD GRAPH {GRAPH_NAME}")
    print(f"Loaded graph {GRAPH_NAME}")
    db.set_graph(GRAPH_NAME)
    print(f"Set graph {GRAPH_NAME}")

    result = db.query("MATCH (n) RETURN COUNT(n)")
    count = result["COUNT(n)"][0]

    assert count == EXPECTED_NODE_COUNT, f"Expected {EXPECTED_NODE_COUNT} nodes, got {count}"
    print(f"COUNT(n) returned {count} as expected")

    print("* in_process_count_simpledb: done")

if __name__ == "__main__":
    main()
