import turingdb

GRAPH_NAME = "cyber_security"
EXPECTED_NODE_COUNT = 953
EXPECTED_EDGE_COUNT = 4698

# Expected node labels from the cyber-security-db
EXPECTED_LABELS = {"Group", "Domain", "OU", "User", "Computer", "GPO", "HighValue"}

# Expected edge types from the cyber-security-db
EXPECTED_EDGE_TYPES = {
    "GP_LINK", "CONTAINS", "GENERIC_ALL", "OWNS", "WRITE_OWNER",
    "WRITE_DACL", "DC_SYNC", "GET_CHANGES", "GET_CHANGES_ALL",
    "MEMBER_OF", "ADMIN_TO", "CAN_RDP", "EXECUTE_DCOM",
    "ALLOWED_TO_DELEGATE", "HAS_SESSION", "GENERIC_WRITE"
}


def main() -> None:
    client = turingdb.TuringDB(instance_id='', auth_token='', host='http://localhost:6666')
    print("Connected to DB")

    client.query(f"LOAD GRAPH {GRAPH_NAME}")
    print(f"Loaded graph {GRAPH_NAME}")
    client.set_graph(GRAPH_NAME)
    print(f"Set graph {GRAPH_NAME}")

    # Test node count
    result = client.query("MATCH (n) RETURN COUNT(n)")
    node_count = result["COUNT(n)"][0]
    assert node_count == EXPECTED_NODE_COUNT, f"Expected {EXPECTED_NODE_COUNT} nodes, got {node_count}"
    print(f"Node count: {node_count} (expected {EXPECTED_NODE_COUNT})")

    # Test edge count
    result = client.query("MATCH ()-[r]->() RETURN COUNT(r)")
    edge_count = result["COUNT(r)"][0]
    assert edge_count == EXPECTED_EDGE_COUNT, f"Expected {EXPECTED_EDGE_COUNT} edges, got {edge_count}"
    print(f"Edge count: {edge_count} (expected {EXPECTED_EDGE_COUNT})")

    # Test that we can query specific node labels
    for label in ["User", "Computer", "Group"]:
        result = client.query(f"MATCH (n:{label}) RETURN COUNT(n)")
        count = result["COUNT(n)"][0]
        assert count > 0, f"Expected at least one {label} node, got {count}"
        print(f"{label} nodes: {count}")

    # Test that we can query specific edge types
    for edge_type in ["MEMBER_OF", "ADMIN_TO", "HAS_SESSION"]:
        result = client.query(f"MATCH ()-[r:{edge_type}]->() RETURN COUNT(r)")
        count = result["COUNT(r)"][0]
        assert count > 0, f"Expected at least one {edge_type} edge, got {count}"
        print(f"{edge_type} edges: {count}")

    print("* neo4j_import: done")


if __name__ == "__main__":
    main()
