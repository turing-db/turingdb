import csv

import turingdb

GRAPH_NAME = "csvtest"


def load_csv_rows(path: str) -> list[dict[str, str]]:
    with open(path) as f:
        return list(csv.DictReader(f))


def new_change(client: turingdb.TuringDB) -> str:
    client.checkout('main')
    change_id: str = client.query("CHANGE NEW")['changeID'][0]
    client.checkout(change=change_id)
    return change_id


def submit_change(client: turingdb.TuringDB) -> None:
    client.query("CHANGE SUBMIT")
    client.checkout('main')


def main() -> None:
    client = turingdb.TuringDB(
        instance_id='', auth_token='', host='http://localhost:6666')
    print("Connected to DB")

    client.query(f"LOAD GRAPH {GRAPH_NAME}")
    client.set_graph(GRAPH_NAME)
    print(f"Set graph {GRAPH_NAME}")

    # Load CSV source data for verification
    people = load_csv_rows("people.csv")
    edges = load_csv_rows("edges.csv")

    # ---------------------------------------------------------------
    # Test 1: LOAD CSV + CREATE nodes with string and int properties
    # ---------------------------------------------------------------
    print("\n=== Test 1: LOAD CSV + CREATE nodes ===")

    new_change(client)
    client.query(
        "LOAD CSV 'people.csv' WITH HEADERS AS row "
        "CREATE (n:Person {name: row.name, age: toInteger(row.age), city: row.city})"
    )
    submit_change(client)

    # Query back and verify all nodes were created
    result = client.query("MATCH (n:Person) RETURN n.name, n.age, n.city")
    names = result["n.name"].tolist()
    ages = result["n.age"].tolist()
    cities = result["n.city"].tolist()

    assert len(names) == len(people), \
        f"Expected {len(people)} nodes, got {len(names)}"

    # Build lookup for verification
    actual = {}
    for i in range(len(names)):
        actual[names[i]] = {"age": ages[i], "city": cities[i]}

    for person in people:
        name = person["name"]
        assert name in actual, f"Missing node for {name}"
        assert actual[name]["age"] == int(person["age"]), \
            f"{name}: expected age {person['age']}, got {actual[name]['age']}"
        assert actual[name]["city"] == person["city"], \
            f"{name}: expected city {person['city']}, got {actual[name]['city']}"

    print(f"  Created {len(names)} nodes with correct properties: OK")

    # ---------------------------------------------------------------
    # Test 2: LOAD CSV + CREATE edges between new nodes
    # ---------------------------------------------------------------
    print("\n=== Test 2: LOAD CSV + CREATE edges ===")

    new_change(client)
    client.query(
        "LOAD CSV 'edges.csv' WITH HEADERS AS row "
        "CREATE (a:Friend {name: row.src})-[:KNOWS]->(b:Friend {name: row.tgt})"
    )
    submit_change(client)

    # Query back edges via pattern match
    result = client.query(
        "MATCH (a:Friend)-[:KNOWS]->(b:Friend) RETURN a.name, b.name"
    )
    src_names = result["a.name"].tolist()
    tgt_names = result["b.name"].tolist()

    assert len(src_names) == len(edges), \
        f"Expected {len(edges)} edges, got {len(src_names)}"

    actual_edges = set()
    for i in range(len(src_names)):
        actual_edges.add((src_names[i], tgt_names[i]))

    for edge in edges:
        pair = (edge["src"], edge["tgt"])
        assert pair in actual_edges, \
            f"Missing edge {pair[0]} -> {pair[1]}"

    print(f"  Created {len(src_names)} edges with correct relationships: OK")

    # ---------------------------------------------------------------
    # Test 3: LOAD CSV + CREATE with index access (no headers)
    # ---------------------------------------------------------------
    print("\n=== Test 3: LOAD CSV + CREATE with index access ===")

    new_change(client)
    client.query(
        "LOAD CSV 'people.csv' AS row "
        "CREATE (n:IndexedPerson {col0: row[0], col1: row[1], col2: row[2]})"
    )
    submit_change(client)

    result = client.query(
        "MATCH (n:IndexedPerson) RETURN n.col0, n.col1, n.col2"
    )
    col0s = result["n.col0"].tolist()
    col1s = result["n.col1"].tolist()
    col2s = result["n.col2"].tolist()

    # With no headers, first row is treated as data, so we get header row + data rows
    # people.csv has a header line "name,age,city", so row[0] of first line is "name"
    expected_count = len(people) + 1  # header row is also data
    assert len(col0s) == expected_count, \
        f"Expected {expected_count} nodes (header + data), got {len(col0s)}"

    # Verify the header row was treated as data (row[0]="name", row[1]="age", row[2]="city")
    actual_idx = {}
    for i in range(len(col0s)):
        actual_idx[col0s[i]] = {"col1": col1s[i], "col2": col2s[i]}
    assert "name" in actual_idx, "Header row should be treated as data when no WITH HEADERS"
    assert actual_idx["name"]["col1"] == "age", \
        f"Header row col1 should be 'age', got {actual_idx['name']['col1']}"

    print(f"  Created {len(col0s)} nodes via index access: OK")

    print("\n* load_csv_create: all assertions passed")


if __name__ == "__main__":
    main()
