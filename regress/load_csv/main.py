import turingdb

GRAPH_NAME = "csvtest"


def main() -> None:
    client = turingdb.TuringDB(host='http://localhost:6666')
    print("Connected to DB")

    client.query(f"LOAD GRAPH {GRAPH_NAME}")
    client.set_graph(GRAPH_NAME)
    print(f"Set graph {GRAPH_NAME}")

    csv_path = "test.csv"
    csv_no_headers_path = "test_no_headers.csv"

    # ---------------------------------------------------------------
    # Test 1: LOAD CSV without headers (index access)
    # ---------------------------------------------------------------
    print("\n=== Test 1: LOAD CSV without headers ===")
    query = (
        f"LOAD CSV '{csv_no_headers_path}' AS row "
        f"RETURN row[0] AS c0, row[1] AS c1, row[2] AS c2"
    )
    result = client.query(query)

    names = result["c0"].tolist()
    ages = result["c1"].tolist()
    cities = result["c2"].tolist()

    assert len(names) == 3, f"Expected 3 rows, got {len(names)}"
    assert set(names) == {"Alice", "Bob", "Charlie"}, \
        f"Expected names Alice/Bob/Charlie, got {names}"
    assert set(ages) == {"30", "25", "35"}, \
        f"Expected ages 30/25/35 (as strings), got {ages}"
    assert set(cities) == {"London", "Paris", "Berlin"}, \
        f"Expected cities London/Paris/Berlin, got {cities}"

    # Verify row order
    alice_idx = names.index("Alice")
    assert ages[alice_idx] == "30", \
        f"Alice's age should be '30', got '{ages[alice_idx]}'"
    assert cities[alice_idx] == "London", \
        f"Alice's city should be 'London', got '{cities[alice_idx]}'"

    bob_idx = names.index("Bob")
    assert ages[bob_idx] == "25", \
        f"Bob's age should be '25', got '{ages[bob_idx]}'"
    assert cities[bob_idx] == "Paris", \
        f"Bob's city should be 'Paris', got '{cities[bob_idx]}'"

    charlie_idx = names.index("Charlie")
    assert ages[charlie_idx] == "35", \
        f"Charlie's age should be '35', got '{ages[charlie_idx]}'"
    assert cities[charlie_idx] == "Berlin", \
        f"Charlie's city should be 'Berlin', got '{cities[charlie_idx]}'"

    print("  Index access: OK")
    print(f"  names={names}, ages={ages}, cities={cities}")

    # ---------------------------------------------------------------
    # Test 2: LOAD CSV WITH HEADERS (header access)
    # ---------------------------------------------------------------
    print("\n=== Test 2: LOAD CSV WITH HEADERS ===")
    query = (
        f"LOAD CSV '{csv_path}' WITH HEADERS AS row "
        f"RETURN row.name AS name, row.age AS age, row.city AS city"
    )
    result = client.query(query)

    names = result["name"].tolist()
    ages = result["age"].tolist()
    cities = result["city"].tolist()

    assert len(names) == 3, f"Expected 3 rows, got {len(names)}"
    assert set(names) == {"Alice", "Bob", "Charlie"}, \
        f"Expected names Alice/Bob/Charlie, got {names}"
    assert set(ages) == {"30", "25", "35"}, \
        f"Expected ages 30/25/35 (as strings), got {ages}"
    assert set(cities) == {"London", "Paris", "Berlin"}, \
        f"Expected cities London/Paris/Berlin, got {cities}"

    alice_idx = names.index("Alice")
    assert ages[alice_idx] == "30" and cities[alice_idx] == "London"
    bob_idx = names.index("Bob")
    assert ages[bob_idx] == "25" and cities[bob_idx] == "Paris"
    charlie_idx = names.index("Charlie")
    assert ages[charlie_idx] == "35" and cities[charlie_idx] == "Berlin"

    print("  Header access: OK")
    print(f"  names={names}, ages={ages}, cities={cities}")

    # ---------------------------------------------------------------
    # Test 3: LOAD CSV with LIMIT
    # ---------------------------------------------------------------
    print("\n=== Test 3: LOAD CSV with LIMIT ===")
    query = (
        f"LOAD CSV '{csv_no_headers_path}' AS row "
        f"RETURN row[0] AS c0 LIMIT 2"
    )
    result = client.query(query)

    names = result["c0"].tolist()
    assert len(names) == 2, f"Expected 2 rows with LIMIT 2, got {len(names)}"
    print(f"  LIMIT 2: OK (got {len(names)} rows)")

    print("\n* load_csv: all assertions passed")


if __name__ == "__main__":
    main()
