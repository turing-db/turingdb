import turingdb

GRAPH_NAME = "simpledb"

def main() -> None:
    client = turingdb.TuringDB(instance_id='', auth_token='', host='http://localhost:6666')
    print("Connected to DB")

    client.query(f"LOAD GRAPH {GRAPH_NAME}")
    print(f"Loaded graph {GRAPH_NAME}")
    client.set_graph(GRAPH_NAME)
    print(f"Set graph {GRAPH_NAME}")

    # Query properties of different types: String, Int64, Bool
    result = client.query("MATCH (n:Person) RETURN n.name, n.age, n.isFrench ORDER BY n.name")

    names = list(result[0])
    ages = list(result[1])
    is_french = list(result[2])

    # Expected data based on SimpleGraph.cpp
    # Only Remy and Adam have age property set
    expected = [
        ("Adam", 32, True),
        ("Cyrus", None, False),
        ("Doruk", None, False),
        ("Luc", None, True),
        ("Martina", None, False),
        ("Maxime", None, True),
        ("Remy", 32, True),
        ("Suhas", None, False),
    ]

    assert len(names) == len(expected), f"Expected {len(expected)} persons, got {len(names)}"

    for i, (exp_name, exp_age, exp_french) in enumerate(expected):
        assert names[i] == exp_name, f"Expected name '{exp_name}', got '{names[i]}'"
        assert ages[i] == exp_age, f"Expected age {exp_age} for {exp_name}, got {ages[i]}"
        assert is_french[i] == exp_french, f"Expected isFrench {exp_french} for {exp_name}, got {is_french[i]}"

    print(f"Verified {len(names)} persons with name (String), age (Int64), isFrench (Bool)")
    print("* multi_type_properties: done")

if __name__ == "__main__":
    main()
