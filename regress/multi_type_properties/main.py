import pandas as pd
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
    result = client.query("MATCH (n:Person) RETURN n.name, n.age, n.isFrench")

    names = list(result["n.name"])
    ages = list(result["n.age"])
    is_french = list(result["n.isFrench"])

    # Expected data based on SimpleGraph.cpp
    # Only Remy and Adam have age property set
    expected = {
        "Adam": (32, True),
        "Cyrus": (None, False),
        "Doruk": (None, False),
        "Luc": (None, True),
        "Martina": (None, False),
        "Maxime": (None, True),
        "Remy": (32, True),
        "Suhas": (None, False),
    }

    assert len(names) == len(expected), f"Expected {len(expected)} persons, got {len(names)}"

    for i, name in enumerate(names):
        assert name in expected, f"Unexpected person '{name}'"
        exp_age, exp_french = expected[name]
        if exp_age is None:
            assert pd.isna(ages[i]), f"Expected age None for {name}, got {ages[i]}"
        else:
            assert ages[i] == exp_age, f"Expected age {exp_age} for {name}, got {ages[i]}"
        assert is_french[i] == exp_french, f"Expected isFrench {exp_french} for {name}, got {is_french[i]}"

    print(f"Verified {len(names)} persons with name (String), age (Int64), isFrench (Bool)")
    print("* multi_type_properties: done")

if __name__ == "__main__":
    main()
