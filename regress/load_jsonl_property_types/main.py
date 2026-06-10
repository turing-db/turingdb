import os
import turingdb

GRAPH_NAME = "typecoverage"
JSONL_FILENAME = "type_coverage.jsonl"


def main() -> None:
    client = turingdb.TuringDB(host="http://localhost:6666")
    client.try_reach()
    print("Connected to TuringDB")

    result = client.query(f'LOAD JSONL "{JSONL_FILENAME}" AS {GRAPH_NAME}')
    print(f"LOAD JSONL result: {result}")

    client.set_graph(GRAPH_NAME)

    result = client.query("CALL db.propertyTypes()")
    print(f"Property types:\n{result}")

    result = result.sort_values("propertyType").reset_index(drop=True)

    expected = {
        "arrProp": "String",
        "boolProp": "Bool",
        "embProp": "Embedding",
        "floatProp": "Double",
        "intProp": "Int64",
        "strProp": "String",
    }

    actual_names = list(result["propertyType"])
    assert actual_names == sorted(expected.keys()), \
        f"Property names mismatch: {actual_names}"

    for _, row in result.iterrows():
        name = row["propertyType"]
        assert row["valueType"] == expected[name], \
            f"Type mismatch for '{name}': expected {expected[name]}, got {row['valueType']}"

    print("* load_jsonl_property_types: PASSED")


if __name__ == "__main__":
    main()
