import turingdb
from turingdb.exceptions import TuringDBException

GRAPH_NAME = "typecoverage"
JSONL_FILENAME = "type_coverage.jsonl"


def test_property_types(client: turingdb.TuringDB) -> None:
    result = client.query(f'LOAD JSONL "{JSONL_FILENAME}" AS {GRAPH_NAME} WITH EMBEDDINGS [{{"embProp", 3}}, {{"mixedNumEmbProp", 3}}]')
    print(f"LOAD JSONL result: {result}")

    client.set_graph(GRAPH_NAME)

    result = client.query("CALL db.propertyTypes()")
    print(f"Property types:\n{result}")

    result = result.sort_values("propertyType").reset_index(drop=True)

    expected = {
        "arrProp": "String",    # homogeneous array, turned to string
        "boolProp": "Bool",
        "embProp": "Embedding", # integer array, specified as embedding, parsed as embedding
        "floatProp": "Double",
        "intArrProp": "String", # integer array, not specified as embedding, turned to string
        "intProp": "Int64",
        "mixedArrProp": "String", # hetereogeneous array, turned to string
        "mixedNumEmbProp": "Embedding", # mixed float/int array, specified as embedding
        "strProp": "String",
    }

    actual_names = list(result["propertyType"])
    assert actual_names == sorted(expected.keys()), \
        f"Property names mismatch: {actual_names}"

    for _, row in result.iterrows():
        name = row["propertyType"]
        assert row["valueType"] == expected[name], \
            f"Type mismatch for '{name}': expected {expected[name]}, got {row['valueType']}"

    print("* test_property_types: PASSED")


def test_non_numeric_property_as_embedding(client: turingdb.TuringDB) -> None:
    # arrProp contains ["foo","bar"] — non-numeric strings declared as embedding
    try:
        client.query(f'LOAD JSONL "{JSONL_FILENAME}" AS typecoverage_nonemb WITH EMBEDDINGS [{{"arrProp", 2}}]')
        assert False, "Expected load to fail for non-numeric property declared as embedding"
    except TuringDBException as e:
        print(f"Expected error, got error: {e}")
        pass

    print("* test_non_numeric_property_as_embedding: PASSED")


def test_wrong_embedding_dimension(client: turingdb.TuringDB) -> None:
    # embProp has dimension 3, but declared here as 4
    try:
        client.query(f'LOAD JSONL "{JSONL_FILENAME}" AS typecoverage_wrongdim WITH EMBEDDINGS [{{"embProp", 4}}]')
        assert False, "Expected load to fail for mismatched embedding dimension"
    except TuringDBException as e:
        print(f"Expected error, got error: {e}")
        pass

    print("* test_wrong_embedding_dimension: PASSED")


def main() -> None:
    client = turingdb.TuringDB(host="http://localhost:6666")
    client.try_reach()
    print("Connected to TuringDB")

    test_property_types(client)
    test_non_numeric_property_as_embedding(client)
    test_wrong_embedding_dimension(client)

    print("* load_jsonl_property_types: ALL PASSED")


if __name__ == "__main__":
    main()
