import csv
import os
import turingdb

GRAPH_NAME = "csvtest"


def main() -> None:
    client = turingdb.TuringDB(
        instance_id='', auth_token='', host='http://localhost:6666')
    print("Connected to DB")

    client.query(f"LOAD GRAPH {GRAPH_NAME}")
    client.set_graph(GRAPH_NAME)
    print(f"Set graph {GRAPH_NAME}")

    test_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(test_dir, "stations.csv")

    # Read the CSV file in Python for ground truth
    with open(csv_path, newline='') as f:
        reader = csv.reader(f)
        headers = next(reader)
        expected_rows = list(reader)

    num_fields = len(headers)
    num_rows = len(expected_rows)
    print(f"CSV has {num_rows} rows, {num_fields} fields: {headers}")

    # ---------------------------------------------------------------
    # Test 1: WITH HEADERS — property access for every column
    # ---------------------------------------------------------------
    print("\n=== Test 1: LOAD CSV WITH HEADERS ===")
    returns = ", ".join(
        f"row.{h} AS `{h}`" for h in headers)
    query = (
        f"LOAD CSV '{csv_path}' WITH HEADERS AS row "
        f"RETURN {returns}"
    )
    result = client.query(query)

    for col_idx, header in enumerate(headers):
        db_col = result[header].tolist()
        assert len(db_col) == num_rows, \
            f"Column '{header}': expected {num_rows} rows, got {len(db_col)}"
        for row_idx, db_val in enumerate(db_col):
            expected_val = expected_rows[row_idx][col_idx]
            assert db_val == expected_val, (
                f"Mismatch at row {row_idx}, column '{header}': "
                f"expected {expected_val!r}, got {db_val!r}"
            )

    print(f"  WITH HEADERS: all {num_rows} x {num_fields} cells match")

    # ---------------------------------------------------------------
    # Test 2: WITHOUT HEADERS — index access for every column
    # ---------------------------------------------------------------
    print("\n=== Test 2: LOAD CSV without headers (index access) ===")
    returns = ", ".join(
        f"row[{i}] AS c{i}" for i in range(num_fields))
    query = (
        f"LOAD CSV '{csv_path}' AS row "
        f"RETURN {returns}"
    )
    result = client.query(query)

    # Without headers the first data row is the header line itself
    all_rows = [headers] + expected_rows
    for col_idx in range(num_fields):
        col_name = f"c{col_idx}"
        db_col = result[col_name].tolist()
        assert len(db_col) == len(all_rows), \
            f"Column c{col_idx}: expected {len(all_rows)} rows, got {len(db_col)}"
        for row_idx, db_val in enumerate(db_col):
            expected_val = all_rows[row_idx][col_idx]
            assert db_val == expected_val, (
                f"Mismatch at row {row_idx}, column c{col_idx}: "
                f"expected {expected_val!r}, got {db_val!r}"
            )

    print(f"  Without headers: all {len(all_rows)} x {num_fields} cells match")

    print("\n* load_csv_stations: all assertions passed")


if __name__ == "__main__":
    main()
