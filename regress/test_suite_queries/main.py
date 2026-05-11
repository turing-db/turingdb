import json
import os
import sys

import pandas as pd
from turingdb import TuringClient, TuringDBException

GRAPH_NAME = "simpledb"

DTYPE_MAP = {
    "String": "string",
    "Int64": "Int64",
    "UInt64": "UInt64",
    "Double": "float64",
    "Bool": "boolean",
}


def parse_result_json(json_str: str) -> pd.DataFrame:
    """Parse a resultJson string into a DataFrame using the same logic as TuringClient._parse_chunks."""
    data = json.loads(json_str)

    header = data["header"]
    column_names = header["column_names"]
    column_types = header["column_types"]

    # Seed with empty typed columns from the header so that zero-row results
    # still carry their schema — matches the HTTP and binary client behavior.
    df = pd.DataFrame(
        {
            cname: pd.Series([], dtype=DTYPE_MAP.get(ctype, "object"))
            for cname, ctype in zip(column_names, column_types)
        }
    )

    for chunk in data["data"]:
        df_chunk = pd.DataFrame(
            {
                cname: pd.Series(col, dtype=DTYPE_MAP.get(ctype, "object"))
                for (cname, ctype), col in zip(zip(column_names, column_types), chunk)
            }
        )
        df = pd.concat([df, df_chunk], ignore_index=True)

    return df


def load_test_files(tests_dir: str) -> list[dict]:
    """Load all test JSON files sorted by name."""
    tests = []
    for entry in sorted(os.listdir(tests_dir)):
        if not entry.endswith(".json"):
            continue
        path = os.path.join(tests_dir, entry)
        with open(path) as f:
            test = json.load(f)
        test["_name"] = entry.removesuffix(".json")
        tests.append(test)
    return tests


def main() -> None:
    src_dir = os.environ.get("TURING_SRC", "")
    if len(src_dir) == 0:
        print("ERROR: TURING_SRC not set")
        sys.exit(1)

    tests_dir = os.path.join(src_dir, "test", "query-test-suite", "tests")

    if not os.path.isdir(tests_dir):
        print(f"ERROR: Tests directory not found: {tests_dir}")
        sys.exit(1)

    client = TuringClient(host="http://localhost:6666")
    client.try_reach()
    print("Connected to DB")

    client.query(f"LOAD GRAPH {GRAPH_NAME}")
    client.set_graph(GRAPH_NAME)
    print(f"Loaded graph {GRAPH_NAME}")

    tests = load_test_files(tests_dir)
    print(f"Loaded {len(tests)} tests")

    transport = os.environ.get("TURINGDB_TRANSPORT", "http")

    passed = 0
    failed = 0
    skipped = 0

    skip_tags = {"value-hash-join"}

    for test in tests:
        name = test["_name"]

        if not test.get("enabled", True):
            skipped += 1
            continue

        if transport == "binary" and not test.get("remote-enabled", True):
            skipped += 1
            continue

        test_tags = set(test.get("tags", []))
        if test_tags & skip_tags:
            skipped += 1
            continue

        expected_json_str = test.get("expect", {}).get("resultJson", "")
        if not expected_json_str:
            print(f"  SKIP {name}: no resultJson")
            skipped += 1
            continue

        query = test.get("query", "")
        write_required = test.get("write-required", False)
        expected_json = json.loads(expected_json_str)
        is_error = "error" in expected_json

        try:
            client.checkout()

            if write_required:
                client.new_change()

            if is_error:
                expected_error = expected_json["error"]
                expected_details = expected_json.get("error_details", "")
                expected_msg = (
                    f"{expected_error}: {expected_details}"
                    if expected_details
                    else expected_error
                )

                try:
                    client.query(query)
                    print(f"  FAIL {name}: expected error but query succeeded")
                    failed += 1
                except TuringDBException as e:
                    if str(e) == expected_msg:
                        passed += 1
                    else:
                        print(f"  FAIL {name}: error mismatch")
                        print(f"    expected: {expected_msg}")
                        print(f"    actual:   {e}")
                        failed += 1
            else:
                actual_df = client.query(query)
                expected_df = parse_result_json(expected_json_str)

                if actual_df.equals(expected_df):
                    passed += 1
                else:
                    print(f"  FAIL {name}: DataFrame mismatch")
                    print(
                        f"    expected columns: {list(expected_df.columns)} types: {list(expected_df.dtypes)}"
                    )
                    print(
                        f"    actual columns:   {list(actual_df.columns)} types: {list(actual_df.dtypes)}"
                    )
                    print(f"    expected:\n{expected_df}")
                    print(f"    actual:\n{actual_df}")
                    failed += 1

            if write_required:
                client.checkout()

        except Exception as e:
            print(f"  FAIL {name}: {type(e).__name__}: {e}")
            failed += 1
            if write_required:
                try:
                    client.checkout()
                except Exception:
                    pass

    print(f"\nResults: {passed} passed, {failed} failed, {skipped} skipped")

    if failed > 0:
        sys.exit(1)

    print("* test_suite_queries: done")


if __name__ == "__main__":
    main()
