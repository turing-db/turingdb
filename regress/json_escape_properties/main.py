"""
Regression test for GitHub issue #592:
JSON API responses contain invalid escape sequences in string property values.

TuringDB used to return \\' (backslash-apostrophe) in JSON strings, which is not
a valid JSON escape sequence, plus unescaped double quotes inside string values.
This caused JSON.parse() to fail in JavaScript clients (e.g. the visualizer).

Valid JSON escapes are: \\" \\\\ \\/ \\b \\f \\n \\r \\t \\uXXXX
"""

import json
import os
import subprocess
import sys

from turingdb import TuringDB

HOST = "http://localhost:6666"
GRAPH = "json_escape_test"


def main():
    print("=== json_escape_properties regression test ===")
    print()

    # The properties blob is emitted identically over both wire protocols, but
    # this test is about the JSON string the JSON-speaking clients consume, so
    # keep it scoped to type='json' as before.
    if os.environ.get("TURINGDB_TYPE") == "native":
        print("Skipping under type='native' (json-specific test)")
        sys.exit(0)

    client = TuringDB(host=HOST)

    # 1. Create a graph with nodes whose properties contain special characters
    print("1. Creating test graph with problematic string properties...")
    client.query(f"CREATE GRAPH {GRAPH}")
    client.set_graph(GRAPH)

    change = client.new_change()
    client.checkout(change=change)

    # Single quote (used to trigger \' in the old output)
    client.query("CREATE (:Company {name: \"Lloyd's of London\"})")

    # Double quote inside value (used to trigger an unescaped " in old output)
    client.query("CREATE (:Equipment {description: '40ft container (9\\'6\")', code: 'HC96'})")

    # Backslash itself
    client.query("CREATE (:Path {value: 'C:\\\\Users\\\\test'})")

    client.query("COMMIT")
    client.query("CHANGE SUBMIT")
    client.checkout()
    print("   Created 3 nodes with special characters in properties")
    print()

    # 2. Fetch nodes via the db.getNodes CALL procedure. properties comes back
    #    as a JSON string per node.
    print("2. Fetching nodes via CALL db.getNodes ...")
    node_ids = client.query("MATCH (n) RETURN n")["n"].tolist()
    print(f"   Node IDs: {node_ids}")

    result = client.query(
        f"CALL db.getNodes({node_ids}) YIELD id, labels, properties RETURN id, properties"
    )
    prop_blobs = result["properties"].tolist()
    print(f"   Fetched {len(prop_blobs)} property blobs")
    print()

    # 3. Every properties blob must parse with Python json.loads()
    print("3. Validating each properties blob parses with Python json.loads()...")
    parsed_props = []
    for blob in prop_blobs:
        try:
            parsed_props.append(json.loads(blob))
        except json.JSONDecodeError as e:
            print(f"   FAILED: invalid JSON at position {e.pos}: {e.msg}")
            start = max(0, e.pos - 40)
            end = min(len(blob), e.pos + 40)
            print(f"   Context: ...{repr(blob[start:end])}...")
            print()
            print("TEST FAILED: db.getNodes returned invalid property JSON (Python)")
            return 1
    print("   OK: Python json.loads() succeeded for all")
    print()

    # 4. Every properties blob must also parse with JavaScript JSON.parse() —
    #    this is the client (the visualizer) that failed under issue #592.
    print("4. Validating each properties blob parses with JavaScript JSON.parse()...")
    for blob in prop_blobs:
        js_result = subprocess.run(
            ["node", "-e", "JSON.parse(require('fs').readFileSync('/dev/stdin','utf8'))"],
            input=blob.encode(),
            capture_output=True,
        )
        if js_result.returncode != 0:
            stderr = js_result.stderr.decode().strip()
            print(f"   FAILED: JavaScript JSON.parse() threw:")
            print(f"   {stderr}")
            print(f"   Blob: {repr(blob)}")
            print()
            print("TEST FAILED: db.getNodes returned property JSON that JavaScript cannot parse")
            return 1
    print("   OK: JavaScript JSON.parse() succeeded for all")
    print()

    # 5. Validate the actual property values round-tripped correctly
    print("5. Validating property values...")
    found_apostrophe = False
    found_quote = False
    found_backslash = False

    for props in parsed_props:
        for key, value in props.items():
            if isinstance(value, str):
                if "Lloyd" in value:
                    assert value == "Lloyd's of London", f"Expected \"Lloyd's of London\", got {repr(value)}"
                    found_apostrophe = True
                    print(f"   {key} = {repr(value)}  (apostrophe)")

                if "container" in value:
                    assert '9\'6"' in value, f"Expected value containing 9'6\", got {repr(value)}"
                    found_quote = True
                    print(f"   {key} = {repr(value)}  (double quote)")

                if "Users" in value:
                    assert "\\" in value, f"Expected backslash in path, got {repr(value)}"
                    found_backslash = True
                    print(f"   {key} = {repr(value)}  (backslash)")

    assert found_apostrophe, "Did not find node with apostrophe property"
    assert found_quote, "Did not find node with double-quote property"
    assert found_backslash, "Did not find node with backslash property"

    print()
    print("TEST PASSED: All property JSON is valid and property values are correct")
    return 0


if __name__ == "__main__":
    sys.exit(main())
