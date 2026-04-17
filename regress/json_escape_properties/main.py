"""
Regression test for GitHub issue #592:
JSON API responses contain invalid escape sequences in string property values.

TuringDB returns \\' (backslash-apostrophe) in JSON strings, which is not a valid
JSON escape sequence. It also returns unescaped double quotes inside string values.

This causes JSON.parse() to fail in JavaScript clients (e.g. the visualizer).

Valid JSON escapes are: \\" \\\\ \\/ \\b \\f \\n \\r \\t \\uXXXX
"""

import json
import subprocess
import sys
import httpx
from turingdb import TuringDB

HOST = "http://localhost:6666"
GRAPH = "json_escape_test"


def main():
    print("=== json_escape_properties regression test ===")
    print()

    client = TuringDB(host=HOST)

    # 1. Create a graph with nodes whose properties contain special characters
    print("1. Creating test graph with problematic string properties...")
    client.query(f"CREATE GRAPH {GRAPH}")
    client.set_graph(GRAPH)

    change = client.new_change()
    client.checkout(change=change)

    # Single quote (triggers \' in current output)
    client.query("CREATE (:Company {name: \"Lloyd's of London\"})")

    # Double quote inside value (triggers unescaped " in current output)
    client.query("CREATE (:Equipment {description: '40ft container (9\\'6\")', code: 'HC96'})")

    # Backslash itself
    client.query("CREATE (:Path {value: 'C:\\\\Users\\\\test'})")

    client.query("COMMIT")
    client.query("CHANGE SUBMIT")
    client.checkout()
    print("   Created 3 nodes with special characters in properties")
    print()

    # 2. Fetch nodes via /get_nodes and validate the JSON is parseable
    print("2. Fetching nodes via /get_nodes HTTP endpoint...")
    node_ids = client.query(f"MATCH (n) RETURN n")["n"].tolist()
    print(f"   Node IDs: {node_ids}")

    response = httpx.post(
        f"{HOST}/get_nodes?graph={GRAPH}",
        json={"nodeIDs": node_ids},
        timeout=10,
    )

    assert response.status_code == 200, f"Expected 200, got {response.status_code}"

    raw_text = response.text
    print(f"   Response length: {len(raw_text)} bytes")
    print()

    # 3. Validate that the response is valid JSON (Python)
    print("3. Validating JSON is parseable by Python json.loads()...")
    try:
        parsed = json.loads(raw_text)
        print("   OK: Python json.loads() succeeded")
    except json.JSONDecodeError as e:
        print(f"   FAILED: Invalid JSON at position {e.pos}")
        print(f"   Error: {e.msg}")
        start = max(0, e.pos - 40)
        end = min(len(raw_text), e.pos + 40)
        print(f"   Context: ...{repr(raw_text[start:end])}...")
        print()
        print("TEST FAILED: /get_nodes returned invalid JSON (Python)")
        return 1
    print()

    # 4. Validate that the response is valid JSON (JavaScript / Node.js)
    #    This is the actual client that fails — the visualizer uses JSON.parse()
    print("4. Validating JSON is parseable by JavaScript JSON.parse()...")
    js_result = subprocess.run(
        ["node", "-e", "JSON.parse(require('fs').readFileSync('/dev/stdin','utf8'))"],
        input=raw_text.encode(),
        capture_output=True,
    )
    if js_result.returncode != 0:
        stderr = js_result.stderr.decode().strip()
        print(f"   FAILED: JavaScript JSON.parse() threw:")
        print(f"   {stderr}")
        print()
        print("TEST FAILED: /get_nodes returned JSON that JavaScript cannot parse")
        return 1
    print("   OK: JavaScript JSON.parse() succeeded")
    print()

    # 5. Validate the actual property values are correct
    print("5. Validating property values...")
    nodes = parsed["data"]

    found_apostrophe = False
    found_quote = False
    found_backslash = False

    for node_id, node in nodes.items():
        props = node.get("properties", {})
        for key, value in props.items():
            if isinstance(value, str):
                if "Lloyd" in value:
                    assert value == "Lloyd's of London", f"Expected \"Lloyd's of London\", got {repr(value)}"
                    found_apostrophe = True
                    print(f"   Node {node_id}.{key} = {repr(value)}  (apostrophe)")

                if "container" in value:
                    assert '9\'6"' in value, f"Expected value containing 9'6\", got {repr(value)}"
                    found_quote = True
                    print(f"   Node {node_id}.{key} = {repr(value)}  (double quote)")

                if "Users" in value:
                    assert "\\" in value, f"Expected backslash in path, got {repr(value)}"
                    found_backslash = True
                    print(f"   Node {node_id}.{key} = {repr(value)}  (backslash)")

    assert found_apostrophe, "Did not find node with apostrophe property"
    assert found_quote, "Did not find node with double-quote property"
    assert found_backslash, "Did not find node with backslash property"

    print()
    print("TEST PASSED: All JSON responses are valid and property values are correct")
    return 0


if __name__ == "__main__":
    sys.exit(main())
