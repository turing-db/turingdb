"""
Regression test for /get_nodes crash with invalid node ID.

This test demonstrates that requesting a non-existent node ID via the /get_nodes
endpoint causes the server to crash with a segmentation fault.

The crash occurs because:
1. GraphReader::getNodeView() returns an invalid NodeView for non-existent nodes
2. The invalid NodeView has a null _labelset pointer (default-constructed LabelSetHandle)
3. PayloadWriter::write(const NodeView&) calls labelset().decompose()
4. decompose() calls hasLabel() which dereferences the null _labelset pointer

Expected: Once fixed, the server should return an error or skip invalid nodes.
"""

import sys
import time
import httpx
from turingdb import TuringDB

HOST = "http://localhost:6666"

def main():
    print("=== get_nodes_invalid_id regression test ===")
    print()

    # Setup: Create a graph with one node
    client = TuringDB(host=HOST)

    print("1. Creating test graph with one node...")
    client.query("CREATE GRAPH testgraph")
    client.set_graph("testgraph")
    change = client.query("CHANGE NEW")["changeID"][0]
    client.checkout(change=str(change))
    client.query("CREATE (:Person {name: 'Alice'})")
    client.query("COMMIT")
    client.query("CHANGE SUBMIT")
    print("   Graph created with node ID 0")
    print()

    # Test: Request a non-existent node ID via /get_nodes
    print("2. Requesting non-existent node ID 999999 via /get_nodes...")

    headers = {
        "Content-Type": "application/json",
        "X-TuringDB-Graph": "testgraph",
    }
    payload = {
        "nodeIDs": [999999]
    }

    try:
        response = httpx.post(f"{HOST}/get_nodes", json=payload, headers=headers, timeout=5)
        print(f"   Response status: {response.status_code}")
        print(f"   Response body: {response.text[:200]}")
        print()
    except httpx.RemoteProtocolError as e:
        print(f"   ERROR: Response ended prematurely - server crashed mid-response")
        print(f"   Exception: {e}")
        print()
        print("TEST FAILED: Server crashed when requesting invalid node ID")
        return 1
    except httpx.ConnectError as e:
        print(f"   ERROR: Connection failed - server likely crashed")
        print(f"   Exception: {e}")
        print()
        print("TEST FAILED: Server crashed when requesting invalid node ID")
        return 1
    except httpx.TimeoutException:
        print("   ERROR: Request timed out - server may have crashed or hung")
        print()
        print("TEST FAILED: Server unresponsive when requesting invalid node ID")
        return 1

    # Verify server is still alive
    print("3. Verifying server is still responding...")
    try:
        check_response = httpx.get(f"{HOST}/status", timeout=5)
        print(f"   Server status: {check_response.status_code}")
    except Exception as e:
        print(f"   ERROR: Server not responding after /get_nodes request")
        print(f"   Exception: {e}")
        print()
        print("TEST FAILED: Server died after /get_nodes request")
        return 1

    print()
    print("TEST PASSED: Server handled invalid node ID without crashing")
    return 0


if __name__ == "__main__":
    sys.exit(main())