"""
Regression test: requesting a non-existent node ID must not crash the server.
"""

import sys

from turingdb import TuringDB
from turingdb.exceptions import TuringDBException

HOST = "http://localhost:6666"


def main():
    print("=== get_nodes_invalid_id regression test ===")
    print()

    client = TuringDB(host=HOST)

    print("1. Creating test graph with one node...")
    client.query("CREATE GRAPH testgraph")
    client.set_graph("testgraph")
    change = client.new_change()
    client.checkout(change=change)
    client.query("CREATE (:Person {name: 'Alice'})")
    client.query("COMMIT")
    client.query("CHANGE SUBMIT")
    client.checkout()
    print("   Graph created with node ID 0")
    print()

    print("2. Requesting non-existent node ID 999999 via db.getNodes...")
    # The invariant under test is "no crash". A graceful error OR an empty
    # result are both acceptable; a server crash would raise a non-TuringDB
    # (transport) exception here, failing the test.
    try:
        res = client.query(
            "CALL db.getNodes([999999]) YIELD id, labels, properties RETURN id"
        )
        assert len(res) == 0, f"Expected an error or 0 rows for an unknown id, got {len(res)}"
        print("   OK: unknown id returned 0 rows (no crash)")
    except TuringDBException as e:
        print(f"   OK: unknown id rejected cleanly (no crash): {e}")
    print()

    print("3. Verifying server is still responding...")
    client.query("LIST GRAPH")
    print("   OK: server still responding")
    print()

    print("TEST PASSED: unknown node ID handled without crashing")
    return 0


if __name__ == "__main__":
    sys.exit(main())
