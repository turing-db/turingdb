import subprocess
import time

import turingdb


GRAPH_NAME = "reload_submit_edge"
HOST = "http://localhost:6666"
TURING_DIR = ".turing"


def wait_until_reachable(client: turingdb.TuringDB, timeout_s: float = 10.0) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            client.try_reach()
            return
        except Exception:
            time.sleep(0.1)

    raise RuntimeError("Timed out waiting for turingdb to become reachable")


def restart_db() -> None:
    assert subprocess.call(f"turingdb stop -turing-dir {TURING_DIR}", shell=True) == 0
    assert subprocess.call(f"turingdb -demon -turing-dir {TURING_DIR}", shell=True) == 0


def load_graph(client: turingdb.TuringDB) -> None:
    client.load_graph(GRAPH_NAME)
    client.set_graph(GRAPH_NAME)


def main() -> None:
    client = turingdb.TuringDB(instance_id="", auth_token="", host=HOST)
    wait_until_reachable(client)

    print("1. Create graph")
    client.query(f"CREATE GRAPH {GRAPH_NAME}")
    client.set_graph(GRAPH_NAME)

    print("2. Add 2 nodes and submit")
    change = client.new_change()
    client.checkout(change=change)
    client.query("CREATE (:Person {id: 1, name: 'Alice'})")
    client.query("CREATE (:Person {id: 2, name: 'Bob'})")
    client.query("CHANGE SUBMIT")
    client.checkout()

    print("3. Submit and reload graph")
    restart_db()
    wait_until_reachable(client)
    load_graph(client)

    print("4-6. Create new change, add edge, submit and reload")
    change = client.new_change()
    client.checkout(change=change)
    client.query(
        "MATCH (a:Person), (b:Person) "
        "WHERE a.id = 1 AND b.id = 2 "
        "CREATE (a)-[:KNOWS]->(b)"
    )
    client.query("CHANGE SUBMIT")
    client.checkout()

    restart_db()
    wait_until_reachable(client)
    load_graph(client)

    print("7. Verify nodes and edges exist")
    nodes = client.query("MATCH (n:Person) RETURN COUNT(n)")
    edges = client.query("MATCH (:Person)-[e:KNOWS]->(:Person) RETURN COUNT(e)")

    node_count = nodes["COUNT(n)"][0]
    edge_count = edges["COUNT(e)"][0]

    assert node_count == 2, f"Expected 2 nodes after reload, got {node_count}"
    assert edge_count == 1, f"Expected 1 edge after reload, got {edge_count}"

    print("* reload_submit_edge: done")


if __name__ == "__main__":
    main()
