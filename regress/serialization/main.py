from turingdb import TuringDB

import pandas as pd

import subprocess
import os
import shutil

GREEN = "\033[0;32m"
BLUE = "\033[0;34m"
NC = "\033[0m"


def spawn_turingdb():
    cmd = "turingdb -demon -turing-dir .turing"
    print(f"- {GREEN}Starting turingdb with `{BLUE}{cmd}{NC}`{NC}")
    return subprocess.call(cmd, shell=True) == 0


def stop_turingdb():
    cmd = "turingdb stop -turing-dir .turing"
    print(f"- {GREEN}Stopping turingdb with `{BLUE}{cmd}{NC}`{NC}")
    return subprocess.call(cmd, shell=True) == 0


if __name__ == "__main__":
    if os.path.exists(".turing"):
        shutil.rmtree(".turing")

    assert spawn_turingdb()

    # Connect to turingdb
    client = TuringDB(host="http://localhost:6666")

    print(f"- {BLUE}Creating graph{NC}")
    print(client.query("CREATE GRAPH mygraph"))

    client.set_graph("mygraph")

    # Make changes
    print(f"- {BLUE}Making changes{NC}")
    change = client.new_change()
    client.checkout(change=change)

    create_query = "CREATE (a:Person {name: 'Alice'}), (j:Person {name: 'John'}), (a)-[:KNOWS]->(j)"
    print(client.query(create_query))
    print(client.query("COMMIT"))

    create_query = "MATCH (j:Person {name: 'John'}) CREATE (b:Person {name: 'Bob'}), (j)-[:KNOWS]->(b)"
    print(client.query(create_query))
    print(client.query("COMMIT"))

    create_query = "CREATE (c:Person {name: 'Charlie'}), (m:Person {name: 'Mike'}), (c)-[:KNOWS]->(m)"
    print(client.query("CHANGE SUBMIT"))

    client.checkout()

    res: pd.DataFrame = client.query("MATCH (n) RETURN n, n.name")
    print(res)

    if res.shape[0] != 3:
        raise Exception(
            f"After reloading the graph, the query should return three rows. "
            f"Returned {res.shape[0]} instead. Query result:\n{res}"
        )

    # Restart turingdb
    print(f"- {BLUE}Restarting turingdb{NC}")
    assert stop_turingdb()

    assert spawn_turingdb()
    # The daemon is a fresh process; refresh the client's transport so the
    # next query opens a new TCP connection. reconnect() resets session
    # state (current graph etc.) — we re-establish it below.
    client.reconnect()

    # Test after reload
    client.load_graph("mygraph")
    client.set_graph("mygraph")
    res: pd.DataFrame = client.query("MATCH (n) RETURN n, n.name")

    if res.shape[0] != 3:
        raise Exception(
            f"After reloading the graph, the query should return three rows. "
            f"Returned {res.shape[0]} instead. Query result:\n{res}"
        )
    assert stop_turingdb()
