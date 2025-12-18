from turingdb import TuringDB

import pandas as pd

import subprocess
import signal
import os
import shutil
import time

GREEN = "\033[0;32m"
BLUE = "\033[0;34m"
NC = "\033[0m"


def spawn_turingdb():
    print(f"- {GREEN}Starting turingdb{NC}")
    return subprocess.Popen("exec turingdb -demon -turing-dir .turing", shell=True)


def stop_turingdb(proc):
    print(f"- {GREEN}Stopping turingdb{NC}")
    proc.send_signal(signal.SIGTERM)
    proc.wait()


def wait_ready(client):
    t0 = time.time()
    while time.time() - t0 < 6:
        try:
            client.try_reach(timeout=1)
            return
        except:
            time.sleep(0.1)


if __name__ == "__main__":
    proc = None
    try:
        if os.path.exists(".turing"):
            shutil.rmtree(".turing")

        proc = spawn_turingdb()

        # Connect to turingdb
        client = TuringDB(host="http://localhost:6666")
        wait_ready(client)

        print(f"- {BLUE}Creating graph{NC}")
        print(client.query("CREATE GRAPH mygraph"))

        client.set_graph("mygraph")

        # Make changes
        print(f"- {BLUE}Making changes{NC}")
        change = client.query("CHANGE NEW")[0][0]
        client.checkout(change=str(change))

        create_query = "CREATE (a:Person {name: 'Alice'}), (j:Person {name: 'John'}), (a)-[:KNOWS]-(j)"
        print(client.query(create_query))
        print(client.query("COMMIT"))

        create_query = "CREATE (b:Person {name: 'Bob'}), (j at 1)-[:KNOWS]-(b)"
        print(client.query(create_query))
        print(client.query("COMMIT"))

        create_query = "CREATE (c:Person {name: 'Charlie'}), (m:Person {name: 'Mike'}), (c)-[:KNOWS]-(m)"
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
        stop_turingdb(proc)

        proc = spawn_turingdb()
        wait_ready(client)

        # Test after reload
        client.load_graph("mygraph")
        res: pd.DataFrame = client.query("MATCH (n) RETURN n, n.name")

        if res.shape[0] != 3:
            raise Exception(
                f"After reloading the graph, the query should return three rows. "
                f"Returned {res.shape[0]} instead. Query result:\n{res}"
            )

    finally:
        if proc:
            stop_turingdb(proc)
