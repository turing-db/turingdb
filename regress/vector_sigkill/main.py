from turingdb import TuringDB

import os
import shutil
import signal
import socket
import subprocess
import time

GREEN = "\033[0;32m"
BLUE = "\033[0;34m"
NC = "\033[0m"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TURING_DIR = ".turing"
LOCK_FILE = os.path.join(TURING_DIR, "turingdb.lock")
DATA_DIR = os.path.join(TURING_DIR, "data")

INDEX_TYPES = ["FLAT", "HNSW"]
RESULT_COUNT = 30
QUERY_VECTORS = [
    "(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)",
    "(1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)",
    "(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8)",
    "(-0.5, 0.5, -0.5, 0.5, -0.5, 0.5, -0.5, 0.5)",
    "(0.9, -0.9, 0.9, -0.9, 0.9, -0.9, 0.9, -0.9)",
]


def port_is_open():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("127.0.0.1", 6666)) == 0


def spawn_turingdb():
    print(f"- {GREEN}Starting turingdb{NC}")
    subprocess.check_call(f"exec turingdb -demon -turing-dir {TURING_DIR}", shell=True)

    for _ in range(100):
        if port_is_open():
            return
        time.sleep(0.1)

    raise RuntimeError("turingdb did not start listening on port 6666")


def read_server_pid():
    with open(LOCK_FILE) as f:
        return int(f.readline().strip())


def sigkill_turingdb():
    pid = read_server_pid()
    print(f"- {GREEN}Sending SIGKILL to turingdb (PID {pid}){NC}")
    os.kill(pid, signal.SIGKILL)

    for _ in range(100):
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            break
        time.sleep(0.1)
    else:
        raise RuntimeError(f"turingdb (PID {pid}) survived SIGKILL")

    for _ in range(100):
        if not port_is_open():
            return
        time.sleep(0.1)

    raise RuntimeError("Port 6666 still open after SIGKILL")


def stop_turingdb():
    print(f"- {GREEN}Stopping turingdb{NC}")
    subprocess.call(f"exec turingdb stop -turing-dir {TURING_DIR}", shell=True)

    for _ in range(100):
        if not port_is_open():
            return
        time.sleep(0.1)


def wait_ready(client):
    t0 = time.time()
    while time.time() - t0 < 6:
        try:
            client.reconnect()
            client.try_reach(timeout=1)
            return
        except:
            time.sleep(1)

    raise RuntimeError("Failed to connect to turingdb")


def index_name(index_type):
    return f"sigkill_{index_type.lower()}"


def create_indexes(client):
    for index_type in INDEX_TYPES:
        print(f"- {BLUE}Creating {index_type} vector index{NC}")
        client.query(
            f"CREATE VECTOR INDEX {index_name(index_type)} "
            f"WITH DIMENSION 8 METRIC EUCLID TYPE {index_type}"
        )


def load_vectors(client, file_name):
    shutil.copy(os.path.join(SCRIPT_DIR, file_name), DATA_DIR)
    for index_type in INDEX_TYPES:
        print(f"- {BLUE}Loading {file_name} into {index_type} index{NC}")
        client.query(f'LOAD VECTOR FROM "{file_name}" IN {index_name(index_type)}')


def search_rows(client, index_type, query_vector):
    query = (
        f"VECTOR SEARCH IN {index_name(index_type)} FOR {RESULT_COUNT} {query_vector} "
        f"YIELD ids, score RETURN ids, score"
    )
    result = client.query(query)
    rows = list(zip(result["ids"], result["score"]))

    # Rows with equal scores may come back in either order
    return sorted(rows, key=lambda row: (row[1], row[0]))


def search_all(client, expected_row_count):
    results = {}
    for index_type in INDEX_TYPES:
        for query_vector in QUERY_VECTORS:
            rows = search_rows(client, index_type, query_vector)
            if len(rows) != expected_row_count:
                raise Exception(
                    f"{index_type} search for {query_vector}: expected {expected_row_count} rows, "
                    f"got {len(rows)}: {rows}"
                )
            results[(index_type, query_vector)] = rows

    return results


def check_survives_sigkill(client, phase, expected_row_count):
    print(f"- {BLUE}{phase}: searching before SIGKILL{NC}")
    before = search_all(client, expected_row_count)

    sigkill_turingdb()
    spawn_turingdb()
    wait_ready(client)

    print(f"- {BLUE}{phase}: searching after restart{NC}")
    after = search_all(client, expected_row_count)

    for (index_type, query_vector), rows_before in before.items():
        rows_after = after[(index_type, query_vector)]
        if rows_before != rows_after:
            raise Exception(
                f"{phase}: {index_type} search for {query_vector} differs after SIGKILL\n"
                f"  before: {rows_before}\n"
                f"  after:  {rows_after}"
            )

    print(f"  {phase}: {len(before)} searches match")


if __name__ == "__main__":
    try:
        if os.path.exists(TURING_DIR):
            shutil.rmtree(TURING_DIR)

        spawn_turingdb()

        client = TuringDB(host="http://localhost:6666")
        wait_ready(client)

        create_indexes(client)
        os.makedirs(DATA_DIR, exist_ok=True)

        load_vectors(client, "vectors.csv")
        check_survives_sigkill(client, "Initial load", 10)

        load_vectors(client, "vectors_more.csv")
        check_survives_sigkill(client, "Load after restart", 20)

        load_vectors(client, "vectors_replace.csv")
        check_survives_sigkill(client, "Load replacing existing IDs", 20)

        print(f"\n* {GREEN}vector_sigkill: PASSED{NC}")

    finally:
        if port_is_open():
            stop_turingdb()
