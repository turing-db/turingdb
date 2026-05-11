from turingdb import TuringClient

import subprocess
import os
import shutil
import time

GREEN = "\033[0;32m"
BLUE = "\033[0;34m"
NC = "\033[0m"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
VECTORS_FILE = os.path.join(SCRIPT_DIR, "vectors.csv")


def spawn_turingdb():
    print(f"- {GREEN}Starting turingdb{NC}")
    proc = subprocess.Popen("exec turingdb -demon -turing-dir .turing", shell=True)
    # Wait for the daemon to accept TCP connections on 6666 before returning,
    # so clients constructed immediately after don't race the startup.
    import socket
    for _ in range(100):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex(("127.0.0.1", 6666)) == 0:
                return proc
        time.sleep(0.1)
    raise RuntimeError("turingdb did not start listening on port 6666")


def stop_turingdb(proc):
    print(f"- {GREEN}Stopping turingdb{NC}")
    subprocess.call("exec turingdb stop -turing-dir .turing", shell=True)
    # Wait for port to be released
    import socket

    for _ in range(100):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex(("127.0.0.1", 6666)) != 0:
                break
        time.sleep(0.1)


def wait_ready(client):
    t0 = time.time()
    while time.time() - t0 < 6:
        try:
            # Drop any stale socket from a previous daemon and open fresh.
            # No-op on the HTTP transport.
            client.reconnect()
            client.try_reach(timeout=1)
            return
        except:
            time.sleep(1)

    raise RuntimeError("Failed to connect to turingdb")


def test_vector_search(client, query_vector, expected_first_id, description):
    """
    Perform a vector search and verify the nearest neighbor is as expected.
    """
    query = f"VECTOR SEARCH IN test_vectors FOR 3 {query_vector} YIELD ids RETURN ids"
    result = client.query(query)

    # Result should have 'ids' column
    ids = list(result["ids"])

    if len(ids) == 0:
        raise Exception(f"{description}: Expected results but got empty")

    if ids[0] != expected_first_id:
        raise Exception(
            f"{description}: Expected first result to be ID {expected_first_id}, "
            f"got {ids[0]}. Full results: {ids}"
        )

    print(f"  {description}: OK (nearest neighbor is ID {ids[0]})")
    return ids


def get_vector_indexes(client):
    """Get list of vector indexes with their metadata."""
    result = client.query("SHOW VECTOR INDEXES")
    return result


if __name__ == "__main__":
    proc = None
    try:
        if os.path.exists(".turing"):
            shutil.rmtree(".turing")

        proc = spawn_turingdb()

        # Connect to turingdb
        client = TuringClient(host="http://localhost:6666")
        wait_ready(client)

        # Create vector index
        print(f"- {BLUE}Creating vector index{NC}")
        client.query("CREATE VECTOR INDEX test_vectors WITH DIMENSION 8 METRIC EUCLID")

        # Verify index was created
        indexes = get_vector_indexes(client)
        print(f"  Vector indexes: {indexes}")

        names = list(indexes["name"])
        if "test_vectors" not in names:
            raise Exception("Vector index 'test_vectors' was not created")

        dimensions = list(indexes["dimension"])
        idx = names.index("test_vectors")
        if dimensions[idx] != 8:
            raise Exception(f"Expected dimension 8, got {dimensions[idx]}")

        # Copy vectors file into the data directory
        data_dir = os.path.join(".turing", "data")
        os.makedirs(data_dir, exist_ok=True)
        shutil.copy(VECTORS_FILE, data_dir)

        # Load vectors from file (path relative to .turing/data)
        print(f"- {BLUE}Loading vectors from file{NC}")
        client.query('LOAD VECTOR FROM "vectors.csv" IN test_vectors')
        print("  Vectors loaded successfully")

        # Test vector search before restart
        print(f"- {BLUE}Testing vector search before restart{NC}")

        # Query for vector closest to [0,0,0,0,0,0,0,0] - should be ID 0
        results_before_1 = test_vector_search(
            client, "(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)", 0, "Query for origin"
        )

        # Query for vector closest to [1,0,0,0,0,0,0,0] - should be ID 1
        results_before_2 = test_vector_search(
            client,
            "(1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)",
            1,
            "Query for [1,0,0,...]",
        )

        # Query for vector closest to [0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8] - should be ID 6
        results_before_3 = test_vector_search(
            client,
            "(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8)",
            6,
            "Query for [0.1,0.2,...]",
        )

        # Restart turingdb
        print(f"- {BLUE}Restarting turingdb{NC}")
        stop_turingdb(proc)

        proc = spawn_turingdb()
        wait_ready(client)

        # Verify index still exists after restart
        print(f"- {BLUE}Verifying vector index after restart{NC}")
        indexes = get_vector_indexes(client)
        print(f"  Vector indexes after restart: {indexes}")

        names = list(indexes["name"])
        if "test_vectors" not in names:
            raise Exception("Vector index 'test_vectors' not found after restart")

        dimensions = list(indexes["dimension"])
        idx = names.index("test_vectors")
        if dimensions[idx] != 8:
            raise Exception(
                f"Expected dimension 8 after restart, got {dimensions[idx]}"
            )

        print("  Index metadata persisted correctly")

        # Test vector search after restart - should get same results
        print(f"- {BLUE}Testing vector search after restart{NC}")

        results_after_1 = test_vector_search(
            client, "(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)", 0, "Query for origin"
        )

        results_after_2 = test_vector_search(
            client,
            "(1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)",
            1,
            "Query for [1,0,0,...]",
        )

        results_after_3 = test_vector_search(
            client,
            "(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8)",
            6,
            "Query for [0.1,0.2,...]",
        )

        # Verify results match
        print(f"- {BLUE}Verifying results match before/after restart{NC}")

        if results_before_1 != results_after_1:
            raise Exception(
                f"Results mismatch for query 1: before={results_before_1}, after={results_after_1}"
            )

        if results_before_2 != results_after_2:
            raise Exception(
                f"Results mismatch for query 2: before={results_before_2}, after={results_after_2}"
            )

        if results_before_3 != results_after_3:
            raise Exception(
                f"Results mismatch for query 3: before={results_before_3}, after={results_after_3}"
            )

        print(f"  {GREEN}All results match! Vector serialization works correctly.{NC}")
        print(f"\n* {GREEN}vector_serialization: PASSED{NC}")

    finally:
        if proc:
            stop_turingdb(proc)
