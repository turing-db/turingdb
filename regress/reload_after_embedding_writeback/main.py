"""Regression for GRAPH_LOAD_ERROR after SET <embedding> writeback (issue #618).

Mirrors the failing CI flow in turingdb-dgl-example:

  1. LOAD JSONL reactome (~2.97M nodes, multi-label).
  2. Spawn N worker processes; each opens its own change, writes a
     stride-partitioned slice with ``SET nX.embedding = (...)``, then
     ``COMMIT`` + ``CHANGE SUBMIT`` + ``checkout("main")``.
  3. ``turingdb stop`` (server emits one ``Dumping graph reactome`` per change).
  4. ``turingdb start`` again, ``LOAD GRAPH reactome`` — must not raise.

Pre-bug-fix this trips ``TuringDBException: GRAPH_LOAD_ERROR`` on the second
``LOAD GRAPH`` (only seen so far on the hetz-bench CI runner — see issue #618
for the reproducibility status).
"""
import multiprocessing as mp
import os
import shutil
import socket
import subprocess
import sys
import time

import turingdb

BUCKET = "turingdb-public"
S3_KEY = "data/reactome.jsonl"
LOCAL_JSONL = "reactome.jsonl"
TURING_DIR = ".turing"
PORT = 6666
HOST = f"http://localhost:{PORT}"
GRAPH = "reactome"

EMBED_DIM = 128
BATCH_SIZE = 500
NUM_WORKERS = 4

REPO_ROOT = os.environ.get(
    "SOURCE_DIR", os.path.join(os.environ.get("TURING_HOME", ""), "..")
)
DATASET_CACHE = os.path.join(
    REPO_ROOT, "external", "datasets", LOCAL_JSONL
)


def listening_port(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("127.0.0.1", port)) == 0


def start_db():
    cmd = f"turingdb -demon -turing-dir {TURING_DIR}"
    print(f"$ {cmd}")
    assert subprocess.call(cmd, shell=True) == 0
    for _ in range(120):
        if listening_port(PORT):
            return
        time.sleep(0.2)
    raise RuntimeError("turingdb did not become ready")


def stop_db():
    cmd = f"turingdb stop -turing-dir {TURING_DIR}"
    print(f"$ {cmd}")
    subprocess.call(cmd, shell=True)
    for _ in range(50):
        if not listening_port(PORT):
            return
        time.sleep(0.2)
    raise RuntimeError("turingdb did not shut down")


def ensure_reactome(dest: str) -> None:
    if os.path.isfile(DATASET_CACHE):
        print(f"Copying cached {DATASET_CACHE} -> {dest}")
        shutil.copy2(DATASET_CACHE, dest)
        return
    import boto3
    print(f"Downloading s3://{BUCKET}/{S3_KEY} -> {DATASET_CACHE}")
    os.makedirs(os.path.dirname(DATASET_CACHE), exist_ok=True)
    boto3.client("s3").download_file(BUCKET, S3_KEY, DATASET_CACHE)
    shutil.copy2(DATASET_CACHE, dest)


def worker(rank: int, num_workers: int, num_nodes: int) -> None:
    c = turingdb.TuringDB(host=HOST)
    c.set_graph(GRAPH)
    c.new_change()

    ids = list(range(rank, num_nodes, num_workers))
    n = len(ids)
    print(f"  worker {rank}: {n:,} nodes")

    for s in range(0, n, BATCH_SIZE):
        e = min(s + BATCH_SIZE, n)
        k = e - s
        match = "MATCH " + ", ".join(f"(n{j})" for j in range(k))
        where = "WHERE " + " AND ".join(
            f"n{j} = {ids[s + j]}" for j in range(k)
        )
        sets = "SET " + ", ".join(
            f"n{j}.embedding = ("
            + ", ".join(
                f"{ids[s + j] * 0.001 + d * 0.01:.6f}"
                for d in range(EMBED_DIM)
            )
            + ")"
            for j in range(k)
        )
        c.query(f"{match} {where} {sets}")

    c.query("COMMIT")
    c.query("CHANGE SUBMIT")
    c.checkout()
    print(f"  worker {rank} done")


def main() -> None:
    data_dir = os.path.join(TURING_DIR, "data")
    os.makedirs(data_dir, exist_ok=True)
    ensure_reactome(os.path.join(data_dir, LOCAL_JSONL))

    start_db()

    c = turingdb.TuringDB(host=HOST)
    print(c.query(f'LOAD JSONL "{LOCAL_JSONL}" AS {GRAPH}'))
    c.set_graph(GRAPH)
    num_nodes = int(c.query("MATCH (n) RETURN count(n) AS num").iloc[0, 0])
    print(f"num_nodes = {num_nodes:,}")
    del c

    print("--- writeback ---")
    procs = []
    for rank in range(NUM_WORKERS):
        p = mp.Process(target=worker, args=(rank, NUM_WORKERS, num_nodes))
        p.start()
        procs.append(p)
    for p in procs:
        p.join()
        if p.exitcode != 0:
            raise RuntimeError(f"worker exited {p.exitcode}")

    print("--- stop + start ---")
    stop_db()
    start_db()

    c2 = turingdb.TuringDB(host=HOST)
    print(f"--- loading {GRAPH} ---")
    try:
        c2.load_graph(GRAPH)
    except Exception as e:
        raise AssertionError(
            f"reload after embedding writeback failed: {e!r} "
            "(regression of issue #618)"
        )
    c2.set_graph(GRAPH)
    n2 = int(c2.query("MATCH (n) RETURN count(n) AS num").iloc[0, 0])
    assert n2 == num_nodes, f"node count after reload: {n2} != {num_nodes}"
    print(f"reload OK ({n2:,} nodes)")

    stop_db()


if __name__ == "__main__":
    main()
