import os
import shutil
import turingdb

BUCKET = "turingdb-external"
S3_KEY = "bench-datasets/reactome.jsonl"
GRAPH_NAME = "reactome"
TURING_DIR = ".turing"
LOCAL_FILENAME = "reactome.jsonl"

# Resolve repo root from TURING_HOME (set by run_regress.sh) or fall back
REPO_ROOT = os.environ.get("SOURCE_DIR", os.path.join(os.environ.get("TURING_HOME", ""), ".."))
DATASET_CACHE = os.path.join(REPO_ROOT, "external", "datasets", LOCAL_FILENAME)


def download_from_s3(dest_path: str) -> None:
    import boto3

    print(f"Downloading s3://{BUCKET}/{S3_KEY} -> {dest_path}")
    s3 = boto3.client("s3")
    s3.download_file(BUCKET, S3_KEY, dest_path)
    print("Download complete")


def ensure_reactome(dest_path: str) -> None:
    if os.path.isfile(DATASET_CACHE):
        print(f"Copying cached {DATASET_CACHE} -> {dest_path}")
        shutil.copy2(DATASET_CACHE, dest_path)
    else:
        download_from_s3(DATASET_CACHE)
        print(f"Copying {DATASET_CACHE} -> {dest_path}")
        shutil.copy2(DATASET_CACHE, dest_path)


def main() -> None:
    # Place reactome.jsonl into the turingdb data directory
    data_dir = os.path.join(TURING_DIR, "data")
    os.makedirs(data_dir, exist_ok=True)
    dest_path = os.path.join(data_dir, LOCAL_FILENAME)
    ensure_reactome(dest_path)

    # Connect to the running turingdb instance
    client = turingdb.TuringDB(
        instance_id="", auth_token="", host="http://localhost:6666"
    )
    client.try_reach()
    print("Connected to TuringDB")

    # Load the JSONL file as a graph
    result = client.query(f'LOAD JSONL "{LOCAL_FILENAME}" AS {GRAPH_NAME}')
    print(f"LOAD JSONL result: {result}")

    client.set_graph(GRAPH_NAME)

    # Count all nodes
    result = client.query("MATCH (n) RETURN COUNT(n) AS count")
    node_count = int(result["count"][0])
    print(f"Total node count: {node_count}")
    assert node_count > 0, "Expected at least some nodes"

    # Count all edges
    result = client.query("MATCH ()-[r]->() RETURN COUNT(r) AS count")
    edge_count = int(result["count"][0])
    print(f"Total edge count: {edge_count}")
    assert edge_count > 0, "Expected at least some edges"

    print(f"* load_jsonl_reactome: PASSED (nodes={node_count}, edges={edge_count})")


if __name__ == "__main__":
    main()
