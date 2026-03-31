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


def test_vector_search_match(client: turingdb.TuringDB, data_dir: str) -> None:
    """Test that VECTOR SEARCH + MATCH hash join works (issue #550)."""

    # Create a vector index
    client.query("CREATE VECTOR INDEX reactome_index WITH DIMENSION 4 METRIC EUCLID")
    print("Created vector index")

    # Get some real dbIds to use as vector IDs
    result = client.query(
        "MATCH (n) WHERE n.dbId IS NOT NULL RETURN n.dbId LIMIT 5"
    )
    db_ids = [int(x) for x in result["n.dbId"]]
    assert len(db_ids) == 5, f"Expected 5 dbIds, got {len(db_ids)}"
    print(f"Using dbIds: {db_ids}")

    # Write a CSV file with test vectors keyed by real dbIds
    vectors_file = "reactome_vectors.csv"
    vectors_path = os.path.join(data_dir, vectors_file)
    embeddings = [
        [1.0, 0.0, 0.0, 0.0],
        [0.9, 0.1, 0.0, 0.0],
        [0.0, 1.0, 0.0, 0.0],
        [0.0, 0.0, 1.0, 0.0],
        [0.0, 0.0, 0.0, 1.0],
    ]
    with open(vectors_path, "w") as f:
        for db_id, emb in zip(db_ids, embeddings):
            f.write(f"{db_id},{','.join(str(v) for v in emb)}\n")

    # Load vectors
    result = client.query(f'LOAD VECTOR FROM "{vectors_file}" IN reactome_index')
    print(f"Loaded vectors: {result}")

    # This query previously threw "Incompatible right key type for string hash join"
    # because ids was UInt64 and n.dbId was Int64
    result = client.query(
        "VECTOR SEARCH IN reactome_index FOR 3 [1.0, 0.0, 0.0, 0.0] "
        "YIELD ids MATCH (n) WHERE n.dbId = ids "
        "RETURN n.dbId, n.displayName"
    )
    matched_ids = result["n.dbId"]
    print(f"Vector search + MATCH returned {len(matched_ids)} results: {matched_ids}")
    assert len(matched_ids) > 0, "Expected at least one result from vector search + MATCH"

    # Clean up
    client.query("DELETE VECTOR INDEX reactome_index")
    print("Vector search + MATCH hash join test passed (issue #550)")


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

    # Test vector search + MATCH hash join (issue #550)
    test_vector_search_match(client, data_dir)

    print(f"* load_jsonl_reactome: PASSED (nodes={node_count}, edges={edge_count})")


if __name__ == "__main__":
    main()
