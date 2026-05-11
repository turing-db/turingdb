import math

import turingdb

GRAPH_NAME = "embeddingdb"


def cosine_similarity(x, r):
    dot = sum(a * b for a, b in zip(x, r))
    nx = math.sqrt(sum(a * a for a in x))
    nr = math.sqrt(sum(b * b for b in r))
    return dot / (nx * nr)


def euclidean_distance(x, r):
    return math.sqrt(sum((a - b) ** 2 for a, b in zip(x, r)))


def main():
    client = turingdb.TuringClient(host="http://localhost:6666")
    client.try_reach()

    print("Running Embedding Properties Test")

    # Create graph and change
    client.query(f"CREATE GRAPH {GRAPH_NAME}")
    client.set_graph(GRAPH_NAME)
    change = client.new_change()
    client.checkout(change=change)

    # Create nodes with embedding properties
    client.query('CREATE (n:Vec {name: "a", vec: (0.3, 0.7, 0.5)})')
    client.query('CREATE (n:Vec {name: "b", vec: (0.9, 0.2, 0.4)})')
    client.query('CREATE (n:Vec {name: "c", vec: (0.1, 0.8, 0.6)})')

    # Submit the change and checkout HEAD
    client.query("COMMIT")
    client.query("CHANGE SUBMIT")
    client.checkout()

    # --- Test 1: Read back embedding properties ---
    result = client.query("MATCH (n:Vec) RETURN n.name, n.vec")
    names = list(result["n.name"])
    vecs = list(result["n.vec"])

    expected_vecs = {
        "a": [0.3, 0.7, 0.5],
        "b": [0.9, 0.2, 0.4],
        "c": [0.1, 0.8, 0.6],
    }

    assert len(names) == 3, f"Expected 3 nodes, got {len(names)}"

    for i, name in enumerate(names):
        assert name in expected_vecs, f"Unexpected node name '{name}'"
        expected = expected_vecs[name]
        actual = vecs[i]
        assert len(actual) == len(expected), f"Dimension mismatch for {name}"
        for j in range(len(expected)):
            assert abs(actual[j] - expected[j]) < 1e-5, \
                f"Vec mismatch for {name}[{j}]: expected {expected[j]}, got {actual[j]}"

    print("  Passed: read back embedding properties")

    # --- Test 2: cosine_similarity function ---
    cos_ref = [0.4, 0.3, 0.8]
    result = client.query(
        "MATCH (n:Vec) RETURN n.name, cosine_similarity(n.vec, (0.4, 0.3, 0.8))"
    )
    cos_names = list(result["n.name"])
    cos_col = [c for c in result.columns if c != "n.name"][0]
    cos_scores = list(result[cos_col])

    assert len(cos_names) == 3, f"Expected 3 cosine results, got {len(cos_names)}"

    for i, name in enumerate(cos_names):
        expected_cos = cosine_similarity(expected_vecs[name], cos_ref)
        assert abs(cos_scores[i] - expected_cos) < 1e-5, \
            f"Cosine mismatch for {name}: expected {expected_cos:.6f}, got {cos_scores[i]:.6f}"

    print("  Passed: cosine_similarity function")

    # --- Test 3: euclidean_distance function ---
    euc_ref = [0.2, 0.5, 0.3]
    result = client.query(
        "MATCH (n:Vec) RETURN n.name, euclidean_distance(n.vec, (0.2, 0.5, 0.3))"
    )
    euc_names = list(result["n.name"])
    euc_col = [c for c in result.columns if c != "n.name"][0]
    euc_distances = list(result[euc_col])

    assert len(euc_names) == 3, f"Expected 3 euclidean results, got {len(euc_names)}"

    for i, name in enumerate(euc_names):
        expected_euc = euclidean_distance(expected_vecs[name], euc_ref)
        assert abs(euc_distances[i] - expected_euc) < 1e-5, \
            f"Euclidean mismatch for {name}: expected {expected_euc:.6f}, got {euc_distances[i]:.6f}"

    print("  Passed: euclidean_distance function")

    print("* embedding_properties: done")


if __name__ == "__main__":
    main()
