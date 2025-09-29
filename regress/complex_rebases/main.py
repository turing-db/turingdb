from turingdb import TuringDB
import time
import random

NUM_CHANGES : int = 10
MAX_NODES : int = 10
MAX_EDGES : int = MAX_NODES

def make_local_changes(client : TuringDB, num_nodes : int, num_edges : int) -> tuple[int, str]:
  ts = int(time.time())

  change = client.query("CHANGE NEW")["Change ID"][0]
  client.checkout(change=change)

  for _ in range(num_nodes):
    client.query(f"create (n:NEWNODE{ts})")

  client.query(f"commit")

  for _ in range(num_edges):
    client.query(f"create (n:NEWNODE{ts})-[:NEWEDGE{ts}]-(m:NEWNODE{ts})")

  return ts, change

def submit_change(client : TuringDB, change_id : str):
  client.checkout(change=change_id)
  client.query("change submit")

def main():
  client = TuringDB(host="http://localhost:6666")
  print("Connected to DB")

  graph = "complex_rebases_graph"
  client.query(f"CREATE GRAPH {graph}")
  print(f"Created {graph}")

  client.set_graph(graph)
  print(f"Set {graph}")

  random.seed(333) # Determinism over tests
  # Random number of nodes/edges to generate
  rand_node_counts = [random.randint(1, MAX_NODES) for _ in range(NUM_CHANGES)]
  rand_edge_counts = [random.randint(1, MAX_EDGES) for _ in range(NUM_CHANGES)]

  for nodes, edges in zip(rand_node_counts, rand_edge_counts):
    make_local_changes(client, nodes, edges)  
    print(f"Created {nodes} nodes and {edges} edges")

  # Choose a random order to submit changes in
  submit_order = [x for x in range (NUM_CHANGES)]
  random.shuffle(submit_order)

  expected_nodes = 0
  expected_edges = 0

  for change in submit_order:
    submit_change(client, change_id=str(change))
    client.checkout('main')

    expected_nodes += rand_node_counts[change] + (2 * rand_edge_counts[change])
    expected_edges += rand_edge_counts[change]

    num_nodes : int = len(client.query("match (n) return n")["n"])
    num_edges : int = len(client.query("match (n)-[e]-(m) return e")["e"])

    assert expected_nodes == num_nodes
    assert expected_edges == num_edges

if __name__ == "__main__":
  main()
