from turingdb import TuringDB

NUM_TOTAL_NODES = 10
NUM_EDGES : int = 2                                 # CREATE (n)-[e]-(m)
NUM_NODES : int = NUM_TOTAL_NODES - (2 * NUM_EDGES) # CREATE (n)

# Helper to create a change and return its ID
def new_change(client : TuringDB) -> str:
  client.checkout('main')
  change_id : str = client.query("CHANGE NEW")[0][0]
  client.checkout(change=change_id)
  return change_id

# Helper to submit current change and switch back to main
def submit_current_change(client : TuringDB) -> None:
  client.query("change submit")
  client.checkout('main')

# Creates a graph for conflict checking to take place on
def setup_graph(client : TuringDB, graph_name : str) -> None:
  client.query(f"CREATE GRAPH {graph_name}")
  print(f"Created {graph_name}")

  client.set_graph(graph_name)
  print(f"Set {graph_name}")
  
  new_change(client)

  node_id : int = 0
  edge_id : int = 0

  for _ in range(NUM_NODES):
    client.query(f"create (n:NEWNODE {{id: {node_id}}})")
    node_id += 1

  for _ in range(NUM_EDGES):
    client.query(f"create (n:NEWNODE {{id: {node_id}}})-[:NEWEDGE {{id: {edge_id}}}]-(m:NEWNODE {{id: {node_id + 1}}})")
    edge_id += 1
    node_id += 2

  submit_current_change(client)

def validate_graph_setup(client : TuringDB) -> bool:
  num_nodes : int = len(client.query("match (n) return n")[0])
  num_edges : int = len(client.query("match (n)-[e]-(m) return e")[0])

  if not num_nodes == NUM_TOTAL_NODES:
    print(f"Expected {NUM_NODES} nodes but got {num_nodes} nodes.")
  if not num_edges == NUM_EDGES:
    print(f"Expected {NUM_EDGES} edges but got {num_edges} edges.")

  return (num_nodes == NUM_TOTAL_NODES) and (num_edges == NUM_EDGES)
