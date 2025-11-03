from turingdb import TuringDB

# CREATE after DELETE as specified in
# https://www.notion.so/turingbio/Specification-12-9-25-26b3aad664c88075a272d4d1969daf13?source=copy_link#26c3aad664c8806b82fcd28b870ed275

GRAPH_NAME : str = "ssi_cad_graph"

NUM_TOTAL_NODES = 10
NUM_EDGES : int = 2                                 # CREATE (n)-[e]-(m)
NUM_NODES : int = NUM_TOTAL_NODES - (2 * NUM_EDGES) # CREATE (n)

# Helper to create a change and return its ID
def new_change(client : TuringDB) -> str:
  change_id : str = client.query("CHANGE NEW")[0][0]
  client.checkout(change=change_id)
  return change_id

# Creates a graph for conflict checking to take place on
def setup_graph(client : TuringDB) -> None:
  client.query(f"CREATE GRAPH {GRAPH_NAME}")
  print(f"Created {GRAPH_NAME}")

  client.set_graph(GRAPH_NAME)
  print(f"Set {GRAPH_NAME}")
  
  new_change(client)

  for _ in range(NUM_NODES):
    client.query(f"create (n:NEWNODE)")

  for _ in range(NUM_EDGES):
    client.query(f"create (n:NEWNODE)-[:NEWEDGE]-(m:NEWNODE)")

  submit_current_change(client)

def validate_graph_setup(client : TuringDB) -> bool:
  num_nodes : int = len(client.query("match (n) return n")[0])
  num_edges : int = len(client.query("match (n)-[e]-(m) return e")[0])

  if not num_nodes == NUM_TOTAL_NODES:
    print(f"Expected {NUM_NODES} nodes but got {num_nodes} nodes.")
  if not num_edges == NUM_EDGES:
    print(f"Expected {NUM_EDGES} edges but got {num_edges} edges.")

  return (num_nodes == NUM_TOTAL_NODES) and (num_edges == NUM_EDGES)

# Helper to submit current change and switch back to main
def submit_current_change(client : TuringDB) -> None:
  client.query("change submit")
  client.checkout('main')

def run(client : TuringDB) -> None:
  setup_graph(client)
  assert validate_graph_setup(client)

  change_creator = new_change(client)
  change_deletor = new_change(client)

  # Perform a create command from 
  client.set_change(change=change_creator)
  # Try to create an edge between nodes 3 and 4: fine locally
  client.query("create (n @ 3)-[e:NEWEDGE]-(m @ 4)")

  # Try to delete node 3: fine locally, and submit
  client.set_change(change=change_deletor)
  client.query("delete nodes 3")
  submit_current_change(client)

  # Try and submit the change which created (3)-[NEWEDGE]-(4)
  # This should reject because 3 has since been deleted
  client.set_change(change=change_creator)
  try:
    submit_current_change(client)
  except Exception as e:
    assert str(e) == "EXEC_ERROR: Unexpected exception: This change attempted to create an edge with source Node 3 (which is now Node 3 on main) which has been modified on main."
  else:
    assert False # If an exception wasn't  raised: bug
