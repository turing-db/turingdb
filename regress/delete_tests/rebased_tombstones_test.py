from turingdb import TuringDB
from graph_utils import new_change, submit_current_change
import pandas as pd

GRAPH_NAME : str = "simpledb"

def run(client : TuringDB) -> None:
  EXPECTED_NODE_IDS : list[int] = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,16]
  def query(q : str):
    return client.query(q)

  query(f"LOAD GRAPH {GRAPH_NAME}")
  print(f"Loaded graph {GRAPH_NAME}")
  client.set_graph(GRAPH_NAME)
  print(f"Set graph {GRAPH_NAME}")
  
  first_change = new_change(client)
  second_change = new_change(client)

  # Create changes under the second
  client.set_change(first_change)
  # query(r'CREATE (n:Person{name="Cyrus"})') # node 13 FIXME
  # query(r'CREATE (n:Person{name="Doruk"})') # node 14 FIXME
  query(r'CREATE (n:Person)') # node 13
  query(r'CREATE (n:Person)') # node 14
  submit_current_change(client)

  # Second change attempts to delete new nodes which need to be rebased
  client.set_change(second_change)
  # query(r'CREATE (n:Interest{name="Gym"})')        # node 13 FIXME
  # query(r'CREATE (n:Interest{name="Manchester"})') # node 14 FIXME
  query(r'CREATE (n:Interest)')        # node 13
  query(r'CREATE (n:Interest)') # node 14
  # query("commit") # FIXME
  # query("delete nodes 13") # FIXME
  submit_current_change(client)

  # Now, second_change's node 13 is node 15, and node 14 is node 16
  output : pd.DataFrame = query("match (n) return n")
  node_ids : pd.Series = output[0]

  for expected, actual in zip(EXPECTED_NODE_IDS, node_ids):
    assert expected == actual
