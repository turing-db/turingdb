from turingdb import TuringDB
from graph_utils import new_change, submit_current_change

GRAPH_NAME : str = "simpledb"

def run(client : TuringDB) -> None:
  def query(q : str):
    client.query(q)

  query(f"LOAD GRAPH {GRAPH_NAME}")
  print(f"Loaded graph {GRAPH_NAME}")
  client.set_graph(GRAPH_NAME)
  print(f"Set graph {GRAPH_NAME}")
  
  first_change = new_change(client)
  second_change = new_change(client)

  client.set_change(first_change)

  query(r'CREATE (n:Person{name="CYRUS"})')
  
