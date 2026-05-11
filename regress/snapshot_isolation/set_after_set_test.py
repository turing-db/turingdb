from turingdb import TuringClient
from graph_utils import *

GRAPH_NAME : str = "ssi_sas_graph"

"""
Tests two changes both setting the value of 
"""
def run(client :TuringClient) -> None:
  setup_graph(client, GRAPH_NAME)
  assert validate_graph_setup(client)

  fst_change = new_change(client)
  snd_change = new_change(client)

  client.set_change(change=fst_change)
  client.query("MATCH (n) WHERE n.id = 1 SET n.id = 100")

  client.set_change(change=snd_change)
  client.query("MATCH (n) WHERE n.id = 1 SET n.id = 1000")
  submit_current_change(client)

  client.set_change(change=fst_change)
  try:
    submit_current_change(client)
  except Exception as e:
    assert str(e) == "EXEC_ERROR: This change attempted to update Node 1 (which is now Node 1 on main) which has been modified on main."
  else:
    assert False # Submit should be rejected: if not, violates snapshot isolation
