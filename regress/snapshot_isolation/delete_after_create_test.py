from turingdb import TuringDB
from graph_utils import *

# DELETE after CREATE as specified in
# https://www.notion.so/turingbio/Specification-12-9-25-26b3aad664c88075a272d4d1969daf13?source=copy_link#26c3aad664c880a28025fa531418d5cd

GRAPH_NAME : str = "ssi_dac_graph"

def run(client: TuringDB) -> None:
  setup_graph(client, GRAPH_NAME)
  assert validate_graph_setup(client)

  change_creator = new_change(client)
  change_deletor = new_change(client)

  # Create an edge between nodes 2 and 3, submit the change onto main
  client.set_change(change=change_creator)
  client.query("create (n @ 2)-[e:NEWEDGE]-(m @ 3)")
  submit_current_change(client)

  # Other change attempts to delete 2, this is fine locally,
  # but fails when we try to submit, because we would inadvertently
  # delete the new edge above
  client.set_change(change=change_deletor)
  client.query("delete nodes 2")
  client.query("commit")

  # Try and submit the deletion: we should reject because we have
  # a write conflict on the newly created edge
  try:
    submit_current_change(client)
  except Exception as e:
    assert str(e) == "EXEC_ERROR: Submit rejected: Commits on main have created an edge (ID: 2) incident to Node 2, which this Change attempts to delete."
  else:
    assert False # If an exception wasn't raised: bug

