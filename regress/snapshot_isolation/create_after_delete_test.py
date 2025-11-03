from turingdb import TuringDB
from graph_utils import *

# CREATE after DELETE as specified in
# https://www.notion.so/turingbio/Specification-12-9-25-26b3aad664c88075a272d4d1969daf13?source=copy_link#26c3aad664c8806b82fcd28b870ed275

GRAPH_NAME : str = "ssi_cad_graph"

def run(client : TuringDB) -> None:
  setup_graph(client, GRAPH_NAME)
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
