from turingdb import TuringDB
from graph_utils import *

# DELETE after DELETE as specified in
# https://www.notion.so/turingbio/Specification-12-9-25-26b3aad664c88075a272d4d1969daf13?source=copy_link#26c3aad664c880eba676ddbc815db708

GRAPH_NAME : str = "ssi_dad_graph"

def run(client : TuringDB) -> None:
  setup_graph(client, GRAPH_NAME)
  assert validate_graph_setup(client)

  change_fst_deletor = new_change(client)
  change_snd_deletor = new_change(client)
  change_thd_deletor = new_change(client)

  # Delete Node 9, also deletes edge 1
  client.set_change(change=change_fst_deletor)
  client.query("delete nodes 9")
  submit_current_change(client);

  # Obvious write-write conflict
  client.set_change(change=change_snd_deletor)
  client.query("delete nodes 9")
  client.query("commit") # Committing locally is fine

  # We expect an exception here
  try:
    submit_current_change(client)
  except Exception as e:
    assert str(e) == "EXEC_ERROR: This change attempted to delete Node 9 (which is now Node 9 on main) which has been modified on main."
  else:
    assert False # If no exception: bug

  # Subtle write-write conflict: Change1 deleting node 9 also deletes edge 1
  client.set_change(change=change_thd_deletor)
  client.query("delete edges 1")
  client.query("commit")
  # We expect an exception here
  try:
    submit_current_change(client)
  except Exception as e:
    assert str(e) == "EXEC_ERROR: This change attempted to delete Edge 1 (which is now Edge 1 on main) which has been modified on main."
  else:
    assert False # If no exception: bug

