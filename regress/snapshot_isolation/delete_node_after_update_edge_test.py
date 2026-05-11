from turingdb import TuringClient
from graph_utils import *

# Review-flagged scenario for ChangeConflictChecker:
#   - pre-branch edge E exists between pre-branch nodes X and Y
#   - main post-branch: property-update on E (commit enters E's ID into edgeWriteSet)
#   - Change (branched before the update): DELETE X
#
# The concern: checkNewEdgesIncidentToDeleted only looks at post-branch-created
# edges, and checkUpdatedEdgeConflicts only fires when the Change itself updates
# the edge. The question is whether any check catches the conflict.
#
# Expected path: WriteProcessor::performDeletions invokes
# CommitWriteBuffer::addHangingEdges on DELETE, which inserts X's incident edges
# (including E) into the Change's _deletedEdges. checkDeletedEdgeConflicts then
# rebases E and finds it in main's edgeWriteSet -> rejects the submit.
#
# This test locks in that coverage so the rebase-over-merge spec (which adds
# IDRemap-based edge renumbering) cannot silently regress it.

GRAPH_NAME : str = "ssi_dnaue_graph"

def run(client : TuringClient) -> None:
    setup_graph(client, GRAPH_NAME)
    assert validate_graph_setup(client)

    # setup_graph created edge id=0 between node id=6 (src) and node id=7 (tgt).
    change_updater = new_change(client)
    change_deletor = new_change(client)

    # change_updater: property-update the pre-branch edge.
    client.set_change(change=change_updater)
    client.query("MATCH ()-[e]->() WHERE e.id = 0 SET e.id = 100")
    submit_current_change(client)

    # change_deletor: delete one endpoint of the edge main just property-updated.
    # DELETE cascades E into the Change's _deletedEdges via addHangingEdges.
    client.set_change(change=change_deletor)
    client.query("MATCH (n) WHERE n.id = 6 DELETE n")
    client.query("commit")

    try:
        submit_current_change(client)
    except Exception as e:
        assert str(e) == "EXEC_ERROR: This change attempted to delete Edge 0 (which is now Edge 0 on main) which has been modified on main."
    else:
        assert False # Submit should be rejected: committing would silently drop main's property update on edge 0.
