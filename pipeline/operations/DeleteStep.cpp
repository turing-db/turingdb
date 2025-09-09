#include "DeleteStep.h"

#include "DataPart.h"
#include "versioning/DataPartModifier.h"
#include "PipelineException.h"
#include "versioning/Transaction.h"
#include "EnumerateFrom.h"
#include "writers/DataPartBuilder.h"

using namespace db;

namespace rg = ranges;
namespace rv = rg::views;

void DeleteStep::prepare(ExecutionContext* ctxt) {
    Transaction* rawTx = ctxt->getTransaction();
    if (!rawTx->writingPendingCommit()) {
        throw PipelineException(
            "DeleteStep: Cannot perform deletion outside of a write transaction");
    }

    auto& tx = rawTx->get<PendingCommitWriteTx>();

    // Get the dataparts of the most recent commit
    _dps = tx.viewGraph().commitDataparts();
    // Get the CommitBuilder for this transaction
    _commitBuilder = tx.commitBuilder();
}


void DeleteStep::detectHangingEdges(const WeakArc<DataPart>& oldDP,
                                    const NodeSet&& nodesToDelete,
                                    EdgeSet& edgesToDelete) {
    const auto& oldEdgeContainer = oldDP->edges();
    const uint64_t oldFirstEdgeID = oldEdgeContainer.getFirstEdgeID().getValue();

    // Get [edgeId, edgeRecord] pairs starting from the first edgeID, check the record to
    // see if the source or target node of the edge is to be deleted. If it is, this edge
    // also needs to be deleted.

    // In edges
    for (const auto& [edgeID, edgeRecord] :
         EnumerateFrom(oldFirstEdgeID, oldEdgeContainer.getIns())) {

        if (nodesToDelete.contains(edgeRecord._nodeID)) {
            edgesToDelete.emplace(edgeID);
        }
        if (nodesToDelete.contains(edgeRecord._otherID)) {
            edgesToDelete.emplace(edgeID);
        }
    }

    // Out edges
    for (const auto& [edgeID, edgeRecord] :
         EnumerateFrom(oldFirstEdgeID, oldEdgeContainer.getOuts())) {

        if (nodesToDelete.contains(edgeRecord._nodeID)) {
            edgesToDelete.emplace(edgeID);
        }
        if (nodesToDelete.contains(edgeRecord._otherID)) {
            edgesToDelete.emplace(edgeID);
        }
    }
}

void DeleteStep::execute() {
    if (_delNodes.empty() && _delEdges.empty()) {
        return;
    }

    // Iterate through existing dataparts of the latest committed commit
    for (const auto& [idx, dp]: rv::enumerate(_dps)) {
        // Calculate the range of nodes we need to delete from this datapart
        NodeID smallestNodeID = dp->getFirstNodeID();
        NodeID largestNodeID = dp->getFirstNodeID() + dp->getNodeCount() - 1;

        auto nlb = std::ranges::lower_bound(_delNodes, smallestNodeID);
        auto nub = std::ranges::upper_bound(_delNodes, largestNodeID);
        // The nodes to be deleted from this datapart are in the interval [lb, ub)
         
        // Create a set containing all NodeIDs in [smallestNodeID, largestNodeID]
        NodeSet thisDPDeletedNodes(nlb, nub);

        // Calculate the range of edges we need to delete from this datapart
        EdgeID smallestEdgeID = dp->getFirstEdgeID();
        EdgeID largestEdgeID = dp->getFirstEdgeID() + dp->getEdgeCount() - 1;

        auto elb = std::ranges::lower_bound(_delEdges, smallestEdgeID);
        auto eub = std::ranges::upper_bound(_delEdges, largestEdgeID);

        // Create a set containing all EdgeIDs in [smallestEdgeID, largestEdgeID]
        EdgeSet thisDPDeletedEdges(elb, eub);

        // We also may have edges created which have nodes in previous dataparts as source
        // or target. Add these to @ref thisDPDeletedEdges. We only need to check nodes to
        // be deleted whose ID is less than the ID of the first node in this DataPart.
        detectHangingEdges(dp, NodeSet(_delNodes.begin(), nlb), thisDPDeletedEdges);

        // If there is nothing to delete in this datapart, do not create a builder.
        if (thisDPDeletedNodes.empty() && thisDPDeletedEdges.empty()) {
            continue;
        }

        // Create a new @ref DataPartBuilder in the CommitBuilder. The initialisation of
        // the builder sets the the _firstNode/EdgeID as if this is a new datapart,
        // however these are overwritten in @ref DataPartModifer
        DataPartBuilder& newBuilder = _commitBuilder->newBuilder(idx);

        // Construct a DataPartModifier to fill the new DataPartBuilder
        auto modifier =
            DataPartModifier(dp, newBuilder, thisDPDeletedNodes, thisDPDeletedEdges);

        // Apply the deletions
        modifier.applyModifications();
    }
}
