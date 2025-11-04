#include "DeleteStep.h"

#include "PipelineException.h"
#include "reader/GraphReader.h"

// TODO: for v2
constexpr bool DETACH = true;

using namespace db;

namespace db {
    template class DeleteStep<NodeID>;
    template class DeleteStep<EdgeID>;
}

template <TypedInternalID IDT>
void DeleteStep<IDT>::addDeletions(const std::vector<IDT>& deletedIDs) {
    bioassert(_deletions.empty());
    _deletions = deletedIDs;
}

template <TypedInternalID IDT>
void DeleteStep<IDT>::prepare(ExecutionContext* ctxt) {
    Transaction* rawTx = ctxt->getTransaction();
    if (!rawTx->writingPendingCommit()) {
        throw PipelineException(
            "DeleteStep: Cannot perform deletion outside of a write transaction");
    }

    auto& tx = rawTx->get<PendingCommitWriteTx>();

    // Get the CommitBuilder for this transaction
    _commitBuilder = tx.commitBuilder();
    _writeBuffer = &_commitBuilder->writeBuffer();
}

template <TypedInternalID IDT>
void DeleteStep<IDT>::execute() {
    GraphReader reader = _commitBuilder->readGraph();

    for (IDT id : _deletions) {
        bool existsAndNotDeleted {false};
        if constexpr (std::is_same_v<IDT, NodeID>) {
            existsAndNotDeleted = reader.graphHasNode(id);
        } else if constexpr (std::is_same_v<IDT, EdgeID>) {
            existsAndNotDeleted = reader.graphHasEdge(id);
        }

        if (!existsAndNotDeleted) [[unlikely]] {
            std::string err =
                fmt::format("Graph does not contain {} with ID: {}.",
                            std::is_same_v<IDT, NodeID> ? "node" : "edge",
                            id.getValue());
            throw PipelineException(std::move(err));
        }
    }

    if constexpr (std::is_same_v<IDT, NodeID>) {
        _writeBuffer->addDeletedNodes(_deletions);
    } else if constexpr (std::is_same_v<IDT, EdgeID>) {
        _writeBuffer->addDeletedEdges(_deletions);
    }

    if constexpr(std::is_same_v<IDT, NodeID> && DETACH) {
        _writeBuffer->addHangingEdges(reader.getView());
    }
}

template <TypedInternalID IDT>
void DeleteStep<IDT>::describe(std::string& descr) const {
    descr.clear();
    if constexpr (std::is_same_v<IDT, NodeID>) {
        descr += "DeleteNodesStep";
    } else if constexpr (std::is_same_v<IDT, EdgeID>) {
        descr += "DeleteEdgesStep";
    }
}
