#include "DeleteStep.h"

#include "ExecutionContext.h"
#include "reader/GraphReader.h"
#include "versioning/CommitBuilder.h"
#include "versioning/CommitWriteBuffer.h"
#include "versioning/Transaction.h"
#include "ID.h"
#include "PipelineException.h"
#include <type_traits>

using namespace db;

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
}

template <TypedInternalID IDT>
void DeleteStep<IDT>::execute() {
    GraphReader reader = _commitBuilder->readGraph();

    for (IDT id : _deletions) {
        bool has {false};
        if constexpr (std::is_same_v<IDT, NodeID>) {
            has = reader.graphHasNode(id);
        } else if constexpr (std::is_same_v<IDT, EdgeID>) {
            has = reader.graphHasEdge(id);
        }

        if (!has) [[unlikely]] {
            std::string err =
                fmt::format("Graph does not contain {} with ID: {}.",
                            std::is_same_v<IDT, NodeID> ? "node" : "edge",
                            id.getValue());
            throw PipelineException(std::move(err));
        }

        CommitWriteBuffer& wb = _commitBuilder->writeBuffer();
        if constexpr (std::is_same_v<IDT, NodeID>) {
            wb.addDeletedNodes(_deletions);
        } else if constexpr (std::is_same_v<IDT, EdgeID>) {
            wb.addDeletedEdges(_deletions);
        }
    }
}
