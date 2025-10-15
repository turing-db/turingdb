#pragma once

#include "ExecutionContext.h"
#include "ID.h"
#include "PipelineException.h"
#include "reader/GraphReader.h"
#include "versioning/CommitBuilder.h"
#include "versioning/CommitWriteBuffer.h"
#include "versioning/Transaction.h"

namespace db {

class ExecutionContext;
class CommitBuilder;

template <TypedInternalID IDT>
class DeleteStep {
public:
    struct Tag {};

    explicit DeleteStep(std::vector<IDT>&& deletedIDs)
        : _deletions(std::move(deletedIDs))
    {
    }

    using DeleteNodesStep = DeleteStep<NodeID>;
    using DeleteEdgesStep = DeleteStep<EdgeID>;

    void prepare(ExecutionContext* ctxt);

    void reset() {};

    bool isFinished() const { return true; };

    void execute();

    void describe(std::string& descr) const;

private:
    std::vector<IDT> _deletions;
    CommitBuilder* _commitBuilder {nullptr};
    CommitWriteBuffer* _writeBuffer {nullptr};
};

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
    }

    if constexpr (std::is_same_v<IDT, NodeID>) {
        _writeBuffer->addDeletedNodes(_deletions);
    } else if constexpr (std::is_same_v<IDT, EdgeID>) {
        _writeBuffer->addDeletedEdges(_deletions);
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

}

