#pragma once

#include "ExecutionContext.h"
#include "ID.h"
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

    explicit DeleteStep()
    {
    }

    using DeleteNodesStep = DeleteStep<NodeID>;
    using DeleteEdgesStep = DeleteStep<EdgeID>;

    void addDeletions(std::vector<IDT>&& deletedIDs);

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

}

