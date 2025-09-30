#pragma once

#include "ID.h"

namespace db {

class ExecutionContext;
class CommitBuilder;

template <TypedInternalID IDT>
class DeleteStep {
public:
    struct Tag{};

    explicit DeleteStep(std::vector<IDT>&& deletedIDs)
        : _deletions(std::move(deletedIDs))
    {
    }

    void prepare(ExecutionContext* ctxt);

    void reset();

    bool isFinished();

    void execute();

    void describe(std::string& descr);

private:
    std::vector<IDT> _deletions;
    CommitBuilder* _commitBuilder {nullptr};
};
    
}
