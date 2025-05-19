#pragma once

#include <string>

#include <variant>

#include "ExecutionContext.h"
#include "columns/ColumnVector.h"
#include "views/GraphView.h"
#include "versioning/CommitResult.h"
#include "versioning/CommitHash.h"

namespace db {

class ExecutionContext;
class SystemManager;
using CommitInfo = std::variant<CommitHash, std::string>;

class CommitStep {
public:
    struct Tag {};

    CommitStep() = default;
    ~CommitStep() = default;

    void prepare(ExecutionContext* ctxt);

    inline bool isFinished() const { return true; }

    void reset() {}

    void execute();

    void describe(std::string& descr) const;

private:
    JobSystem* _jobSystem {nullptr};
    WriteTransaction* _tx {nullptr};
};

}
