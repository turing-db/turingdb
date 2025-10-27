#pragma once

#include <string>

#include "ExecutionContext.h"

namespace db {

class ExecutionContext;
class SystemManager;
class Graph;

class DataPartMergeStep {
public:
    struct Tag {};

    DataPartMergeStep() = default;
    ~DataPartMergeStep() = default;

    void prepare(ExecutionContext* ctxt);

    inline bool isFinished() const { return true; }

    void reset() {}

    void execute();

    void describe(std::string& descr) const;

private:
    JobSystem* _jobSystem {nullptr};
    SystemManager* _systemManager {nullptr};
    Graph* _graph {nullptr};
};

}
