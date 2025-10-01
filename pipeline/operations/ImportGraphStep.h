#pragma once

#include "Path.h"

namespace db {

class ExecutionContext;
class SystemManager;
class JobSystem;

class ImportGraphStep {
public:
    struct Tag {};

    ImportGraphStep(const fs::Path& filePath, const std::string& graphName);
    ~ImportGraphStep();

    void prepare(ExecutionContext* ctxt);

    bool isFinished() const { return true; }

    void reset() {}

    void execute();

    void describe(std::string& descr) const;

private:
    fs::Path _filePath;
    std::string _graphName;
    SystemManager* _sysMan {nullptr};
    JobSystem* _jobSystem {nullptr};
};

} 
