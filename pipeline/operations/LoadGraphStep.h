#pragma once

#include <string>

namespace db {

class ExecutionContext;
class SystemManager;
class JobSystem;

class LoadGraphStep {
public:
    struct Tag {};

    LoadGraphStep(const std::string& fileName, const std::string& graphName);
    LoadGraphStep(const std::string& graphName);
    ~LoadGraphStep();

    void prepare(ExecutionContext* ctxt);

    bool isFinished() const { return true; }

    void reset() {}

    void execute();

    void describe(std::string& descr) const;

private:
    std::string _fileName;
    std::string _graphName;
    SystemManager* _sysMan {nullptr};
    JobSystem* _jobSystem {nullptr};
};

} 
