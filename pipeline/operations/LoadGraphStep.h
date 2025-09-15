#pragma once

#include <string>

namespace db {

class ExecutionContext;
class SystemManager;
class JobSystem;

class LoadGraphStep {
public:
    struct Tag {};

    LoadGraphStep(const std::string& graphFileName, const std::string& graphName);
    LoadGraphStep(const std::string& graphFileName);
    ~LoadGraphStep();

    void prepare(ExecutionContext* ctxt);

    inline bool isFinished() const { return true; }

    void reset() {}

    void execute();

    void describe(std::string& descr) const;

private:
    std::string _graphFileName;
    std::string _graphName;
    SystemManager* _sysMan {nullptr};
    JobSystem* _jobSystem {nullptr};
};

} 
