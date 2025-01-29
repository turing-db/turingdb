#pragma once

#include <string>

namespace db {

class ExecutionContext;
class SystemManager;

class LoadGraphStep {
public:
    struct Tag {};

    LoadGraphStep(const std::string& graphName);
    ~LoadGraphStep();

    void prepare(ExecutionContext* ctxt);

    inline bool isFinished() const { return true; }

    void reset() {}

    void execute();

private:
    std::string _graphName;
    SystemManager* _sysMan {nullptr};
};

} 
