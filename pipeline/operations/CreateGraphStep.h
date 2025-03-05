#pragma once

#include <string>

namespace db {

class ExecutionContext;
class SystemManager;

class CreateGraphStep {
public:
    struct Tag {};

    CreateGraphStep(const std::string& graphName);
    ~CreateGraphStep();

    void prepare(ExecutionContext* ctxt);

    inline bool isFinished() const { return true; }

    void reset() {}

    void execute();

    void describe(std::string& descr) const;

private:
    std::string _graphName;
    SystemManager* _sysMan {nullptr};
};

}
