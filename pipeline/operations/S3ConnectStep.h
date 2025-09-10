#pragma once

#include <string>

namespace db {

class ExecutionContext;
class SystemManager;
class JobSystem;

class S3ConnectStep {
public:
    struct Tag {};

    S3ConnectStep(const std::string& accessId, const std::string& secretKey, const std::string& region);
    ~S3ConnectStep();

    void prepare(ExecutionContext* ctxt);

    inline bool isFinished() const { return true; }

    void reset() {}

    void execute();

    void describe(std::string& descr) const;

private:
    const std::string& _accessId;
    const std::string& _secretKey;
    const std::string& _region;
    SystemManager* _sysMan {nullptr};
};

}
