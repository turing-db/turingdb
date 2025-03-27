#pragma once

#include <string_view>
#include <memory>

#include "QueryStatus.h"
#include "QueryCallback.h"
#include "versioning/CommitHash.h"

namespace db {

class SystemManager;
class LocalMemory;
class Executor;

class QueryInterpreter {
public:
    QueryInterpreter(SystemManager* sysMan);
    ~QueryInterpreter();

    QueryStatus execute(std::string_view query,
                        std::string_view graphName,
                        LocalMemory* mem,
                        QueryCallback callback,
                        CommitHash hash = CommitHash::head());

private:
    SystemManager* _sysMan {nullptr};
    std::unique_ptr<Executor> _executor;
};

}
