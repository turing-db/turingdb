#pragma once

#include <string_view>

#include "QueryStatus.h"

namespace db {
class SystemManager;
class JobSystem;
}

namespace db {

class InterpreterContext;

class QueryInterpreterV2 {
public:
    QueryInterpreterV2(db::SystemManager* sysMan,
                       db::JobSystem* jobSystem);

    ~QueryInterpreterV2();

    void execute(const InterpreterContext& ctxt,
                 QueryStatus& status,
                 std::string_view query,
                 std::string_view graphName);

private:
    SystemManager* _sysMan {nullptr};
    JobSystem* _jobSystem {nullptr};

    void executeImpl(const InterpreterContext& ctxt,
                     QueryStatus& status,
                     std::string_view query,
                     std::string_view graphName);
};

}
