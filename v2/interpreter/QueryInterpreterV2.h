#pragma once

#include <string_view>

#include "QueryStatus.h"

namespace db {

class SystemManager;
class JobSystem;
class InterpreterContext;

class QueryInterpreterV2 {
public:
    QueryInterpreterV2(SystemManager* sysMan,
                       JobSystem* jobSystem);
    ~QueryInterpreterV2();

    QueryStatus execute(const InterpreterContext& ctxt,
                        std::string_view query,
                        std::string_view graphName);

private:
    SystemManager* _sysMan {nullptr};
    JobSystem* _jobSystem {nullptr};
};

}
