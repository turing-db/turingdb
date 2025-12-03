#pragma once

#include <string_view>

#include "QueryStatus.h"

namespace db {

class SystemManager;
class JobSystem;

}

namespace db::v2 {

class InterpreterContext;

class QueryInterpreterV2 {
public:
    QueryInterpreterV2(db::SystemManager* sysMan,
                       db::JobSystem* jobSystem);

    ~QueryInterpreterV2();

    db::QueryStatus execute(const InterpreterContext& ctxt,
                            std::string_view query,
                            std::string_view graphName);

private:
    SystemManager* _sysMan {nullptr};
    JobSystem* _jobSystem {nullptr};
};

}
