#pragma once

#include <string_view>

#include "QueryStatus.h"

namespace db {

class SystemManager;
class InterpreterContext;

class QueryInterpreterV2 {
public:
    explicit QueryInterpreterV2(SystemManager* sysMan);

    ~QueryInterpreterV2();

    void execute(const InterpreterContext& ctxt,
                 QueryStatus& status,
                 std::string_view query,
                 std::string_view graphName);

private:
    SystemManager* _sysMan {nullptr};

    void executeImpl(const InterpreterContext& ctxt,
                     QueryStatus& status,
                     std::string_view query,
                     std::string_view graphName);
};

}
