#pragma once

#include <string_view>

#include "NLOutputSink.h"
#include "QueryStatus.h"
#include "versioning/CommitHash.h"
#include "versioning/ChangeID.h"

namespace db {

class SystemManager;
class LocalMemory;

class QueryInterpreterV3 {
public:
    explicit QueryInterpreterV3(SystemManager* sysMan);
    ~QueryInterpreterV3();

    void execute(QueryStatus& status,
                 std::string_view query,
                 std::string_view graphName,
                 CommitHash hash,
                 ChangeID changeID,
                 LocalMemory* mem,
                 NLOutputSink* sink);

private:
    SystemManager* _sysMan {nullptr};

    void executeImpl(QueryStatus& status,
                     std::string_view query,
                     std::string_view graphName,
                     CommitHash hash,
                     ChangeID changeID,
                     LocalMemory* mem,
                     NLOutputSink* sink);
};

}
