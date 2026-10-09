#pragma once

#include <string_view>

#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

namespace net::HTTP {
class Info;
}

namespace db {

class QueryStatus;

struct DBTransactionInfo {
    std::string_view graphName;
    CommitHash commit;
    ChangeID change;

    static void read(const net::HTTP::Info& httpInfo, DBTransactionInfo& info, QueryStatus& status);
};

}
