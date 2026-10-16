#pragma once

#include <string>
#include <string_view>

#include "ParameterMap.h"

namespace net::HTTP {
class Info;
}

namespace db {

class QueryStatus;

class DBQueryRequest {
public:
    DBQueryRequest();
    ~DBQueryRequest();

    static void read(const net::HTTP::Info& httpInfo, DBQueryRequest& request, QueryStatus& status);

    std::string_view getQuery() const { return _query; }
    const ParameterMap& getParameters() const { return _parameters; }

private:
    std::string _query;
    ParameterMap _parameters;
};

}
