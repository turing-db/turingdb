#pragma once

#include <string_view>

#include "ParameterMap.h"

namespace net::HTTP {
class Info;
}

namespace db {

class QueryStatus;

// The body is [queryLen][query bytes][paramLen][param bytes], each length a WireSize. A
// paramLen of 0 means no parameters; otherwise the bytes are one map value of the nested
// container format.
class TuringProtoQueryRequest {
public:
    TuringProtoQueryRequest();
    ~TuringProtoQueryRequest();

    static void read(const net::HTTP::Info& httpInfo, TuringProtoQueryRequest& request, QueryStatus& status);

    std::string_view getQuery() const { return _query; }
    const ParameterMap& getParameters() const { return _parameters; }

private:
    std::string_view _query;
    ParameterMap _parameters;
};

}
