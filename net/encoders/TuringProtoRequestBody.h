#pragma once

#include <array>
#include <span>
#include <string_view>

#include "TuringProtoHeaders.h"
#include "TuringProtoOutBuf.h"
#include "TuringProtoParameterWriter.h"

namespace db {
class ParameterMap;
}

namespace net::proto {

// The body of a query request is [queryLen][query bytes][paramLen][param bytes]. This holds
// everything except the query bytes which is sent separately.
class TuringProtoRequestBody {
public:
    TuringProtoRequestBody();
    ~TuringProtoRequestBody();

    void build(std::string_view query, const db::ParameterMap* parameters);

    std::span<char> getPrefix() { return _prefix; }
    std::span<char> getSuffix() { return {_suffix.data(), _suffix.size()}; }

private:
    std::array<char, sizeof(WireSize)> _prefix {};
    TuringProtoOutBuf _suffix;
    TuringProtoParameterWriter _parameterWriter;
};

}
