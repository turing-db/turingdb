#pragma once

#include <string>
#include <spdlog/fmt/bundled/format.h>

#include "SourceLocation.h"

namespace db::v2 {

class CypherError {
public:
    explicit CypherError(std::string_view query)
        :_query(query)
    {
    }

    void generate(std::string& errorOutput);

    void setTitle(std::string_view title) { _title = title; }
    void setErrorMsg(std::string_view msg) { _errorMsg = msg; }
    void setLocation(const SourceLocation& loc) { _loc = &loc; }

    std::string_view getTitle() const { return _title; }
    std::string_view getErrorMsg() const { return _errorMsg; }
    std::string_view getQuery() const { return _query; }
    const SourceLocation* getLocation() const { return _loc; }

private:
    std::string_view _title = "Cypher error";
    std::string_view _query;
    std::string_view _errorMsg = "Unknown error";
    const SourceLocation* _loc {nullptr};

    void generateSingleLine(std::string& errorOutput);
    void generateMultiLine(std::string& errorOutput);
};

}
