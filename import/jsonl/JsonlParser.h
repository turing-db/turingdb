#pragma once

#include <stddef.h>
#include <istream>

#include "JsonlImportResult.h"

namespace db {

class ChangeAccessor;

class JsonlParser {
public:
    JsonlParser() = delete;
    ~JsonlParser() = delete;

    JsonlParser(const JsonlParser&) = delete;
    JsonlParser(JsonlParser&&) = delete;
    JsonlParser& operator=(const JsonlParser&) = delete;
    JsonlParser& operator=(JsonlParser&&) = delete;

    static JsonlImportResult<void> parse(ChangeAccessor& change, std::istream& stream);
};

}
