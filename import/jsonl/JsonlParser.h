#pragma once

#include <stddef.h>
#include <istream>
#include <string_view>
#include <unordered_map>

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

    [[nodiscard]] static JsonlImportResult<void> parse(ChangeAccessor& change,
                                                       std::istream& stream,
                                                       const std::unordered_map<std::string_view, size_t>& embeddingSpecs = {});
};

}
