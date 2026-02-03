#pragma once

#include "BasicResult.h"
#include "EnumToString.h"

namespace db {

enum class JsonlImportErrorType : uint8_t {
    UNKNOWN = 0,
    JSONL_PARSE_ERROR,
    MISSING_ENTITY_TYPE,
    MISSING_LABELS,
    MISSING_EDGE_TYPE,
    MISSING_EDGE_SRC,
    MISSING_EDGE_TGT,
    MISSING_EDGE_SRC_ID,
    MISSING_EDGE_TGT_ID,

    _SIZE
};

using JsonlImportErrorTypeDescription = EnumToString<JsonlImportErrorType>::Create<
    EnumStringPair<JsonlImportErrorType::UNKNOWN, "Unknown">,
    EnumStringPair<JsonlImportErrorType::JSONL_PARSE_ERROR, "JSONL parse">,
    EnumStringPair<JsonlImportErrorType::MISSING_ENTITY_TYPE, "Missing entity type">,
    EnumStringPair<JsonlImportErrorType::MISSING_LABELS, "Missing labels">,
    EnumStringPair<JsonlImportErrorType::MISSING_EDGE_TYPE, "Missing edge type">,
    EnumStringPair<JsonlImportErrorType::MISSING_EDGE_SRC, "Missing edge source node data">,
    EnumStringPair<JsonlImportErrorType::MISSING_EDGE_TGT, "Missing edge target node data">,
    EnumStringPair<JsonlImportErrorType::MISSING_EDGE_SRC_ID, "Missing edge source node id">,
    EnumStringPair<JsonlImportErrorType::MISSING_EDGE_TGT_ID, "Missing edge target node id">>;

class JsonlImportError {
public:
    explicit JsonlImportError(JsonlImportErrorType type,
                              size_t line,
                              const std::string& message = "")
        : _type(type),
        _line(line),
        _message(message)
    {
    }

    [[nodiscard]] JsonlImportErrorType getType() const { return _type; }
    [[nodiscard]] size_t getLine() const { return _line; }
    [[nodiscard]] std::string_view getMessage() const { return _message; }
    [[nodiscard]] std::string fmtMessage() const;

    template <typename... T>
    static BadResult<JsonlImportError> result(JsonlImportErrorType type,
                                              size_t line,
                                              const std::string& message = "") {
        return BadResult<JsonlImportError>(JsonlImportError(type, line, message));
    }

private:
    JsonlImportErrorType _type {JsonlImportErrorType::UNKNOWN};
    size_t _line {0};
    std::string _message;
};

template <typename T>
using JsonlImportResult = BasicResult<T, class JsonlImportError>;

}
