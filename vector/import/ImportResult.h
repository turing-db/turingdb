#pragma once

#include "BasicResult.h"
#include "EnumToString.h"

namespace vec {

enum class ImportErrorCode : uint8_t {
    Unknown = 0,

    NoVectors,
    InvalidData,
    InvalidDimension,
    InvalidVector,

    JSONInvalidValue,
    JSONParseError,
    JSONInvalidFormat,
    JSONInvalidMetadata,
    JSONMissingIDField,
    JSONMissingVectorField,

    _SIZE,
};

using ImportErrorTypeDescription = EnumToString<ImportErrorCode>::Create<
    EnumStringPair<ImportErrorCode::Unknown, "Unknown">,

    EnumStringPair<ImportErrorCode::NoVectors, "No vectors found to import">,
    EnumStringPair<ImportErrorCode::InvalidData, "Invalid data for import">,
    EnumStringPair<ImportErrorCode::InvalidDimension , "Invalid dimension">,
    EnumStringPair<ImportErrorCode::InvalidVector, "Vector has invalid dimensions">,

    EnumStringPair<ImportErrorCode::JSONInvalidValue, "Encountered invalid value during import">,
    EnumStringPair<ImportErrorCode::JSONParseError, "JSON parse error">,
    EnumStringPair<ImportErrorCode::JSONInvalidFormat, "JSON format error">,
    EnumStringPair<ImportErrorCode::JSONInvalidMetadata, "Invalid metadata for import">,
    EnumStringPair<ImportErrorCode::JSONMissingIDField, "Missing `id` field in vector entry">,
    EnumStringPair<ImportErrorCode::JSONMissingVectorField, "Missing `vector` field in vector entry">>;

class ImportError {
public:
    explicit ImportError(ImportErrorCode type, std::string message = "")
        : _type(type),
        _message(std::move(message))
    {
    }

    [[nodiscard]] ImportErrorCode getType() const { return _type; }
    [[nodiscard]] std::string fmtMessage() const;

    template <typename... T>
    static BadResult<ImportError> result(ImportErrorCode type,
                                         std::string message = "") {
        return BadResult<ImportError>(ImportError(type, std::move(message)));
    }

private:
    ImportErrorCode _type {};
    std::string _message;
};

template <typename T>
using ImportResult = BasicResult<T, class ImportError>;

}
