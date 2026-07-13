#pragma once

#include <optional>
#include <string>

#include "BasicResult.h"
#include "EnumToString.h"
#include "dump/DumpResult.h"

namespace db {

enum class LoadGraphErrorType : uint8_t {
    UNKNOWN = 0,
    GRAPH_EXISTS,
    FILE_ERROR,
    _SIZE
};

using LoadGraphErrorTypeDescription = EnumToString<LoadGraphErrorType>::Create<
    EnumStringPair<LoadGraphErrorType::UNKNOWN, "Unknown">,
    EnumStringPair<LoadGraphErrorType::GRAPH_EXISTS, "Graph already loaded">,
    EnumStringPair<LoadGraphErrorType::FILE_ERROR, "Could not load graph from disk">>;

class LoadGraphError {
public:
    explicit LoadGraphError(LoadGraphErrorType type)
        : _type(type)
    {
    }

    LoadGraphError(LoadGraphErrorType type,
                   std::optional<DumpError> dumpError)
        : _type(type),
        _dumpError(dumpError)
    {
    }

    [[nodiscard]] LoadGraphErrorType getType() const { return _type; }
    [[nodiscard]] std::optional<DumpError> getDumpError() const { return _dumpError; }
    [[nodiscard]] std::string fmtMessage() const;

    template <typename... T>
    static BadResult<LoadGraphError> result(LoadGraphErrorType type) {
        return BadResult<LoadGraphError>(LoadGraphError(type, std::nullopt));
    }

    template <typename... T>
    static BadResult<LoadGraphError> result(LoadGraphErrorType type,
                                            DumpError dumpError) {
        return BadResult<LoadGraphError>(LoadGraphError(type, dumpError));
    }

private:
    LoadGraphErrorType _type {LoadGraphErrorType::UNKNOWN};
    std::optional<DumpError> _dumpError;
};

template <typename T>
using LoadGraphResult = BasicResult<T, class LoadGraphError>;

}
