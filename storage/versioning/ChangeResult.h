#pragma once

#include <optional>

#include "BasicResult.h"
#include "EnumToString.h"
#include "versioning/CommitResult.h"

namespace db {

enum class ChangeErrorType : uint8_t {
    GRAPH_NOT_FOUND,
    CHANGE_NOT_FOUND,
    COMMIT_NOT_FOUND,
    COULD_NOT_CREATE_CHANGE,
    COULD_NOT_ACCEPT_CHANGE,
    SERIALIZATION_ERROR,

    _SIZE,
};

using ChangeErrorTypeDescription = EnumToString<ChangeErrorType>::Create<
    EnumStringPair<ChangeErrorType::GRAPH_NOT_FOUND, "Graph does not exist">,
    EnumStringPair<ChangeErrorType::CHANGE_NOT_FOUND, "Change does not exist">,
    EnumStringPair<ChangeErrorType::COMMIT_NOT_FOUND, "Commit does not exist">,
    EnumStringPair<ChangeErrorType::COULD_NOT_CREATE_CHANGE, "Could not create change">,
    EnumStringPair<ChangeErrorType::COULD_NOT_ACCEPT_CHANGE, "Could not accept change">,
    EnumStringPair<ChangeErrorType::SERIALIZATION_ERROR, "Could not sync the graph state with the disk">>;

class ChangeError {
public:
    explicit ChangeError(ChangeErrorType type)
        : _type(type)
    {
    }

    ChangeError(ChangeErrorType type, CommitError commitError)
        : _commitError(commitError),
          _type(type)
        
    {
    }

    [[nodiscard]] ChangeErrorType getType() const { return _type; }
    [[nodiscard]] std::string fmtMessage() const;

    template <typename... T>
    static BadResult<ChangeError> result(ChangeErrorType type) {
        return BadResult<ChangeError>(ChangeError(type));
    }

    template <typename... T>
    static BadResult<ChangeError> result(ChangeErrorType type, CommitError commitError) {
        return BadResult<ChangeError>(ChangeError(type, commitError));
    }

private:
    std::optional<CommitError> _commitError;
    ChangeErrorType _type {};
};

template <typename T>
using ChangeResult = BasicResult<T, class ChangeError>;

}
