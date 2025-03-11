#pragma once

#include <spdlog/fmt/bundled/format.h>

#include "BasicResult.h"
#include "EnumToString.h"

namespace db {

enum class CommitErrorType : uint8_t {
    COMMIT_INVALID,
    COMMIT_HASH_EXISTS,
    COMMIT_NEEDS_REBASE,

    _SIZE,
};

using CommitErrorTypeDescription = EnumToString<CommitErrorType>::Create<
    EnumStringPair<CommitErrorType::COMMIT_INVALID, "Commit is invalid (is not built are was already commited)">,
    EnumStringPair<CommitErrorType::COMMIT_HASH_EXISTS, "Commit hash is already registered">,
    EnumStringPair<CommitErrorType::COMMIT_NEEDS_REBASE, "Commit needs rebase">>;

class CommitError {
public:
    explicit CommitError(CommitErrorType type)
        : _type(type)
    {
    }

    [[nodiscard]] CommitErrorType getType() const { return _type; }
    [[nodiscard]] std::string fmtMessage() const;

    template <typename... T>
    static BadResult<CommitError> result(CommitErrorType type) {
        return BadResult<CommitError>(CommitError(type));
    }

private:
    CommitErrorType _type {};
};

template <typename T>
using CommitResult = BasicResult<T, class CommitError>;

}
