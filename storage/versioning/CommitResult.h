#pragma once

#include <spdlog/fmt/bundled/format.h>

#include "BasicResult.h"
#include "EnumToString.h"

namespace db {

enum class CommitErrorType : uint8_t {
    COMMIT_INVALID,
    COMMIT_HASH_EXISTS,
    COMMIT_HASH_NOT_EXISTS,
    COMMIT_NEEDS_REBASE,
    CHANGE_NEEDS_REBASE,
    BUILD_DATAPART_FAILED,
    NO_PENDING_COMMIT,

    _SIZE,
};

using CommitErrorTypeDescription = EnumToString<CommitErrorType>::Create<
    EnumStringPair<CommitErrorType::COMMIT_INVALID, "Commit is invalid (is not built are was already commited)">,
    EnumStringPair<CommitErrorType::COMMIT_HASH_EXISTS, "Commit hash is already registered">,
    EnumStringPair<CommitErrorType::COMMIT_HASH_NOT_EXISTS, "Commit hash does not exist">,
    EnumStringPair<CommitErrorType::COMMIT_NEEDS_REBASE, "Commit needs rebase">,
    EnumStringPair<CommitErrorType::CHANGE_NEEDS_REBASE, "Change needs rebase">,
    EnumStringPair<CommitErrorType::BUILD_DATAPART_FAILED, "Could not build datapart">,
    EnumStringPair<CommitErrorType::NO_PENDING_COMMIT, "No pending commit">>;

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
