#pragma once

#include <spdlog/fmt/bundled/format.h>

#include "BasicResult.h"
#include "DumpResult.h"
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
    DUMP_FAILED,

    _SIZE,
};

using CommitErrorTypeDescription = EnumToString<CommitErrorType>::Create<
    EnumStringPair<CommitErrorType::COMMIT_INVALID, "Commit is invalid (is not built are was already commited)">,
    EnumStringPair<CommitErrorType::COMMIT_HASH_EXISTS, "Commit hash is already registered">,
    EnumStringPair<CommitErrorType::COMMIT_HASH_NOT_EXISTS, "Commit hash does not exist">,
    EnumStringPair<CommitErrorType::COMMIT_NEEDS_REBASE, "Commit needs rebase">,
    EnumStringPair<CommitErrorType::CHANGE_NEEDS_REBASE, "Change needs rebase">,
    EnumStringPair<CommitErrorType::BUILD_DATAPART_FAILED, "Could not build datapart">,
    EnumStringPair<CommitErrorType::NO_PENDING_COMMIT, "No pending commit">,
    EnumStringPair<CommitErrorType::DUMP_FAILED, "Could Not Dump Commit">>;

class CommitError {
public:
    explicit CommitError(CommitErrorType type)
        : _type(type)
    {
    }

    explicit CommitError(CommitErrorType type, DumpError dumpError)
        : _type(type),
        _dumpError(dumpError)
    {
    }

    [[nodiscard]] CommitErrorType getType() const { return _type; }
    [[nodiscard]] std::string fmtMessage() const;

    template <typename... T>
    static BadResult<CommitError> result(CommitErrorType type) {
        return BadResult<CommitError>(CommitError(type));
    }

    template <typename... T>
    static BadResult<CommitError> result(CommitErrorType type, DumpError dumpError) {
        return BadResult<CommitError>(CommitError(type, dumpError));
    }

private:
    CommitErrorType _type {};
    std::optional<DumpError> _dumpError;
};

template <typename T>
using CommitResult = BasicResult<T, class CommitError>;

}
