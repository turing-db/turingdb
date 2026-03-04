#pragma once

#include <spdlog/fmt/bundled/format.h>

#include "BasicResult.h"
#include "EnumToString.h"

namespace db {

enum class LockFileErrorType : uint8_t {
    UNKNOWN,
    PERMISSION_DENIED,
    ALREADY_LOCKED,
    NO_PID,
    NOT_LOCKED,

    _SIZE,
};

using LockFileErrorTypeDescription = EnumToString<LockFileErrorType>::Create<
    EnumStringPair<LockFileErrorType::UNKNOWN, "Unknown error">,
    EnumStringPair<LockFileErrorType::PERMISSION_DENIED, "Permission denied">,
    EnumStringPair<LockFileErrorType::ALREADY_LOCKED, "Already locked">,
    EnumStringPair<LockFileErrorType::NO_PID, "No PID in file">,
    EnumStringPair<LockFileErrorType::NOT_LOCKED, "Not locked">>;

class LockFileError {
public:
    explicit LockFileError(LockFileErrorType type,
                           const std::string& msg = "");

    LockFileErrorType getType() const { return _type; }
    std::string fmtMessage() const;

    template <typename... T>
    static BadResult<LockFileError> result(LockFileErrorType type,
                                           const std::string& msg = "") {
        return BadResult<LockFileError>(LockFileError(type, msg));
    }

private:
    LockFileErrorType _type {};
    std::string _message;
};

template <typename T>
using LockFileResult = BasicResult<T, class LockFileError>;

}
