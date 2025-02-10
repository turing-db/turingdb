#pragma once

#include "BasicResult.h"
#include "EnumToString.h"

#include <spdlog/fmt/bundled/format.h>

namespace fs {

class Path;

enum class ErrorType : uint8_t {
    UNKNOWN = 0,
    NOT_EXISTS,
    NOT_DIRECTORY,
    ALREADY_EXISTS,
    CANNOT_MKDIR,
    CANNOT_REMOVE,
    OPEN_DIRECTORY,
    CLOSE_DIRECTORY,
    OPEN_FILE,
    MAP,
    READ_FILE,
    READ_PAGE,
    WRITE_FILE,
    WRITE_PAGE,
    CLEAR_FILE,
    CLOSE_FILE,
    SYNC_FILE,
    COULD_NOT_SEEK,

    _SIZE,
};

using ErrorTypeDescription = EnumToString<ErrorType>::Create<
    EnumStringPair<ErrorType::UNKNOWN, "Unknown">,
    EnumStringPair<ErrorType::NOT_EXISTS, "Does not exist">,
    EnumStringPair<ErrorType::NOT_DIRECTORY, "Not a directory">,
    EnumStringPair<ErrorType::ALREADY_EXISTS, "Already exists">,
    EnumStringPair<ErrorType::CANNOT_MKDIR, "Could not make directory">,
    EnumStringPair<ErrorType::CANNOT_REMOVE, "Could not remove">,
    EnumStringPair<ErrorType::OPEN_DIRECTORY, "Could not open directory">,
    EnumStringPair<ErrorType::CLOSE_DIRECTORY, "Could not close directory">,
    EnumStringPair<ErrorType::OPEN_FILE, "Could not open file">,
    EnumStringPair<ErrorType::MAP, "Could not map file">,
    EnumStringPair<ErrorType::READ_FILE, "Could not read file">,
    EnumStringPair<ErrorType::READ_PAGE, "Could not read page">,
    EnumStringPair<ErrorType::WRITE_FILE, "Could not write file">,
    EnumStringPair<ErrorType::WRITE_PAGE, "Could not write page">,
    EnumStringPair<ErrorType::CLEAR_FILE, "Could not clear file">,
    EnumStringPair<ErrorType::CLOSE_FILE, "Could not close file">,
    EnumStringPair<ErrorType::SYNC_FILE, "Could not sync file">,
    EnumStringPair<ErrorType::COULD_NOT_SEEK, "Could not seek">>;

class Error {
public:
    explicit Error(ErrorType type, int errnumber = -1)
        : _errno(errnumber),
          _type(type)
    {
    }

    [[nodiscard]] ErrorType getType() const { return _type; }
    [[nodiscard]] int getErrno() const { return _errno; }
    [[nodiscard]] std::string fmtMessage() const;

    template <typename... T>
    static BadResult<Error> result(ErrorType type, int errnumber = -1) {
        return BadResult<Error>(Error(type, errnumber));
    }

private:
    int _errno {-1};
    ErrorType _type {};
};

template <typename T>
using Result = BasicResult<T, class Error>;

}
