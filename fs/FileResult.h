#pragma once

#include "BasicResult.h"

#include <spdlog/fmt/bundled/format.h>

namespace fs {

class Error {
public:
    explicit Error(const char* msg, const char* info = nullptr)
        : _msg(msg),
          _info(info)
    {
    }

    [[nodiscard]] std::string_view getMessage() const { return _msg; }

    [[nodiscard]] std::string fmtMessage() const {
        return _info == nullptr
                 ? fmt::format("Filesystem error: {}", _msg)
                 : fmt::format("Filesystem error: {} ({})", _msg, _info);
    }

    template <typename... T>
    static BadResult<Error> result(const char* msg,
                                   const char* info = nullptr) {
        return BadResult(Error(msg, info));
    }

private:
    const char* _msg {nullptr};
    const char* _info {nullptr};
};

class FileError {
public:
    explicit FileError(const char* path,
                       const char* msg,
                       const char* info = nullptr)
        : _path(path),
          _msg(msg),
          _info(info)
    {
    }

    [[nodiscard]] std::string_view getPath() const { return _path; }
    [[nodiscard]] std::string_view getInfo() const { return _info; }
    [[nodiscard]] std::string_view getMessage() const { return _msg; }

    [[nodiscard]] std::string fmtMessage() const {
        return _info == nullptr
                 ? fmt::format("File ({}) error: {}", _path, _msg)
                 : fmt::format("File ({}) error: {} ({})", _path, _msg, _info);
    }

    template <typename... T>
    static BadResult<FileError> result(const char* path,
                                       const char* msg,
                                       const char* info = nullptr) {
        return BadResult(FileError(path, msg, info));
    }

private:
    const char* _path;
    const char* _msg;
    const char* _info = nullptr;
};

template <typename T>
using Result = BasicResult<T, class Error>;

template <typename T>
using FileResult = BasicResult<T, class FileError>;

}
