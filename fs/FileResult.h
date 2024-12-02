#pragma once

#include "BasicResult.h"

#include "spdlog/fmt/bundled/format.h"

namespace fs {

class FileError {
public:
    template <typename... T>
    explicit FileError(std::string_view path, fmt::format_string<T...> fmt, T&&... args)
        : _path(path),
          _msg(fmt::format("File error '{}': {}", path, fmt::format(fmt, std::forward<T>(args)...)))
    {
    }

    [[nodiscard]] std::string_view getPath() const { return _path; }
    [[nodiscard]] std::string_view getMessage() const { return _msg; }

    template <typename... T>
    static BadResult<FileError> result(std::string_view path, fmt::format_string<T...> fmt, T&&... args) {
        return BadResult(FileError(path, fmt, std::forward<T>(args)...));
    }

private:
    std::string_view _path;
    std::string _msg;
};

template <typename T>
using FileResult = BasicResult<T, class FileError>;

}
