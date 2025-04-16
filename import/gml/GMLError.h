#pragma once

#include <spdlog/fmt/bundled/format.h>

#include "BasicResult.h"

namespace db {

class GMLError {
public:
    template <typename... T>
    GMLError(size_t line, fmt::format_string<T...> fmt, T&&... args)
        : _line(line),
        _msg(fmt::format("GML Error at line {}: {}",
                           line,
                           fmt::format(fmt, std::forward<T>(args)...)))
    {
    }

    [[nodiscard]] size_t getLine() const { return _line; }
    [[nodiscard]] std::string_view getMessage() const { return _msg; }

    template <typename... T>
    static BadResult<GMLError> result(size_t line, fmt::format_string<T...> fmt, T&&... args) {
        return BadResult(GMLError(line, fmt, std::forward<T>(args)...));
    }

private:
    size_t _line {0};
    std::string _msg;
};

using GMLParseResult = BasicResult<void, GMLError>;

}

