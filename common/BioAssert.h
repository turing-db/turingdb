#pragma once

#include <spdlog/fmt/bundled/format.h>

void __bioAssertImpl(const char* file,
                     unsigned line,
                     const char* expr,
                     std::string&& msg);

template <typename... Args>
[[noreturn]] void __bioAssertWithLocation(const char* file,
                                          unsigned line,
                                          const char* expr,
                                          fmt::format_string<Args...>&& msg,
                                          Args&&... args) {
    __bioAssertImpl(file, line, expr, fmt::format(msg, std::forward<Args>(args)...));
    abort();
}

#define bioassert(C, msg, ...) ({ if (!(C)) { __bioAssertWithLocation(__FILE__, __LINE__, #C, msg __VA_OPT__(,) __VA_ARGS__); }})
