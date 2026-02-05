#pragma once

#include <string>
#include <spdlog/fmt/bundled/format.h>

#include "TuringException.h"

[[noreturn]] void panic();
[[noreturn]] void panic(std::string&& msg);

template <typename... Args>
[[noreturn]] inline void panic(fmt::format_string<Args...>&& fmt, Args&&... args) {
    throw TuringException(fmt::format(fmt, std::forward<Args>(args)...));
}

#define COMPILE_ERROR(msg) ([] <bool flag = false>{ static_assert(flag, msg); })()
