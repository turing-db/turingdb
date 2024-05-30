#pragma once

#include <exception>
#include <spdlog/fmt/bundled/format.h>
#include <string>

class TuringException : public std::exception {
public:
    TuringException() = default;
    explicit TuringException(std::string&& msg);
    TuringException(const TuringException&) = default;
    TuringException(TuringException&&) = default;
    TuringException& operator=(const TuringException&) = default;
    TuringException& operator=(TuringException&&) = default;
    ~TuringException() noexcept override = default;

    const char* what() const noexcept override;

protected:
    std::string _msg {"Something went wrong"};
};

[[noreturn]] void panic();
[[noreturn]] void panic(std::string&& msg);

template <typename... Args>
[[noreturn]] inline void panic(fmt::format_string<Args...>&& fmt, Args&&... args) {
    throw TuringException(fmt::format(fmt, std::forward<Args>(args)...));
}
