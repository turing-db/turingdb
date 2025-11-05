#pragma once

#include <string>
#include <exception>

class TuringException : public std::exception {
public:
    TuringException();
    explicit TuringException(std::string&& msg);
    TuringException(const TuringException&) = default;
    TuringException(TuringException&&) = default;
    TuringException& operator=(const TuringException&) = default;
    TuringException& operator=(TuringException&&) = default;
    ~TuringException() noexcept override = default;

    const char* what() const noexcept override;

protected:
    std::string _msg;
};
