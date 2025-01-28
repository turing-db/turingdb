#pragma once

#include <string>
#include <exception>

class TuringDBException : public std::exception {
public:
    TuringDBException();
    explicit TuringDBException(std::string&& msg);
    TuringDBException(const TuringDBException&) = default;
    TuringDBException(TuringDBException&&) = default;
    TuringDBException& operator=(const TuringDBException&) = default;
    TuringDBException& operator=(TuringDBException&&) = default;
    ~TuringDBException() noexcept override = default;

    const char* what() const noexcept override;

protected:
    std::string _msg;
};
