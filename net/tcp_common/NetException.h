#pragma once

#include "TuringException.h"

/**
 * @brief Exception class to denote a network failure usually errors returned from the
 * socket API
 * @detail The error message is the corresponding string error message from strerror()
 */
class NetException : public TuringException {
public:
    NetException();
    explicit NetException(std::string&& msg);
    NetException(const NetException&) = default;
    NetException(NetException&&) = default;
    NetException& operator=(const NetException&) = default;
    NetException& operator=(NetException&&) = default;
    ~NetException() noexcept override;

    const char* what() const noexcept override;
};
