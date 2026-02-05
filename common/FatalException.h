#pragma once

#include "TuringException.h"

/**
 * @brief Exception class to denote an internal logic error; a bad state from which
 * execution should not continue, but due to programmer error as opposed to user error.
 * @detail May be used as a release-friendly `assert`; which can hault current execution
 * path without causing a crash of the DB server
 */
class FatalException : public TuringException {
public:
    FatalException();
    explicit FatalException(std::string&& msg);
    FatalException(const FatalException&) = default;
    FatalException(FatalException&&) = default;
    FatalException& operator=(const FatalException&) = default;
    FatalException& operator=(FatalException&&) = default;
    ~FatalException() noexcept override = default;

    const char* what() const noexcept override;

protected:
    std::string _msg;
};
