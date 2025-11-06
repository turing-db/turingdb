#pragma once

#include "TuringException.h"

namespace db {

class VersionControlException : public TuringException {
public:
    explicit VersionControlException(std::string&& msg);
    ~VersionControlException() noexcept override;
};

}
