#include "VersionControlException.h"

using namespace db;

VersionControlException::VersionControlException(std::string&& msg)
    : TuringException(std::move(msg))
{
}

VersionControlException::~VersionControlException() noexcept {
}
