#include "ImportException.h"

using namespace vec;

ImportException::ImportException(ImportErrorCode code, const std::string& message)
    : _e(code, message)
{
}

ImportException::~ImportException() noexcept {
}
