#include "VectorException.h"

using namespace vec;

VectorException::VectorException(const std::string& message)
    : std::runtime_error(message)
{
}

VectorException::~VectorException() noexcept {
}
