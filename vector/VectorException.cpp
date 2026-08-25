#include "VectorException.h"

using namespace vec;

VectorException::VectorException(std::string&& message)
    : TuringException(std::move(message))
{
}

VectorException::~VectorException() noexcept {
}
