#pragma once

#include <string>

#include "TuringException.h"

namespace vec {

class VectorException : public TuringException {
public:
    explicit VectorException(std::string&& message);
    ~VectorException() noexcept override;
};

}
