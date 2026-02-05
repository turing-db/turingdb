#pragma once

#include <stdexcept>

namespace vec {

class VectorException : public std::runtime_error {
public:
    explicit VectorException(const std::string& message);
    ~VectorException() noexcept override;
};

}
