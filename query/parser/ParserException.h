#pragma once

#include <string>

#include "TuringException.h"

namespace db {

class ParserException : public TuringException {
public:
    explicit ParserException(std::string&& msg);
    ~ParserException() noexcept override;
};

}
