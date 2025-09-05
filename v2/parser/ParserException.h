#pragma once

#include <string>

#include "TuringException.h"

namespace db::v2 {

class ParserException : public TuringException {
public:
    ParserException(const ParserException&) = default;
    ParserException(ParserException&&) = default;
    ParserException& operator=(const ParserException&) = default;
    ParserException& operator=(ParserException&&) = default;

    explicit ParserException(std::string&& msg);
    ~ParserException() noexcept override;
};

}
