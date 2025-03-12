#pragma once

#include <string>

#include "TuringException.h"

namespace db {

class PipelineException : public TuringException {
public:
    explicit PipelineException(std::string&& msg);
    ~PipelineException() noexcept override;
};

} 
