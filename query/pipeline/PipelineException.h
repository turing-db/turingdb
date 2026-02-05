#pragma once

#include <string>

#include "TuringException.h"

namespace db {

class PipelineException : public TuringException {
public:
    PipelineException(const PipelineException&) = default;
    PipelineException(PipelineException&&) = default;
    PipelineException& operator=(const PipelineException&) = default;
    PipelineException& operator=(PipelineException&&) = default;

    explicit PipelineException(std::string&& msg);
    ~PipelineException() noexcept override;
};

}
