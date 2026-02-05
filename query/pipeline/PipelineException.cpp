#include "PipelineException.h"

using namespace db;

PipelineException::PipelineException(std::string&& msg)
    : TuringException(std::move(msg))
{
}

PipelineException::~PipelineException() noexcept {
}
