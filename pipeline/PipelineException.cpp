#include "PipelineException.h"

using namespace db;

PipelineException::PipelineException(const std::string& msg)
    : _msg(msg)
{
}

PipelineException::~PipelineException() noexcept {
}

const char* PipelineException::what() const noexcept {
    return _msg.c_str();
} 
