#include "AnalyzeException.h"

using namespace db;

AnalyzeException::AnalyzeException(std::string&& msg)
    : TuringException(std::move(msg))
{
}

AnalyzeException::~AnalyzeException() noexcept {
}
