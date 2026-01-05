#include "AnalyzeException.h"

using namespace db;

AnalyzeException::AnalyzeException(std::string&& msg)
    : CompilerException(std::move(msg))
{
}

AnalyzeException::~AnalyzeException() noexcept {
}

