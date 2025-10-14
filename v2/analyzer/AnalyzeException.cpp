#include "AnalyzeException.h"

using namespace db::v2;

AnalyzeException::AnalyzeException(std::string&& msg)
    : CompilerException(std::move(msg))
{
}

AnalyzeException::~AnalyzeException() noexcept {
}

