#include "FunctionSignature.h"

using namespace db;

FunctionSignature::FunctionSignature(std::string_view fullName)
    : _fullName(fullName)
{
}

FunctionSignature::~FunctionSignature() {
}
