#include "ProcedureTypeVector.h"

using namespace db;

ProcedureTypeVector::ProcedureTypeVector()
{
}

ProcedureTypeVector::ProcedureTypeVector(std::initializer_list<NamedProcedureType> values)
    : _values(values)
{
    for (const NamedProcedureType& arg : _values) {
        _requiredCount += !arg._optional;
    }
}

ProcedureTypeVector::~ProcedureTypeVector() {
}
