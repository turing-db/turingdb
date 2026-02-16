#include "ProcedureTypeVector.h"

using namespace db;

ProcedureTypeVector::ProcedureTypeVector()
{
}

ProcedureTypeVector::ProcedureTypeVector(std::initializer_list<NamedProcedureType> values)
    : _values(values)
{
}

ProcedureTypeVector::~ProcedureTypeVector() {
}
