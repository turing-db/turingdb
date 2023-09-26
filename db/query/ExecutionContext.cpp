#include "ExecutionContext.h"

using namespace db;

ExecutionContext::ExecutionContext(InterpreterContext* interpCtxt)
    : _interpCtxt(interpCtxt)
{
}

ExecutionContext::~ExecutionContext() {
}
