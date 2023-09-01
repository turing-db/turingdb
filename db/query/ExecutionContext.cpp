#include "ExecutionContext.h"

using namespace db::query;

ExecutionContext::ExecutionContext(InterpreterContext* interpCtxt)
    : _interpCtxt(interpCtxt)
{
}

ExecutionContext::~ExecutionContext() {
}
