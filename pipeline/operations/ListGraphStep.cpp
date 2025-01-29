#include "ListGraphStep.h"

#include "ExecutionContext.h"
#include "SystemManager.h"

using namespace db;

ListGraphStep::ListGraphStep(ColumnVector<std::string_view>* graphNames)
    : _graphNames(graphNames)
{
}

void ListGraphStep::prepare(ExecutionContext* ctxt) {
    _sysMan = ctxt->getSystemManager();
}

void ListGraphStep::execute() {
    _graphNames->clear();
    _sysMan->listGraphs(_graphNames->getRaw());
}

