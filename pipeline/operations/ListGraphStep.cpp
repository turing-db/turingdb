#include "ListGraphStep.h"

#include <sstream>

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

void ListGraphStep::describe(std::string& descr) const {
    std::stringstream ss;
    ss << "ListGraphStep";
    ss << " graphNames={";
    for (const auto& name : *_graphNames) {
        ss << name << " ";
    }
    ss << "}";
    descr = ss.str();
}
