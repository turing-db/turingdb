#include "CreateGraphStep.h"

#include "ExecutionContext.h"
#include "SystemManager.h"

using namespace db;

CreateGraphStep::CreateGraphStep(const std::string& graphName)
    : _graphName(graphName)
{
}

CreateGraphStep::~CreateGraphStep() {
}

void CreateGraphStep::prepare(ExecutionContext* ctxt) {
    _sysMan = ctxt->getSystemManager();
}

void CreateGraphStep::execute() {
    _sysMan->createGraph(_graphName);
}
