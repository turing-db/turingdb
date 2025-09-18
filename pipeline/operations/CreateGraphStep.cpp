#include "CreateGraphStep.h"

#include <sstream>

#include "ExecutionContext.h"
#include "PipelineException.h"
#include "Profiler.h"
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
    Profile profile {"CreateGraphStep::execute"};

    if (!_sysMan->createAndDumpGraph(_graphName)) {
        throw PipelineException("Failed To Create Graph");
    }
}

void CreateGraphStep::describe(std::string& descr) const {
    std::stringstream ss;
    ss << "CreateGraphStep";
    ss << " graphName=" << _graphName;
    descr.assign(ss.str());
}
