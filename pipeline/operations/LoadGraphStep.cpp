#include "LoadGraphStep.h"

#include <sstream>

#include "ExecutionContext.h"
#include "Profiler.h"
#include "SystemManager.h"
#include "PipelineException.h"

using namespace db;

LoadGraphStep::LoadGraphStep(const std::string& graphFileName, const std::string& graphName)
    :_graphFileName(graphFileName),
    _graphName(graphName)
{
}

LoadGraphStep::LoadGraphStep(const std::string& graphFileName) 
    :_graphFileName(graphFileName)
{
}

LoadGraphStep::~LoadGraphStep() {
}

void LoadGraphStep::prepare(ExecutionContext* ctxt) {
    _sysMan = ctxt->getSystemManager();
    _jobSystem = ctxt->getJobSystem();
}

void LoadGraphStep::execute() {
    Profile profile {"LoadGraphStep::execute"};

    bool res {false};

    if (!_graphName.empty()) {
        res = _sysMan->loadGraph(_graphName, _graphFileName, *_jobSystem);
    } else {
        res = _sysMan->loadGraph(_graphFileName, *_jobSystem);
    }

    if (!res) {
        throw PipelineException("Failed to load graph " + _graphName);
    }
}

void LoadGraphStep::describe(std::string& descr) const {
    std::stringstream ss;
    ss << "LoadGraphStep";
    ss << " graphFileName=" << _graphFileName;
    ss << " graphName=" << _graphName;
    descr.assign(ss.str());
}
