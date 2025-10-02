#include "LoadGraphStep.h"

#include <sstream>

#include "ExecutionContext.h"
#include "Profiler.h"
#include "SystemManager.h"
#include "PipelineException.h"

using namespace db;

LoadGraphStep::LoadGraphStep(const std::string& graphName) 
    : _graphName(graphName)
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

    if (!_filePath.empty()) {
        res = _sysMan->importGraph(_graphName, _filePath, *_jobSystem);
    } else {
        res = _sysMan->loadGraph(_graphName);
    }

    if (!res) {
        throw PipelineException("Failed to load graph " + _graphName);
    }
}

void LoadGraphStep::describe(std::string& descr) const {
    std::stringstream ss;
    ss << "LoadGraphStep";
    ss << " fileName=" << _filePath.get();
    ss << " graphName=" << _graphName;
    descr.assign(ss.str());
}
