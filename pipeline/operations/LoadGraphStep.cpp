#include "LoadGraphStep.h"

#include <sstream>

#include "ExecutionContext.h"
#include "Profiler.h"
#include "SystemManager.h"
#include "PipelineException.h"

using namespace db;

LoadGraphStep::LoadGraphStep(const fs::Path& filePath, const std::string& graphName)
    : _filePath(filePath),
    _graphName(graphName)
{
}

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
        fmt::print("Loading graph {} from file {}\n", _graphName, _filePath.get());
        res = _sysMan->importGraph(_graphName, _filePath, *_jobSystem);
    } else {
        fmt::print("Loading turing graph {}\n", _graphName);
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
