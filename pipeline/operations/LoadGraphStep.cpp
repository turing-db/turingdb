#include "LoadGraphStep.h"

#include <sstream>

#include "ExecutionContext.h"
#include "Profiler.h"
#include "SystemManager.h"
#include "PipelineException.h"

using namespace db;

LoadGraphStep::LoadGraphStep(const std::string& fileName, const std::string& graphName)
    : _fileName(fileName),
    _graphName(graphName)
{
}

LoadGraphStep::LoadGraphStep(const std::string& graphName) 
    : _fileName(graphName)
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

    if (!_fileName.empty()) {
        fmt::print("Loading graph {} from file {}\n", _graphName, _fileName);
        res = _sysMan->loadGraphFromFile(_graphName, _fileName, *_jobSystem);
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
    ss << " fileName=" << _fileName;
    ss << " graphName=" << _graphName;
    descr.assign(ss.str());
}
