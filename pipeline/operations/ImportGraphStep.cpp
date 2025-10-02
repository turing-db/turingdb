#include "ImportGraphStep.h"

#include <sstream>

#include "ExecutionContext.h"
#include "Profiler.h"
#include "SystemManager.h"
#include "PipelineException.h"

using namespace db;

ImportGraphStep::ImportGraphStep(const fs::Path& filePath, const std::string& graphName)
    : _filePath(filePath),
    _graphName(graphName)
{
}

ImportGraphStep::~ImportGraphStep() {
}

void ImportGraphStep::prepare(ExecutionContext* ctxt) {
    _sysMan = ctxt->getSystemManager();
    _jobSystem = ctxt->getJobSystem();
}

void ImportGraphStep::execute() {
    Profile profile {"ImportGraphStep::execute"};

    if (_filePath.empty() || _graphName.empty()) {
        throw PipelineException("Invalid graph name or file path");
    }

    const bool res = _sysMan->importGraph(_graphName, _filePath, *_jobSystem);

    if (!res) {
        throw PipelineException("Failed to load graph " + _graphName);
    }
}

void ImportGraphStep::describe(std::string& descr) const {
    std::stringstream ss;
    ss << "ImportGraphStep";
    ss << " fileName=" << _filePath.get();
    ss << " graphName=" << _graphName;
    descr.assign(ss.str());
}
