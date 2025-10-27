#include "DataPartMergeStep.h"

#include <sstream>

#include "ExecutionContext.h"
#include "PipelineException.h"
#include "SystemManager.h"
#include "Graph.h"

using namespace db;

void DataPartMergeStep::prepare(ExecutionContext* ctxt) {
    _systemManager = ctxt->getSystemManager();
    _jobSystem = ctxt->getJobSystem();
    _graph = _systemManager->getGraph(std::string(ctxt->getGraphName()));
}

void DataPartMergeStep::execute() {
    _systemManager->mergeDataParts(_graph, *_jobSystem);
    if (auto res = _systemManager->dumpGraph(_graph->getName()); !res) {
        throw PipelineException(fmt::format("CommitStep: Failed to commit: {}", res.error().fmtMessage()));
    }
}

void DataPartMergeStep::describe(std::string& descr) const {
    std::stringstream ss;
    ss << "DataPartMergeStep";
    descr.assign(ss.str());
}
