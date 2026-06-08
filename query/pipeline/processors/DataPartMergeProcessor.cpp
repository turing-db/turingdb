#include "DataPartMergeProcessor.h"

#include <spdlog/spdlog.h>
#include <spdlog/fmt/fmt.h>

#include "ExecutionContext.h"
#include "SystemManager.h"
#include "Graph.h"
#include "PipelineException.h"

#include "Profiler.h"
#include "BioAssert.h"

using namespace db;

DataPartMergeProcessor::DataPartMergeProcessor()
{
}

DataPartMergeProcessor::~DataPartMergeProcessor() {
}

std::string DataPartMergeProcessor::describe() const {
    return fmt::format("DataPartMergeProcessor @={}", fmt::ptr(this));
}

DataPartMergeProcessor* DataPartMergeProcessor::create(PipelineV2* pipeline) {
    DataPartMergeProcessor* proc = new DataPartMergeProcessor();

    PipelineOutputPort* output = PipelineOutputPort::create(pipeline, proc);
    proc->_output.setPort(output);
    proc->addOutput(output);

    proc->postCreate(pipeline);

    return proc;
}

void DataPartMergeProcessor::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;
    markAsPrepared();
}

void DataPartMergeProcessor::reset() {
    markAsReset();
}

void DataPartMergeProcessor::execute() {
    Profile profile("DataPartMergeProcessor::execute");

    SystemAccessor* system = _ctxt->getSystemAccessor();
    bioassert(system, "DataPartMergeProcessor: System accessor must be set");
    bioassert(!_ctxt->getGraphName().empty(), "DataPartMergeProcessor: Graph name must be set");

    const std::string graphName(_ctxt->getGraphName());
    Graph* graph = system->getGraph(graphName);
    bioassert(graph, "DataPartMergeProcessor: Graph must exist");

    JobSystem* jobSystem = _ctxt->getJobSystem();
    bioassert(jobSystem, "DataPartMergeProcessor: Job system must be set");

    if (auto res = system->mergeDataParts(graph, *jobSystem); !res) {
        throw PipelineException(fmt::format("DataPartMergeProcessor: Failed to merge data parts: {}",
                                            res.error().fmtMessage()));
    }

    spdlog::info("Data parts merged successfully for graph '{}'", graph->getName());

    _output.getPort()->writeData();
    finish();
}
