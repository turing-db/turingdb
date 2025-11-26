#include "PipelineGenerator.h"

#include "PlanBranches.h"

using namespace db::v2;

PipelineGenerator::PipelineGenerator(LocalMemory* mem,
                                     SourceManager* srcMan,
                                     const PlanGraph* graph,
                                     const GraphView& view,
                                     PipelineV2* pipeline,
                                     const QueryCallbackV2& callback)
    : _mem(mem),
    _sourceManager(srcMan),
    _graph(graph),
    _view(view),
    _pipeline(pipeline),
    _callback(callback),
    _builder(mem, pipeline)
{
}

PipelineGenerator::~PipelineGenerator() {
}

void PipelineGenerator::generate() {
    // Get branches of the plan graph
    PlanBranches planBranches;
    planBranches.generate(_graph);

    // Topological sort the branches
    std::vector<PlanBranch*> sortedBranches;
    planBranches.topologicalSort(sortedBranches);
}
