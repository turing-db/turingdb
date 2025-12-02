#include "PipelineGenerator.h"

#include "PipelineBranches.h"

#include "BranchGenerator.h"

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
    _callback(callback)
{
}

PipelineGenerator::~PipelineGenerator() {
}

void PipelineGenerator::generate() {
    // Get branches 
    PipelineBranches pipelineBranches;
    pipelineBranches.generate(_graph);

    // Topological sort of the branches
    std::vector<PipelineBranch*> sortedBranches;
    pipelineBranches.topologicalSort(sortedBranches);

    BranchGenerator branchGen(_mem,
                              _sourceManager,
                              _graph,
                              _view,
                              _pipeline,
                              _callback);
    for (PipelineBranch* branch : sortedBranches) {
        branchGen.translateBranch(branch);
    }
}
