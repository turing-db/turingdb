#include "PipelineGenerator.h"

#include "PipelineV2.h"
#include "PlanGraph.h"

using namespace db;

PipelineGenerator::PipelineGenerator(const PlanGraph& graph)
    : _graph(graph)
{
}

PipelineGenerator::~PipelineGenerator() {
}

std::unique_ptr<PipelineV2> PipelineGenerator::generate() {
    return nullptr;
}
