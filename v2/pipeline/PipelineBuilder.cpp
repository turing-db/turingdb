#include "PipelineBuilder.h"

#include "processors/ScanNodesProcessor.h"
#include "processors/GetOutEdgesProcessor.h"

using namespace db::v2;

PipelineBuilder& PipelineBuilder::addScanNodes() {
    ScanNodesProcessor* proc = ScanNodesProcessor::create(_pipeline);
    _pendingOutput = &proc->outNodeIDs();
    return *this;
}

PipelineBuilder& PipelineBuilder::addGetOutEdges() {
    /*
    GetOutEdgesProcessor* proc = GetOutEdgesProcessor::create(_pipeline);
    _pendingOutput = &proc->outEdges();
    */
    return *this;
}
