#include "ScanNodesProcessor.h"

#include "PipelineV2.h"
#include "iterators/ScanNodesIterator.h"
#include "iterators/ChunkConfig.h"
#include "columns/ColumnIDs.h"
#include "PipelinePort.h"
#include "ExecutionContext.h"

using namespace db::v2;

ScanNodesProcessor::ScanNodesProcessor()
{
}

ScanNodesProcessor::~ScanNodesProcessor() {
}

ScanNodesProcessor* ScanNodesProcessor::create(PipelineV2* pipeline) {
    ScanNodesProcessor* scanNodes = new ScanNodesProcessor();

    PipelineOutputPort* outNodeIDs = PipelineOutputPort::create(pipeline, scanNodes);
    scanNodes->_outNodeIDs = outNodeIDs;
    scanNodes->addOutput(outNodeIDs);

    scanNodes->postCreate(pipeline);
    return scanNodes;
}

void ScanNodesProcessor::prepare(ExecutionContext* ctxt) {
    ColumnNodeIDs* nodeIDs = dynamic_cast<ColumnNodeIDs*>(_outNodeIDs->getBuffer()->getBlock()[0]);

    _it = std::make_unique<ScanNodesChunkWriter>(ctxt->getGraphView());
    _it->setNodeIDs(nodeIDs);
}

void ScanNodesProcessor::reset() {
    _it->reset();
}

void ScanNodesProcessor::execute() {
    _it->fill(ChunkConfig::CHUNK_SIZE);

    if (!_it->isValid()) {
        _finished = true;
    }

    _outNodeIDs->writeData();
}
