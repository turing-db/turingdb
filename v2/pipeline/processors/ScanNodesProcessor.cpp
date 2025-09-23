#include "ScanNodesProcessor.h"

#include "PipelineV2.h"
#include "iterators/ScanNodesIterator.h"
#include "iterators/ChunkConfig.h"
#include "columns/ColumnIDs.h"
#include "PipelineBuffer.h"
#include "ExecutionContext.h"

using namespace db::v2;

ScanNodesProcessor::ScanNodesProcessor()
{
}

ScanNodesProcessor::~ScanNodesProcessor() {
}

ScanNodesProcessor* ScanNodesProcessor::create(PipelineV2* pipeline) {
    ScanNodesProcessor* processor = new ScanNodesProcessor();

    PipelineBuffer* outNodeIDs = PipelineBuffer::create(pipeline);
    processor->_outNodeIDs = outNodeIDs;
    processor->addOutput(outNodeIDs);

    processor->postCreate(pipeline);
    return processor;
}

void ScanNodesProcessor::prepare(ExecutionContext* ctxt) {
    ColumnNodeIDs* nodeIDs = dynamic_cast<ColumnNodeIDs*>(_outNodeIDs->getBlock()[0]);

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
