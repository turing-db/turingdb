#include "ScanNodeProcessor.h"

#include "PipelineV2.h"
#include "iterators/ScanNodesIterator.h"
#include "iterators/ChunkConfig.h"
#include "columns/ColumnIDs.h"
#include "PipelineBuffer.h"
#include "ExecutionContext.h"

using namespace db::v2;

ScanNodeProcessor::ScanNodeProcessor()
{
}

ScanNodeProcessor::~ScanNodeProcessor() {
}

ScanNodeProcessor* ScanNodeProcessor::create(PipelineV2* pipeline) {
    ScanNodeProcessor* processor = new ScanNodeProcessor();

    PipelineBuffer* outNodeIDs = PipelineBuffer::create(pipeline);
    processor->_outNodeIDs = outNodeIDs;
    processor->addOutput(outNodeIDs);

    processor->postCreate(pipeline);
    return processor;
}

void ScanNodeProcessor::prepare(ExecutionContext* ctxt) {
    ColumnNodeIDs* nodeIDs = dynamic_cast<ColumnNodeIDs*>(_outNodeIDs->getColumn());

    _it = std::make_unique<ScanNodesChunkWriter>(ctxt->getGraphView());
    _it->setNodeIDs(nodeIDs);
}

void ScanNodeProcessor::reset() {
    _it->reset();
}

void ScanNodeProcessor::execute() {
    _it->fill(ChunkConfig::CHUNK_SIZE);

    if (!_it->isValid()) {
        _finished = true;
    }

    _outNodeIDs->writeData();
}
