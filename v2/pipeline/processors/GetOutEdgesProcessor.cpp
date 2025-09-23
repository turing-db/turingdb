#include "GetOutEdgesProcessor.h"

#include "PipelineV2.h"
#include "iterators/GetOutEdgesIterator.h"
#include "iterators/ChunkConfig.h"
#include "PipelineBuffer.h"
#include "ExecutionContext.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "columns/ColumnEdgeTypes.h"

using namespace db::v2;

GetOutEdgesProcessor::GetOutEdgesProcessor()
{
}

GetOutEdgesProcessor::~GetOutEdgesProcessor() {
}

GetOutEdgesProcessor* GetOutEdgesProcessor::create(PipelineV2* pipeline) {
    GetOutEdgesProcessor* processor = new GetOutEdgesProcessor();

    PipelineBuffer* inNodeIDs = PipelineBuffer::create(pipeline);
    PipelineBuffer* outIndices = PipelineBuffer::create(pipeline);
    PipelineBuffer* outEdgeIDs = PipelineBuffer::create(pipeline);
    PipelineBuffer* outTargetNodes = PipelineBuffer::create(pipeline);
    PipelineBuffer* outEdgeTypes = PipelineBuffer::create(pipeline);

    processor->_inNodeIDs = inNodeIDs;
    processor->_outIndices = outIndices;
    processor->_outEdgeIDs = outEdgeIDs;
    processor->_outTargetNodes = outTargetNodes;
    processor->_outEdgeTypes = outEdgeTypes;

    processor->addInput(inNodeIDs);
    processor->addOutput(outIndices);
    processor->addOutput(outEdgeIDs);
    processor->addOutput(outTargetNodes);
    processor->addOutput(outEdgeTypes);

    processor->postCreate(pipeline);
    return processor;
}

void GetOutEdgesProcessor::prepare(ExecutionContext* ctxt) {
    ColumnNodeIDs* nodeIDs = dynamic_cast<ColumnNodeIDs*>(_inNodeIDs->getBlock()[0]);
    
    _it = std::make_unique<GetOutEdgesChunkWriter>(ctxt->getGraphView(), nodeIDs);

    Column* indices = _outIndices->getBlock()[0];
    if (indices) {
        _it->setIndices(dynamic_cast<ColumnVector<size_t>*>(indices));
    }

    Column* edgeIDs = _outEdgeIDs->getBlock()[0];
    if (edgeIDs) {
        _it->setEdgeIDs(dynamic_cast<ColumnEdgeIDs*>(edgeIDs));
    }

    Column* targetNodes = _outTargetNodes->getBlock()[0];
    if (targetNodes) {
        _it->setTgtIDs(dynamic_cast<ColumnNodeIDs*>(targetNodes));
    }

    Column* edgeTypes = _outEdgeTypes->getBlock()[0];
    if (edgeTypes) {
        _it->setEdgeTypes(dynamic_cast<ColumnEdgeTypes*>(edgeTypes));
    }
}

void GetOutEdgesProcessor::reset() {
    _it->reset();
}

void GetOutEdgesProcessor::execute() {
    _inNodeIDs->consume();
    _it->fill(ChunkConfig::CHUNK_SIZE);

    if (!_it->isValid()) {
        _finished = true;
    }

    _outIndices->writeData();
    _outEdgeIDs->writeData();
    _outTargetNodes->writeData();
    _outEdgeTypes->writeData();
}
