#include "GetOutEdgesProcessor.h"

#include "PipelineV2.h"
#include "PipelinePort.h"
#include "iterators/GetOutEdgesIterator.h"
#include "iterators/ChunkConfig.h"
#include "PipelineBuffer.h"
#include "ExecutionContext.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "columns/ColumnEdgeTypes.h"

#include "PipelineException.h"

using namespace db::v2;

GetOutEdgesProcessor::GetOutEdgesProcessor()
{
}

GetOutEdgesProcessor::~GetOutEdgesProcessor() {
}

std::string_view GetOutEdgesProcessor::getName() const {
    return "GetOutEdgesProcessor";
}

GetOutEdgesProcessor* GetOutEdgesProcessor::create(PipelineV2* pipeline) {
    GetOutEdgesProcessor* getOutEdges = new GetOutEdgesProcessor();

    PipelineInputPort* input = PipelineInputPort::create(pipeline, getOutEdges);
    getOutEdges->_inNodeIDs.setPort(input);

    PipelineOutputPort* output = PipelineOutputPort::create(pipeline, getOutEdges);
    getOutEdges->_outEdges.setPort(output);

    getOutEdges->postCreate(pipeline);
    return getOutEdges;
}

void GetOutEdgesProcessor::prepare(ExecutionContext* ctxt) {
    ColumnNodeIDs* nodeIDs = dynamic_cast<ColumnNodeIDs*>(nodeIDsBuffer->getBlock()[0]);
    
    _it = std::make_unique<GetOutEdgesChunkWriter>(ctxt->getGraphView(), nodeIDs);

    ColumnVector<size_t>* indices = dynamic_cast<ColumnVector<size_t>*>(_outIndices->getBuffer()->getBlock()[0]);
    _it->setIndices(indices);

    if (_outEdgeIDs->isConnected()) {
        ColumnEdgeIDs* edgeIDs = dynamic_cast<ColumnEdgeIDs*>(_outEdgeIDs->getBuffer()->getBlock()[0]);
        _it->setEdgeIDs(edgeIDs);
    }

    if (_outTargetNodes->isConnected()) {
        ColumnNodeIDs* targetNodes = dynamic_cast<ColumnNodeIDs*>(_outTargetNodes->getBuffer()->getBlock()[0]);
        _it->setTgtIDs(targetNodes);
    }

    if (_outEdgeTypes->isConnected()) {
        ColumnEdgeTypes* edgeTypes = dynamic_cast<ColumnEdgeTypes*>(_outEdgeTypes->getBuffer()->getBlock()[0]);
        _it->setEdgeTypes(edgeTypes);
    }

    markAsPrepared();
}

void GetOutEdgesProcessor::reset() {
    _it->reset();
    markAsReset();
}

void GetOutEdgesProcessor::execute() {
    _inNodeIDs->consume();
    _it->fill(ChunkConfig::CHUNK_SIZE);

    if (!_it->isValid()) {
        finish();
    }

    _outIndices->writeData();
    _outEdgeIDs->writeData();
    _outTargetNodes->writeData();
    _outEdgeTypes->writeData();
    _outIndices->writeData();
}
