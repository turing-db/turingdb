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

GetOutEdgesProcessor* GetOutEdgesProcessor::create(PipelineV2* pipeline) {
    GetOutEdgesProcessor* getOutEdges = new GetOutEdgesProcessor();

    PipelineInputPort* inNodeIDs = PipelineInputPort::create(pipeline, getOutEdges);
    PipelineOutputPort* outIndices = PipelineOutputPort::create(pipeline, getOutEdges);
    PipelineOutputPort* outEdgeIDs = PipelineOutputPort::create(pipeline, getOutEdges);
    PipelineOutputPort* outTargetNodes = PipelineOutputPort::create(pipeline, getOutEdges);
    PipelineOutputPort* outEdgeTypes = PipelineOutputPort::create(pipeline, getOutEdges);

    getOutEdges->_inNodeIDs.setPort(inNodeIDs);
    getOutEdges->_outIndices.setPort(outIndices);
    getOutEdges->_outEdgeIDs.setPort(outEdgeIDs);
    getOutEdges->_outTargetNodes.setPort(outTargetNodes);
    getOutEdges->_outEdgeTypes.setPort(outEdgeTypes);

    getOutEdges->addInput(inNodeIDs);
    getOutEdges->addOutput(outIndices);
    getOutEdges->addOutput(outEdgeIDs);
    getOutEdges->addOutput(outTargetNodes);
    getOutEdges->addOutput(outEdgeTypes);

    getOutEdges->postCreate(pipeline);
    return getOutEdges;
}

void GetOutEdgesProcessor::prepare(ExecutionContext* ctxt) {
    ColumnNodeIDs* nodeIDs = dynamic_cast<ColumnNodeIDs*>(_inNodeIDs.getRawColumn());
    ColumnVector<size_t>* indices = dynamic_cast<ColumnVector<size_t>*>(_outIndices.getRawColumn());
    ColumnEdgeIDs* edgeIDs = dynamic_cast<ColumnEdgeIDs*>(_outEdgeIDs.getRawColumn());
    ColumnNodeIDs* targetNodes = dynamic_cast<ColumnNodeIDs*>(_outTargetNodes.getRawColumn());
    ColumnEdgeTypes* edgeTypes = dynamic_cast<ColumnEdgeTypes*>(_outEdgeTypes.getRawColumn());

    if (!nodeIDs || !indices || !edgeIDs || !targetNodes || !edgeTypes) {
        throw PipelineException("GetOutEdgesProcessor: Invalid columns");
    }
    
    _it = std::make_unique<GetOutEdgesChunkWriter>(ctxt->getGraphView(), nodeIDs);
    _it->setIndices(indices);
    _it->setEdgeIDs(edgeIDs);
    _it->setTgtIDs(targetNodes);
    _it->setEdgeTypes(edgeTypes);

    markAsPrepared();
}

void GetOutEdgesProcessor::reset() {
    _it->reset();
    markAsReset();
}

void GetOutEdgesProcessor::execute() {
    _inNodeIDs.getPort()->consume();
    _it->fill(ChunkConfig::CHUNK_SIZE);

    if (!_it->isValid()) {
        finish();
    }

    _outIndices.getPort()->writeData();
    _outEdgeIDs.getPort()->writeData();
    _outTargetNodes.getPort()->writeData();
    _outEdgeTypes.getPort()->writeData();
    _outIndices.getPort()->writeData();
}
