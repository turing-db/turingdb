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

    PipelinePort* inNodeIDs = PipelinePort::create(pipeline, getOutEdges);
    PipelinePort* outIndices = PipelinePort::create(pipeline, getOutEdges);
    PipelinePort* outEdgeIDs = PipelinePort::create(pipeline, getOutEdges);
    PipelinePort* outTargetNodes = PipelinePort::create(pipeline, getOutEdges);
    PipelinePort* outEdgeTypes = PipelinePort::create(pipeline, getOutEdges);

    getOutEdges->_inNodeIDs = inNodeIDs;
    getOutEdges->_outIndices = outIndices;
    getOutEdges->_outEdgeIDs = outEdgeIDs;
    getOutEdges->_outTargetNodes = outTargetNodes;
    getOutEdges->_outEdgeTypes = outEdgeTypes;

    getOutEdges->addInput(inNodeIDs);
    getOutEdges->addOutput(outIndices);
    getOutEdges->addOutput(outEdgeIDs);
    getOutEdges->addOutput(outTargetNodes);
    getOutEdges->addOutput(outEdgeTypes);

    getOutEdges->postCreate(pipeline);
    return getOutEdges;
}

void GetOutEdgesProcessor::prepare(ExecutionContext* ctxt) {
    PipelineBuffer* nodeIDsBuffer = _inNodeIDs->getBuffer();
    if (!nodeIDsBuffer) {
        throw PipelineException("GetOutEdgesProcessor: Node IDs port not connected");
    }

    ColumnNodeIDs* nodeIDs = dynamic_cast<ColumnNodeIDs*>(nodeIDsBuffer->getBlock()[0]);
    
    _it = std::make_unique<GetOutEdgesChunkWriter>(ctxt->getGraphView(), nodeIDs);

    PipelineBuffer* indicesBuffer = _outIndices->getBuffer();
    if (indicesBuffer) {
        ColumnVector<size_t>* indices = dynamic_cast<ColumnVector<size_t>*>(indicesBuffer->getBlock()[0]);
        _it->setIndices(indices);
    }

    PipelineBuffer* edgeIDsBuffer = _outEdgeIDs->getBuffer();
    if (edgeIDsBuffer) {
        ColumnEdgeIDs* edgeIDs = dynamic_cast<ColumnEdgeIDs*>(edgeIDsBuffer->getBlock()[0]);
        _it->setEdgeIDs(edgeIDs);
    }

    PipelineBuffer* targetNodesBuffer = _outTargetNodes->getBuffer();
    if (targetNodesBuffer) {
        ColumnNodeIDs* targetNodes = dynamic_cast<ColumnNodeIDs*>(targetNodesBuffer->getBlock()[0]);
        _it->setTgtIDs(targetNodes);
    }

    PipelineBuffer* edgeTypesBuffer = _outEdgeTypes->getBuffer();
    if (edgeTypesBuffer) {
        ColumnEdgeTypes* edgeTypes = dynamic_cast<ColumnEdgeTypes*>(edgeTypesBuffer->getBlock()[0]);
        _it->setEdgeTypes(edgeTypes);
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
