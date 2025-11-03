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

using namespace db::v2;

GetOutEdgesProcessor::GetOutEdgesProcessor()
{
}

GetOutEdgesProcessor::~GetOutEdgesProcessor() {
}

GetOutEdgesProcessor* GetOutEdgesProcessor::create(PipelineV2* pipeline) {
    GetOutEdgesProcessor* getOutEdges = new GetOutEdgesProcessor();

    PipelineInputPort* inNodeIDs = PipelineInputPort::create(pipeline, getOutEdges);
    PipelineOutputPort* outEdges = PipelineOutputPort::create(pipeline, getOutEdges);

    getOutEdges->_inNodeIDs.setPort(inNodeIDs);
    getOutEdges->_outEdges.setPort(outEdges);

    getOutEdges->addInput(inNodeIDs);
    getOutEdges->addOutput(outEdges);
    getOutEdges->postCreate(pipeline);
    return getOutEdges;
}

void GetOutEdgesProcessor::prepare(ExecutionContext* ctxt) {
    ColumnNodeIDs* nodeIDs = _inNodeIDs.getNodeIDs();
    ColumnVector<size_t>* indices = _outEdges.getIndices();
    ColumnEdgeIDs* edgeIDs = _outEdges.getEdgeIDs();
    ColumnNodeIDs* targetNodes = _outEdges.getTargetNodes();
    ColumnEdgeTypes* edgeTypes = _outEdges.getEdgeTypes();
    
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

    _outEdges.getPort()->writeData();
}
