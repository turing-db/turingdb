#include "GetEdgesProcessor.h"

#include "PipelineV2.h"
#include "PipelinePort.h"

#include "iterators/GetEdgesIterator.h"

#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "columns/ColumnEdgeTypes.h"
#include "dataframe/NamedColumn.h"

#include "PipelineBuffer.h"
#include "ExecutionContext.h"

using namespace db::v2;

GetEdgesProcessor::GetEdgesProcessor()
{
}

GetEdgesProcessor::~GetEdgesProcessor() {
}

GetEdgesProcessor* GetEdgesProcessor::create(PipelineV2* pipeline) {
    GetEdgesProcessor* getInEdges = new GetEdgesProcessor();

    PipelineInputPort* inNodeIDs = PipelineInputPort::create(pipeline, getInEdges);
    PipelineOutputPort* outEdges = PipelineOutputPort::create(pipeline, getInEdges);

    getInEdges->_inNodeIDs.setPort(inNodeIDs);
    getInEdges->_outEdges.setPort(outEdges);

    getInEdges->addInput(inNodeIDs);
    getInEdges->addOutput(outEdges);
    getInEdges->postCreate(pipeline);
    return getInEdges;
}

void GetEdgesProcessor::prepare(ExecutionContext* ctxt) {
    ColumnNodeIDs* nodeIDs = dynamic_cast<ColumnNodeIDs*>(_inNodeIDs.getNodeIDs()->getColumn());
    ColumnVector<size_t>* indices = dynamic_cast<ColumnVector<size_t>*>(_outEdges.getIndices()->getColumn());
    ColumnEdgeIDs* edgeIDs = dynamic_cast<ColumnEdgeIDs*>(_outEdges.getEdgeIDs()->getColumn());
    ColumnNodeIDs* otherNodes = dynamic_cast<ColumnNodeIDs*>(_outEdges.getNeighbourNodes()->getColumn());
    ColumnEdgeTypes* edgeTypes = dynamic_cast<ColumnEdgeTypes*>(_outEdges.getEdgeTypes()->getColumn());
    
    _it = std::make_unique<GetEdgesChunkWriter>(ctxt->getGraphView(), nodeIDs);
    _it->setIndices(indices);
    _it->setEdgeIDs(edgeIDs);
    _it->setOtherIDs(otherNodes);
    _it->setEdgeTypes(edgeTypes);
    _chunkSize = ctxt->getChunkSize();

    markAsPrepared();
}

void GetEdgesProcessor::reset() {
    _it->reset();
    markAsReset();
}

void GetEdgesProcessor::execute() {
    _it->fill(_chunkSize);

    if (!_it->isValid()) {
        _inNodeIDs.getPort()->consume();
        finish();
    }

    _outEdges.getPort()->writeData();
}
