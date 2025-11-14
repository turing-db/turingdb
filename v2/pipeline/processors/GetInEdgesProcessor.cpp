#include "GetInEdgesProcessor.h"

#include "PipelineV2.h"
#include "PipelinePort.h"

#include "iterators/GetInEdgesIterator.h"

#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "columns/ColumnEdgeTypes.h"
#include "dataframe/NamedColumn.h"

#include "PipelineBuffer.h"
#include "ExecutionContext.h"

using namespace db::v2;

GetInEdgesProcessor::GetInEdgesProcessor()
{
}

GetInEdgesProcessor::~GetInEdgesProcessor() {
}

std::string GetInEdgesProcessor::describe() const {
    return fmt::format("GetInEdgesProcessor @={}", fmt::ptr(this));
}

GetInEdgesProcessor* GetInEdgesProcessor::create(PipelineV2* pipeline) {
    GetInEdgesProcessor* getInEdges = new GetInEdgesProcessor();

    PipelineInputPort* inNodeIDs = PipelineInputPort::create(pipeline, getInEdges);
    PipelineOutputPort* outEdges = PipelineOutputPort::create(pipeline, getInEdges);

    getInEdges->_inNodeIDs.setPort(inNodeIDs);
    getInEdges->_outEdges.setPort(outEdges);

    getInEdges->addInput(inNodeIDs);
    getInEdges->addOutput(outEdges);
    getInEdges->postCreate(pipeline);
    return getInEdges;
}

void GetInEdgesProcessor::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;

    ColumnNodeIDs* nodeIDs = dynamic_cast<ColumnNodeIDs*>(_inNodeIDs.getNodeIDs()->getColumn());
    ColumnVector<size_t>* indices = dynamic_cast<ColumnVector<size_t>*>(_outEdges.getIndices()->getColumn());
    ColumnEdgeIDs* edgeIDs = dynamic_cast<ColumnEdgeIDs*>(_outEdges.getEdgeIDs()->getColumn());
    ColumnNodeIDs* sourceNodes = dynamic_cast<ColumnNodeIDs*>(_outEdges.getOtherNodes()->getColumn());
    ColumnEdgeTypes* edgeTypes = dynamic_cast<ColumnEdgeTypes*>(_outEdges.getEdgeTypes()->getColumn());
    
    _it = std::make_unique<GetInEdgesChunkWriter>(ctxt->getGraphView(), nodeIDs);
    _it->setIndices(indices);
    _it->setEdgeIDs(edgeIDs);
    _it->setSrcIDs(sourceNodes);
    _it->setEdgeTypes(edgeTypes);

    markAsPrepared();
}

void GetInEdgesProcessor::reset() {
    _it->reset();
    markAsReset();
}

void GetInEdgesProcessor::execute() {
    if (_it->isValid()) {
        _it->fill(_ctxt->getChunkSize());
        _outEdges.getPort()->writeData();

        return;
    }

    // Iterator is invalid, just check if the output was consumed
    // if so, we can finish the processor and consume the input
    if (!_outEdges.getPort()->hasData()) {
        _inNodeIDs.getPort()->consume();
        finish();
    }
}
