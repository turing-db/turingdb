#include "GetOutEdgesProcessor.h"

#include "PipelineV2.h"
#include "PipelinePort.h"

#include "iterators/GetOutEdgesIterator.h"

#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "columns/ColumnEdgeTypes.h"
#include "dataframe/NamedColumn.h"

#include "PipelineBuffer.h"
#include "ExecutionContext.h"

using namespace db::v2;

GetOutEdgesProcessor::GetOutEdgesProcessor()
{
}

GetOutEdgesProcessor::~GetOutEdgesProcessor() {
}

std::string GetOutEdgesProcessor::describe() const {
    return fmt::format("GetOutEdgesProcessor @={}", fmt::ptr(this));
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
    _ctxt = ctxt;

    ColumnNodeIDs* nodeIDs = dynamic_cast<ColumnNodeIDs*>(_inNodeIDs.getNodeIDs()->getColumn());
    ColumnVector<size_t>* indices = dynamic_cast<ColumnVector<size_t>*>(_outEdges.getIndices()->getColumn());
    ColumnEdgeIDs* edgeIDs = dynamic_cast<ColumnEdgeIDs*>(_outEdges.getEdgeIDs()->getColumn());
    ColumnNodeIDs* targetNodes = dynamic_cast<ColumnNodeIDs*>(_outEdges.getOtherNodes()->getColumn());
    ColumnEdgeTypes* edgeTypes = dynamic_cast<ColumnEdgeTypes*>(_outEdges.getEdgeTypes()->getColumn());

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
    _it->fill(_ctxt->getChunkSize());

    if (!_it->isValid()) {
        _inNodeIDs.getPort()->consume();
        finish();
    }

    _outEdges.getPort()->writeData();
}
