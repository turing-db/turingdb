#include "PipelineBuilder.h"

#include "processors/ScanNodesProcessor.h"
#include "processors/GetOutEdgesProcessor.h"
#include "processors/MaterializeProcessor.h"
#include "processors/LambdaProcessor.h"
#include "processors/GetPropertiesProcessor.h"
#include "processors/SkipProcessor.h"
#include "processors/LimitProcessor.h"
#include "processors/CountProcessor.h"

#include "columns/ColumnEdgeTypes.h"

#include "PipelineException.h"

using namespace db::v2;
using namespace db;

namespace {

void duplicateDataframeShape(LocalMemory* mem,
                             DataframeManager* dfMan,
                             Dataframe* src,
                             Dataframe* dest) {
    for (const NamedColumn* col : src->cols()) {
        Column* newCol = mem->allocSame(col->getColumn());
        auto* newNamedCol = NamedColumn::create(dfMan, newCol, col->getHeader());
        dest->addColumn(newNamedCol);
    }
}

}

PipelineNodeOutputInterface& PipelineBuilder::addScanNodes() {
    openMaterialize();

    ScanNodesProcessor* proc = ScanNodesProcessor::create(_pipeline);
    PipelineNodeOutputInterface& outNodeIDs = proc->outNodeIDs();

    NamedColumn* nodeIDs = allocColumn<ColumnNodeIDs>(outNodeIDs.getDataframe());
    outNodeIDs.setNodeIDs(nodeIDs);

    _matProc->getMaterializeData().addToStep<ColumnNodeIDs>(nodeIDs);

    _pendingOutput = &outNodeIDs;

    return outNodeIDs;
}

PipelineEdgeOutputInterface& PipelineBuilder::addGetOutEdges() {
    GetOutEdgesProcessor* getOutEdges = GetOutEdgesProcessor::create(_pipeline);
    _pendingOutput->connectTo(getOutEdges->inNodeIDs());

    PipelineEdgeOutputInterface& outEdges = getOutEdges->outEdges();
    Dataframe* outDf = outEdges.getDataframe();

    NamedColumn* indices = allocColumn<ColumnIndices>(outDf);
    outEdges.setIndices(indices);
    
    MaterializeData& matData = _matProc->getMaterializeData();
    matData.createStep(indices);

    NamedColumn* edgeIDs = allocColumn<ColumnEdgeIDs>(outDf);
    NamedColumn* edgeTypes = allocColumn<ColumnEdgeTypes>(outDf);
    NamedColumn* targetNodes = allocColumn<ColumnNodeIDs>(outDf);
    outEdges.setEdges(edgeIDs, edgeTypes, targetNodes);
    matData.addToStep<ColumnEdgeIDs>(edgeIDs);
    matData.addToStep<ColumnEdgeTypes>(edgeTypes);
    matData.addToStep<ColumnNodeIDs>(targetNodes);

    _pendingOutput = &outEdges;

    return outEdges;
}

void PipelineBuilder::openMaterialize() {
    _matProc = MaterializeProcessor::create(_pipeline, _mem);
}

void PipelineBuilder::closeMaterialize() {
    _matProc = nullptr;
}

PipelineBlockOutputInterface& PipelineBuilder::addMaterialize() {
    _pendingOutput->connectTo(_matProc->input());
    _pendingOutput = &_matProc->output();
    closeMaterialize();

    return _matProc->output();
}

void PipelineBuilder::addLambda(const LambdaProcessor::Callback& callback) {
    if (_matProc) {
        throw PipelineException("PipelineBuilder: cannot add lambda on pipeline with active materialize");
    }

    LambdaProcessor* lambda = LambdaProcessor::create(_pipeline, callback);
    _pendingOutput->connectTo(lambda->input());
    _pendingOutput = nullptr;
}

PipelineBlockOutputInterface& PipelineBuilder::addSkip(size_t count) {
    SkipProcessor* skip = SkipProcessor::create(_pipeline, count);
    _pendingOutput->connectTo(skip->input());

    duplicateDataframeShape(_mem, _dfMan, skip->input().getDataframe(), skip->output().getDataframe());
    
    _pendingOutput = &skip->output();

    return skip->output();
}

PipelineBlockOutputInterface& PipelineBuilder::addLimit(size_t count) {
    LimitProcessor* limit = LimitProcessor::create(_pipeline, count);
    _pendingOutput->connectTo(limit->input());

    duplicateDataframeShape(_mem, _dfMan, limit->input().getDataframe(), limit->output().getDataframe());

    _pendingOutput = &limit->output();

    return limit->output();
}

PipelineValueOutputInterface& PipelineBuilder::addCount() {
    CountProcessor* count = CountProcessor::create(_pipeline);
    _pendingOutput->connectTo(count->input());

    NamedColumn* countColumn = allocColumn<ColumnConst<size_t>>(count->output().getDataframe());
    count->output().setValue(countColumn);

    _pendingOutput = &count->output();
    return count->output();
}

PipelineBlockOutputInterface& PipelineBuilder::addLambdaSource(const LambdaSourceProcessor::Callback& callback) {
    LambdaSourceProcessor* source = LambdaSourceProcessor::create(_pipeline, callback);
    _pendingOutput = &source->output();
    return source->output();
}

template <db::SupportedType T>
PipelineValuesOutputInterface& PipelineBuilder::addGetNodeProperties(PropertyType propertyType) {
    auto* getProps = GetNodePropertiesProcessor<T>::create(_pipeline, propertyType);
    _pendingOutput->connectTo(getProps->inIDs());

    using ColumnValues = ColumnVector<typename T::Primitive>;

    PipelineValuesOutputInterface& outValues = getProps->outValues();
    Dataframe* outDf = outValues.getDataframe();

    NamedColumn* indices = allocColumn<ColumnIndices>(outDf);
    outValues.setIndices(indices);

    MaterializeData& matData = _matProc->getMaterializeData();
    matData.createStep(indices);

    NamedColumn* values = allocColumn<ColumnValues>(outDf);
    outValues.setValues(values);
    matData.addToStep<ColumnValues>(values);

    _pendingOutput = &getProps->outValues();
    
    return getProps->outValues();
}

template PipelineValuesOutputInterface& PipelineBuilder::addGetNodeProperties<db::types::Int64>(PropertyType propertyType);
template PipelineValuesOutputInterface& PipelineBuilder::addGetNodeProperties<db::types::UInt64>(PropertyType propertyType);
template PipelineValuesOutputInterface& PipelineBuilder::addGetNodeProperties<db::types::Double>(PropertyType propertyType);
template PipelineValuesOutputInterface& PipelineBuilder::addGetNodeProperties<db::types::String>(PropertyType propertyType);
template PipelineValuesOutputInterface& PipelineBuilder::addGetNodeProperties<db::types::Bool>(PropertyType propertyType);
