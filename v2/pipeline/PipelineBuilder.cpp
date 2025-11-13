#include "PipelineBuilder.h"

#include "PipelineInterface.h"
#include "processors/CartesianProductProcessor.h"
#include "processors/ScanNodesProcessor.h"
#include "processors/GetInEdgesProcessor.h"
#include "processors/GetEdgesProcessor.h"
#include "processors/GetOutEdgesProcessor.h"
#include "processors/MaterializeProcessor.h"
#include "processors/ProjectionProcessor.h"
#include "processors/LambdaProcessor.h"
#include "processors/GetPropertiesProcessor.h"
#include "processors/SkipProcessor.h"
#include "processors/LimitProcessor.h"
#include "processors/CountProcessor.h"

#include "columns/ColumnEdgeTypes.h"

using namespace db::v2;
using namespace db;

namespace {

void duplicateDataframeShape(LocalMemory* mem,
                             DataframeManager* dfMan,
                             db::Dataframe* src,
                             db::Dataframe* dest) {
    for (const NamedColumn* col : src->cols()) {
        Column* newCol = mem->allocSame(col->getColumn());
        auto* newNamedCol = NamedColumn::create(dfMan, newCol, col->getTag());
        dest->addColumn(newNamedCol);
    }
}

/*
* @brief Column-wise concatenation of @param src onto @param dest
*/
void concatDataframeShape(LocalMemory* mem,
                          DataframeManager* dfMan,
                          db::Dataframe* src,
                          db::Dataframe* dest) {
    for (const NamedColumn* col : src->cols()) {
        Column* newCol = mem->allocSame(col->getColumn());
        auto* newNamedCol = NamedColumn::create(dfMan, newCol, col->getTag());
        dest->addColumn(newNamedCol);
    }
}

}

PipelineNodeOutputInterface& PipelineBuilder::addScanNodes() {
    openMaterialize();

    // Create scan nodes processor
    ScanNodesProcessor* proc = ScanNodesProcessor::create(_pipeline);
    PipelineNodeOutputInterface& outNodeIDs = proc->outNodeIDs();

    // Allocate output node IDs column
    NamedColumn* nodeIDs = allocColumn<ColumnNodeIDs>(outNodeIDs.getDataframe());
    outNodeIDs.setNodeIDs(nodeIDs);

    // Register output in materialize data
    _matProc->getMaterializeData().addToStep<ColumnNodeIDs>(nodeIDs);

    _pendingOutput.setInterface(&outNodeIDs);

    return outNodeIDs;
}

PipelineEdgeOutputInterface& PipelineBuilder::addGetOutEdges() {
    // Create get out edges processor
    GetOutEdgesProcessor* getOutEdges = GetOutEdgesProcessor::create(_pipeline);
    _pendingOutput.connectTo(getOutEdges->inNodeIDs());

    PipelineEdgeOutputInterface& outEdges = getOutEdges->outEdges();
    Dataframe* outDf = outEdges.getDataframe();

    // Allocate indices column
    NamedColumn* indices = allocColumn<ColumnIndices>(outDf);
    outEdges.setIndices(indices);

    // Allocate output columns for edges
    NamedColumn* edgeIDs = allocColumn<ColumnEdgeIDs>(outDf);
    NamedColumn* edgeTypes = allocColumn<ColumnEdgeTypes>(outDf);
    NamedColumn* targetNodes = allocColumn<ColumnNodeIDs>(outDf);
    outEdges.setEdges(edgeIDs, edgeTypes, targetNodes);

    // Register output in materialize data
    MaterializeData& matData = _matProc->getMaterializeData();
    matData.createStep(indices);
    matData.addToStep<ColumnEdgeIDs>(edgeIDs);
    matData.addToStep<ColumnEdgeTypes>(edgeTypes);
    matData.addToStep<ColumnNodeIDs>(targetNodes);

    _pendingOutput.setInterface(&outEdges);

    return outEdges;
}

PipelineBlockOutputInterface& PipelineBuilder::addCartesianProduct(PipelineOutputInterface* rhs) {
    CartesianProductProcessor* cartProd = CartesianProductProcessor::create(_pipeline);

    // LHS is implict in @ref _pendingOutput
    _pendingOutput.connectTo(cartProd->leftHandSide());
    rhs->connectTo(cartProd->rightHandSide());

    PipelineBlockOutputInterface& output = cartProd->output();
    Dataframe* outDf = output.getDataframe();

    Dataframe* leftDf = cartProd->leftHandSide().getDataframe();
    Dataframe* rightDf = cartProd->rightHandSide().getDataframe();

    // The output DF should be the same as the inputs, concatenated
    duplicateDataframeShape(_mem, _dfMan, leftDf, outDf);
    concatDataframeShape(_mem, _dfMan, rightDf, outDf);

    _pendingOutput.setInterface(&output);

    // Initialise the processor's "memory" stores for left and right ports
    Dataframe* leftMemory = new Dataframe;
    duplicateDataframeShape(_mem, _dfMan, leftDf, leftMemory);
    cartProd->setLeftMemory(std::unique_ptr<Dataframe>(leftMemory));

    Dataframe* rightMemory = new Dataframe;
    duplicateDataframeShape(_mem, _dfMan, rightDf, rightMemory);
    cartProd->setRightMemory(std::unique_ptr<Dataframe>(rightMemory));

    return output;
}

PipelineEdgeOutputInterface& PipelineBuilder::addGetInEdges() {
    GetInEdgesProcessor* getInEdges = GetInEdgesProcessor::create(_pipeline);
    _pendingOutput.connectTo(getInEdges->inNodeIDs());

    PipelineEdgeOutputInterface& inEdges = getInEdges->outEdges();
    Dataframe* df = inEdges.getDataframe();

    NamedColumn* indices = allocColumn<ColumnIndices>(df);
    inEdges.setIndices(indices);
    
    MaterializeData& matData = _matProc->getMaterializeData();
    matData.createStep(indices);

    NamedColumn* edgeIDs = allocColumn<ColumnEdgeIDs>(df);
    NamedColumn* edgeTypes = allocColumn<ColumnEdgeTypes>(df);
    NamedColumn* sourceNodes = allocColumn<ColumnNodeIDs>(df);
    inEdges.setEdges(edgeIDs, edgeTypes, sourceNodes);
    matData.addToStep<ColumnEdgeIDs>(edgeIDs);
    matData.addToStep<ColumnEdgeTypes>(edgeTypes);
    matData.addToStep<ColumnNodeIDs>(sourceNodes);

    _pendingOutput.setInterface(&inEdges);

    return inEdges;
}

PipelineEdgeOutputInterface& PipelineBuilder::addGetEdges() {
    GetEdgesProcessor* getInEdges = GetEdgesProcessor::create(_pipeline);
    _pendingOutput.connectTo(getInEdges->inNodeIDs());

    PipelineEdgeOutputInterface& inEdges = getInEdges->outEdges();
    Dataframe* df = inEdges.getDataframe();

    NamedColumn* indices = allocColumn<ColumnIndices>(df);
    inEdges.setIndices(indices);
    
    MaterializeData& matData = _matProc->getMaterializeData();
    matData.createStep(indices);

    NamedColumn* edgeIDs = allocColumn<ColumnEdgeIDs>(df);
    NamedColumn* edgeTypes = allocColumn<ColumnEdgeTypes>(df);
    NamedColumn* otherNodes = allocColumn<ColumnNodeIDs>(df);
    inEdges.setEdges(edgeIDs, edgeTypes, otherNodes);
    matData.addToStep<ColumnEdgeIDs>(edgeIDs);
    matData.addToStep<ColumnEdgeTypes>(edgeTypes);
    matData.addToStep<ColumnNodeIDs>(otherNodes);

    _pendingOutput.setInterface(&inEdges);

    return inEdges;
}

void PipelineBuilder::openMaterialize() {
    _matProc = MaterializeProcessor::create(_pipeline, _mem);
}

void PipelineBuilder::closeMaterialize() {
    _matProc = nullptr;
}

bool PipelineBuilder::isSingleMaterializeStep() const {
    return _matProc->getMaterializeData().isSingleStep();
}

PipelineBlockOutputInterface& PipelineBuilder::addMaterialize() {
    _pendingOutput.connectTo(_matProc->input());
    _pendingOutput.setInterface(&_matProc->output());

    auto& output = _matProc->output();

    closeMaterialize();

    return output;
}

void PipelineBuilder::addLambda(const LambdaProcessor::Callback& callback) {
    LambdaProcessor* lambda = LambdaProcessor::create(_pipeline, callback);
    _pendingOutput.connectTo(lambda->input());
    _pendingOutput.setInterface(nullptr);
}

PipelineBlockOutputInterface& PipelineBuilder::addSkip(size_t count) {
    SkipProcessor* skip = SkipProcessor::create(_pipeline, count);
    _pendingOutput.connectTo(skip->input());

    duplicateDataframeShape(_mem, _dfMan, skip->input().getDataframe(), skip->output().getDataframe());
    
    _pendingOutput.setInterface(&skip->output());

    return skip->output();
}

PipelineBlockOutputInterface& PipelineBuilder::addLimit(size_t count) {
    LimitProcessor* limit = LimitProcessor::create(_pipeline, count);
    _pendingOutput.connectTo(limit->input());

    duplicateDataframeShape(_mem, _dfMan, limit->input().getDataframe(), limit->output().getDataframe());

    _pendingOutput.setInterface(&limit->output());

    return limit->output();
}

PipelineValueOutputInterface& PipelineBuilder::addCount() {
    CountProcessor* count = CountProcessor::create(_pipeline);
    _pendingOutput.connectTo(count->input());

    NamedColumn* countColumn = allocColumn<ColumnConst<size_t>>(count->output().getDataframe());
    count->output().setValue(countColumn);

    _pendingOutput.setInterface(&count->output());
    return count->output();
}

PipelineBlockOutputInterface& PipelineBuilder::addProjection(std::span<ColumnTag> tags) {
    ProjectionProcessor* projection = ProjectionProcessor::create(_pipeline);
    
    PipelineBlockInputInterface& input = projection->input();
    PipelineBlockOutputInterface& output = projection->output();

    _pendingOutput.connectTo(input);

    Dataframe* inDf = input.getDataframe();
    Dataframe* outDf = output.getDataframe();

    // Forward only the projected columns to the next processor
    for (const ColumnTag& tag : tags) {
        outDf->addColumn(inDf->getColumn(tag));
    }

    _pendingOutput.setInterface(&output);

    return output;
}

PipelineBlockOutputInterface& PipelineBuilder::addLambdaSource(const LambdaSourceProcessor::Callback& callback) {
    LambdaSourceProcessor* source = LambdaSourceProcessor::create(_pipeline, callback);
    _pendingOutput.setInterface(&source->output());
    return source->output();
}

PipelineBlockOutputInterface& PipelineBuilder::addLambdaTransform(const LambdaTransformProcessor::Callback& cb) {
    LambdaTransformProcessor* transf = LambdaTransformProcessor::create(_pipeline, cb);

    PipelineBlockInputInterface& input = transf->input();
    PipelineBlockOutputInterface& output = transf->output();

    _pendingOutput.connectTo(input);
    _pendingOutput.setInterface(&output);

    return output;
}

template <db::SupportedType T>
PipelineValuesOutputInterface& PipelineBuilder::addGetNodeProperties(PropertyType propertyType) {
    using ColumnValues = ColumnVector<typename T::Primitive>;

    // Create get node properties processor
    auto* getProps = GetNodePropertiesProcessor<T>::create(_pipeline, propertyType);
    _pendingOutput.connectTo(getProps->inIDs());

    PipelineValuesOutputInterface& outValues = getProps->outValues();
    Dataframe* outDf = outValues.getDataframe();

    // Allocate indices column
    NamedColumn* indices = allocColumn<ColumnIndices>(outDf);
    outValues.setIndices(indices);

    // Allocate output values column
    NamedColumn* values = allocColumn<ColumnValues>(outDf);
    outValues.setValues(values);

    // Register output in materialize data
    MaterializeData& matData = _matProc->getMaterializeData();
    matData.createStep(indices);
    matData.addToStep<ColumnValues>(values);

    _pendingOutput.setInterface(&getProps->outValues());
    
    return getProps->outValues();
}

template PipelineValuesOutputInterface& PipelineBuilder::addGetNodeProperties<db::types::Int64>(PropertyType propertyType);
template PipelineValuesOutputInterface& PipelineBuilder::addGetNodeProperties<db::types::UInt64>(PropertyType propertyType);
template PipelineValuesOutputInterface& PipelineBuilder::addGetNodeProperties<db::types::Double>(PropertyType propertyType);
template PipelineValuesOutputInterface& PipelineBuilder::addGetNodeProperties<db::types::String>(PropertyType propertyType);
template PipelineValuesOutputInterface& PipelineBuilder::addGetNodeProperties<db::types::Bool>(PropertyType propertyType);
