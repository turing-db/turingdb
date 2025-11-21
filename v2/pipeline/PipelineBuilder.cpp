#include "PipelineBuilder.h"

#include "processors/CartesianProductProcessor.h"
#include "processors/ScanNodesProcessor.h"
#include "processors/GetInEdgesProcessor.h"
#include "processors/GetEdgesProcessor.h"
#include "processors/GetOutEdgesProcessor.h"
#include "processors/MaterializeProcessor.h"
#include "processors/ProjectionProcessor.h"
#include "processors/LambdaProcessor.h"
#include "processors/GetPropertiesProcessor.h"
#include "processors/GetPropertiesWithNullProcessor.h"
#include "processors/SkipProcessor.h"
#include "processors/LimitProcessor.h"
#include "processors/CountProcessor.h"

#include "columns/ColumnIDs.h"
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

    _pendingOutput.updateInterface(&outNodeIDs);

    return outNodeIDs;
}

PipelineEdgeOutputInterface& PipelineBuilder::addGetOutEdges() {
    // Create get out edges processor
    if (!_isMaterializeOpen) {
        bioassert(_matProc != nullptr);
        _matProc = MaterializeProcessor::createFromPrev(_pipeline, _mem, *_matProc);
        _isMaterializeOpen = true;
    }

    GetOutEdgesProcessor* getOutEdges = GetOutEdgesProcessor::create(_pipeline);

    PipelineNodeInputInterface& input = getOutEdges->inNodeIDs();
    PipelineEdgeOutputInterface& output = getOutEdges->outEdges();

    _pendingOutput.connectTo(input);
    input.propagateColumns(output);

    Dataframe* outDf = output.getDataframe();

    // Allocate indices column
    NamedColumn* indices = allocColumn<ColumnIndices>(outDf);
    output.setIndices(indices);

    // Allocate output columns for edges
    NamedColumn* edgeIDs = allocColumn<ColumnEdgeIDs>(outDf);
    NamedColumn* edgeTypes = allocColumn<ColumnEdgeTypes>(outDf);
    NamedColumn* targetNodes = allocColumn<ColumnNodeIDs>(outDf);
    output.setEdges(edgeIDs, edgeTypes, targetNodes);

    // Register output in materialize data
    MaterializeData& matData = _matProc->getMaterializeData();
    matData.createStep(indices);
    matData.addToStep<ColumnEdgeIDs>(edgeIDs);
    matData.addToStep<ColumnEdgeTypes>(edgeTypes);
    matData.addToStep<ColumnNodeIDs>(targetNodes);

    _pendingOutput.updateInterface(&output);

    return output;
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

    _pendingOutput.updateInterface(&output);

    // Initialise the processor's "memory" stores for left and right ports
    duplicateDataframeShape(_mem, _dfMan, leftDf, &cartProd->leftMemory());
    duplicateDataframeShape(_mem, _dfMan, rightDf, &cartProd->rightMemory());

    return output;
}

PipelineEdgeOutputInterface& PipelineBuilder::addGetInEdges() {
    GetInEdgesProcessor* getInEdges = GetInEdgesProcessor::create(_pipeline);

    PipelineNodeInputInterface& input = getInEdges->inNodeIDs();
    PipelineEdgeOutputInterface& output = getInEdges->outEdges();

    _pendingOutput.connectTo(input);
    input.propagateColumns(output);

    Dataframe* df = output.getDataframe();

    NamedColumn* indices = allocColumn<ColumnIndices>(df);
    output.setIndices(indices);

    MaterializeData& matData = _matProc->getMaterializeData();
    matData.createStep(indices);

    NamedColumn* edgeIDs = allocColumn<ColumnEdgeIDs>(df);
    NamedColumn* edgeTypes = allocColumn<ColumnEdgeTypes>(df);
    NamedColumn* sourceNodes = allocColumn<ColumnNodeIDs>(df);
    output.setEdges(edgeIDs, edgeTypes, sourceNodes);
    matData.addToStep<ColumnEdgeIDs>(edgeIDs);
    matData.addToStep<ColumnEdgeTypes>(edgeTypes);
    matData.addToStep<ColumnNodeIDs>(sourceNodes);

    _pendingOutput.updateInterface(&output);

    return output;
}

PipelineEdgeOutputInterface& PipelineBuilder::addGetEdges() {
    GetEdgesProcessor* getInEdges = GetEdgesProcessor::create(_pipeline);

    PipelineNodeInputInterface& input = getInEdges->inNodeIDs();
    PipelineEdgeOutputInterface& output = getInEdges->outEdges();

    _pendingOutput.connectTo(input);
    input.propagateColumns(output);

    Dataframe* df = output.getDataframe();

    NamedColumn* indices = allocColumn<ColumnIndices>(df);
    output.setIndices(indices);

    MaterializeData& matData = _matProc->getMaterializeData();
    matData.createStep(indices);

    NamedColumn* edgeIDs = allocColumn<ColumnEdgeIDs>(df);
    NamedColumn* edgeTypes = allocColumn<ColumnEdgeTypes>(df);
    NamedColumn* otherNodes = allocColumn<ColumnNodeIDs>(df);
    output.setEdges(edgeIDs, edgeTypes, otherNodes);
    matData.addToStep<ColumnEdgeIDs>(edgeIDs);
    matData.addToStep<ColumnEdgeTypes>(edgeTypes);
    matData.addToStep<ColumnNodeIDs>(otherNodes);

    _pendingOutput.updateInterface(&output);

    return output;
}

void PipelineBuilder::openMaterialize() {
    _matProc = MaterializeProcessor::create(_pipeline, _mem);
    _isMaterializeOpen = true;
}

bool PipelineBuilder::isSingleMaterializeStep() const {
    return _matProc->getMaterializeData().isSingleStep();
}

PipelineBlockOutputInterface& PipelineBuilder::addMaterialize() {
    auto& input = _matProc->input();
    auto& output = _matProc->output();

    _pendingOutput.connectTo(input);
    output.setStream(input.getStream());
    _isMaterializeOpen = false;

    _pendingOutput.updateInterface(&output);

    return output;
}

void PipelineBuilder::addLambda(const LambdaProcessor::Callback& callback) {
    LambdaProcessor* lambda = LambdaProcessor::create(_pipeline, callback);
    _pendingOutput.connectTo(lambda->input());
    _pendingOutput.updateInterface(nullptr);
}

PipelineBlockOutputInterface& PipelineBuilder::addSkip(size_t count) {
    SkipProcessor* skip = SkipProcessor::create(_pipeline, count);

    auto& input = skip->input();
    auto& output = skip->output();

    _pendingOutput.connectTo(input);
    output.setStream(input.getStream());
    duplicateDataframeShape(_mem, _dfMan, input.getDataframe(), output.getDataframe());

    _pendingOutput.updateInterface(&output);

    return skip->output();
}

PipelineBlockOutputInterface& PipelineBuilder::addLimit(size_t count) {
    LimitProcessor* limit = LimitProcessor::create(_pipeline, count);

    auto& input = limit->input();
    auto& output = limit->output();

    _pendingOutput.connectTo(input);
    output.setStream(input.getStream());
    duplicateDataframeShape(_mem, _dfMan, input.getDataframe(), output.getDataframe());

    _pendingOutput.updateInterface(&output);

    return limit->output();
}

PipelineValueOutputInterface& PipelineBuilder::addCount(ColumnTag colTag) {
    CountProcessor* count = CountProcessor::create(_pipeline, colTag);
    _pendingOutput.connectTo(count->input());

    NamedColumn* countColumn = allocColumn<ColumnConst<size_t>>(count->output().getDataframe());
    count->output().setValue(countColumn);

    _pendingOutput.updateInterface(&count->output());
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

    _pendingOutput.updateInterface(&output);

    return output;
}

PipelineBlockOutputInterface& PipelineBuilder::addLambdaSource(const LambdaSourceProcessor::Callback& callback) {
    LambdaSourceProcessor* source = LambdaSourceProcessor::create(_pipeline, callback);
    _pendingOutput.updateInterface(&source->output());
    return source->output();
}

PipelineBlockOutputInterface& PipelineBuilder::addLambdaTransform(const LambdaTransformProcessor::Callback& cb) {
    LambdaTransformProcessor* transf = LambdaTransformProcessor::create(_pipeline, cb);

    PipelineBlockInputInterface& input = transf->input();
    PipelineBlockOutputInterface& output = transf->output();

    _pendingOutput.connectTo(input);
    input.propagateColumns(output);
    _pendingOutput.updateInterface(&output);

    return output;
}

template <EntityType Entity, db::SupportedType T>
PipelineValuesOutputInterface& PipelineBuilder::addGetProperties(PropertyType propertyType) {
    using GetPropsProc = GetPropertiesProcessor<Entity, T>;
    using ColumnValues = typename GetPropsProc::ColumnValues;

    // Create get node properties processor
    auto* getProps = GetPropsProc::create(_pipeline, propertyType);

    PipelineBlockInputInterface& input = getProps->input();
    PipelineValuesOutputInterface& output = getProps->output();

    _pendingOutput.connectTo(input);
    input.propagateColumns(output);
    output.setStream(input.getStream());

    Dataframe* outDf = output.getDataframe();

    // Allocate output values column
    NamedColumn* values = allocColumn<ColumnValues>(outDf);
    output.setValues(values);

    // Allocate indices column
    NamedColumn* indices = allocColumn<ColumnIndices>(outDf);
    output.setIndices(indices);

    MaterializeData& matData = _matProc->getMaterializeData();
    matData.createStep(indices);
    matData.addToStep<ColumnValues>(values);

    _pendingOutput.updateInterface(&output);

    return output;
}

template <EntityType Entity, db::SupportedType T>
PipelineValuesOutputInterface& PipelineBuilder::addGetPropertiesWithNull(ColumnTag entityTag,
                                                                         PropertyType propertyType) {
    using GetPropsProc = GetPropertiesWithNullProcessor<Entity, T>;
    using ColumnValues = typename GetPropsProc::ColumnValues;

    // Create get node properties processor
    auto* getProps = GetPropsProc::create(_pipeline, entityTag, propertyType);

    PipelineBlockInputInterface& input = getProps->input();
    PipelineValuesOutputInterface& output = getProps->output();

    _pendingOutput.connectTo(input);
    input.propagateColumns(output);
    output.setStream(input.getStream());

    Dataframe* outDf = output.getDataframe();

    // Allocate output values column
    NamedColumn* values = allocColumn<ColumnValues>(outDf);
    output.setValues(values);

    MaterializeData& matData = _matProc->getMaterializeData();
    matData.addToStep<ColumnValues>(values);

    _pendingOutput.updateInterface(&output);

    return output;
}

template PipelineValuesOutputInterface& PipelineBuilder::addGetProperties<EntityType::Node, db::types::Int64>(PropertyType);
template PipelineValuesOutputInterface& PipelineBuilder::addGetProperties<EntityType::Node, db::types::UInt64>(PropertyType);
template PipelineValuesOutputInterface& PipelineBuilder::addGetProperties<EntityType::Node, db::types::Double>(PropertyType);
template PipelineValuesOutputInterface& PipelineBuilder::addGetProperties<EntityType::Node, db::types::String>(PropertyType);
template PipelineValuesOutputInterface& PipelineBuilder::addGetProperties<EntityType::Node, db::types::Bool>(PropertyType);
template PipelineValuesOutputInterface& PipelineBuilder::addGetProperties<EntityType::Edge, db::types::Int64>(PropertyType);
template PipelineValuesOutputInterface& PipelineBuilder::addGetProperties<EntityType::Edge, db::types::UInt64>(PropertyType);
template PipelineValuesOutputInterface& PipelineBuilder::addGetProperties<EntityType::Edge, db::types::Double>(PropertyType);
template PipelineValuesOutputInterface& PipelineBuilder::addGetProperties<EntityType::Edge, db::types::String>(PropertyType);
template PipelineValuesOutputInterface& PipelineBuilder::addGetProperties<EntityType::Edge, db::types::Bool>(PropertyType);

template PipelineValuesOutputInterface& PipelineBuilder::addGetPropertiesWithNull<EntityType::Node, db::types::Int64>(ColumnTag, PropertyType);
template PipelineValuesOutputInterface& PipelineBuilder::addGetPropertiesWithNull<EntityType::Node, db::types::UInt64>(ColumnTag, PropertyType);
template PipelineValuesOutputInterface& PipelineBuilder::addGetPropertiesWithNull<EntityType::Node, db::types::Double>(ColumnTag, PropertyType);
template PipelineValuesOutputInterface& PipelineBuilder::addGetPropertiesWithNull<EntityType::Node, db::types::String>(ColumnTag, PropertyType);
template PipelineValuesOutputInterface& PipelineBuilder::addGetPropertiesWithNull<EntityType::Node, db::types::Bool>(ColumnTag, PropertyType);
template PipelineValuesOutputInterface& PipelineBuilder::addGetPropertiesWithNull<EntityType::Edge, db::types::Int64>(ColumnTag, PropertyType);
template PipelineValuesOutputInterface& PipelineBuilder::addGetPropertiesWithNull<EntityType::Edge, db::types::UInt64>(ColumnTag, PropertyType);
template PipelineValuesOutputInterface& PipelineBuilder::addGetPropertiesWithNull<EntityType::Edge, db::types::Double>(ColumnTag, PropertyType);
template PipelineValuesOutputInterface& PipelineBuilder::addGetPropertiesWithNull<EntityType::Edge, db::types::String>(ColumnTag, PropertyType);
template PipelineValuesOutputInterface& PipelineBuilder::addGetPropertiesWithNull<EntityType::Edge, db::types::Bool>(ColumnTag, PropertyType);
