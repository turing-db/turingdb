#include "PipelineBuilder.h"

#include "processors/CartesianProductProcessor.h"
#include "processors/DatabaseProcedureProcessor.h"
#include "processors/ForkProcessor.h"
#include "processors/HashJoinProcessor.h"
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
#include "processors/WriteProcessor.h"
#include "processors/WriteProcessorTypes.h"

#include "columns/ColumnIDs.h"
#include "columns/ColumnEdgeTypes.h"
#include "interfaces/PipelineBlockOutputInterface.h"
#include "dataframe/ColumnTag.h"
#include "dataframe/NamedColumn.h"

#include "FatalException.h"

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

void createHashJoinDataFrameShape(LocalMemory* mem,
                                  DataframeManager* dfMan,
                                  Dataframe* leftSrc,
                                  Dataframe* rightSrc,
                                  Dataframe* dest,
                                  ColumnTag leftJoinKeyTag,
                                  ColumnTag rightJoinKeyTag) {
    // Add left input columns
    for (const NamedColumn* col : leftSrc->cols()) {
        if (col->getTag() == leftJoinKeyTag) {
            continue;
        }

        Column* newCol = mem->allocSame(col->getColumn());
        auto* newNamedCol = NamedColumn::create(dfMan, newCol, col->getTag());
        dest->addColumn(newNamedCol);
    }

    // Add right input columns
    for (const NamedColumn* col : rightSrc->cols()) {
        // Skip Columns present in the left input
        if (leftSrc->getColumn(col->getTag()) != nullptr || col->getTag() == rightJoinKeyTag) {
            continue;
        }

        Column* newCol = mem->allocSame(col->getColumn());
        auto* newNamedCol = NamedColumn::create(dfMan, newCol, col->getTag());
        dest->addColumn(newNamedCol);
    }

    // allocate a join-key column tag in the output with a new column tag if the left
    // and right join column tags aren't the same
    const ColumnTag joinColTag = leftJoinKeyTag == rightJoinKeyTag ? leftJoinKeyTag : dfMan->allocTag();
    Column* newCol = mem->allocSame(leftSrc->getColumn(leftJoinKeyTag)->getColumn());
    auto* newNamedCol = NamedColumn::create(dfMan, newCol, joinColTag);
    dest->addColumn(newNamedCol);
}

}

PipelineNodeOutputInterface& PipelineBuilder::addScanNodes() {
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
    GetEdgesProcessor* getEdges = GetEdgesProcessor::create(_pipeline);

    PipelineNodeInputInterface& input = getEdges->inNodeIDs();
    PipelineEdgeOutputInterface& output = getEdges->outEdges();

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

bool PipelineBuilder::isSingleMaterializeStep() const {
    return _matProc->getMaterializeData().isSingleStep();
}

PipelineBuilder::ForkOutputs& PipelineBuilder::addFork(size_t count) {
    ForkProcessor* fork = ForkProcessor::create(_pipeline, count);

    PipelineBlockInputInterface& input = fork->input();
    std::vector<PipelineBlockOutputInterface>& outputs = fork->outputs();

    _pendingOutput.connectTo(input);

    for (auto& output : outputs) {
        duplicateDataframeShape(_mem, _dfMan, input.getDataframe(), output.getDataframe());
        output.setStream(input.getStream());
    }

    return outputs;
}

PipelineBlockOutputInterface& PipelineBuilder::addMaterialize() {
    auto& input = _matProc->input();
    auto& output = _matProc->output();

    _pendingOutput.connectTo(input);
    output.setStream(input.getStream());

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

PipelineBlockOutputInterface& PipelineBuilder::addProjection(std::span<ProjectionItem> items) {
    ProjectionProcessor* projection = ProjectionProcessor::create(_pipeline);

    PipelineBlockInputInterface& input = projection->input();
    PipelineBlockOutputInterface& output = projection->output();

    _pendingOutput.connectTo(input);

    Dataframe* inDf = input.getDataframe();
    Dataframe* outDf = output.getDataframe();

    // Forward only the projected columns to the next processor
    for (const auto& item : items) {
        NamedColumn* col = inDf->getColumn(item._tag);
        outDf->addColumn(col);

        if (!item._name.empty()) {
            col->rename(item._name);
        }
    }

    _pendingOutput.updateInterface(&output);

    return output;
}

PipelineBlockOutputInterface& PipelineBuilder::addHashJoin(PipelineBlockOutputInterface* rhs,
                                                           ColumnTag leftJoinKey,
                                                           ColumnTag rightJoinKey) {
    HashJoinProcessor* join = HashJoinProcessor::create(_pipeline,
                                                        leftJoinKey,
                                                        rightJoinKey);

    _pendingOutput.connectTo(join->leftInput());
    rhs->connectTo(join->rightInput());

    Dataframe* leftDf = join->leftInput().getDataframe();
    Dataframe* rightDf = join->rightInput().getDataframe();

    PipelineBlockOutputInterface& outInterface = join->output();
    Dataframe* outDf = outInterface.getDataframe();

    // need to change output shape
    createHashJoinDataFrameShape(_mem,
                                 _dfMan,
                                 leftDf,
                                 rightDf,
                                 outDf,
                                 leftJoinKey,
                                 rightJoinKey);

    _pendingOutput.setInterface(&outInterface);
    return outInterface;
}

PipelineBlockOutputInterface& PipelineBuilder::addLambdaSource(const LambdaSourceProcessor::Callback& callback) {
    LambdaSourceProcessor* source = LambdaSourceProcessor::create(_pipeline, callback);
    _pendingOutput.updateInterface(&source->output());
    return source->output();
}

PipelineBlockOutputInterface& PipelineBuilder::addDatabaseProcedure(const ProcedureBlueprint& blueprint,
                                                                    std::span<ProcedureBlueprint::YieldItem> yield) {
    DatabaseProcedureProcessor* proc = DatabaseProcedureProcessor::create(_pipeline, blueprint);
    auto& output = proc->output();

    _pendingOutput.updateInterface(&output);

    proc->allocColumns(*_mem, *_dfMan, yield);

    return output;
}

PipelineBlockOutputInterface& PipelineBuilder::addLambdaTransform(const LambdaTransformProcessor::Callback& callback) {
    LambdaTransformProcessor* transf = LambdaTransformProcessor::create(_pipeline, callback);

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

PipelineBlockOutputInterface& PipelineBuilder::addWrite(const WriteProcessor::DeletedNodes& nodeColumnsToDelete,
                                                        const WriteProcessor::DeletedEdges& edgeColumnsToDelete,
                                                        WriteProcessor::PendingNodes& pendingNodes,
                                                        WriteProcessor::PendingEdges& pendingEdges) {
    auto* processor = WriteProcessor::create(_pipeline);

    if (!_pendingOutput.getInterface()) {
        throw FatalException("WriteProcessor has no input");
    }

    // NOTE: CREATE (n:P) DELETE n has no affect but seems to run on Neo4j
    _pendingOutput.connectTo(processor->input());

    PipelineBlockInputInterface& input = processor->input();
    PipelineBlockOutputInterface& output = processor->output();
    Dataframe* inDf = input.getDataframe();
    Dataframe* outDf = output.getDataframe();

    bioassert(outDf->size() == 0);

    // 1. Register columns for deleted nodes and edges
    for (const ColumnTag deletedNodeCol : nodeColumnsToDelete) {
        NamedColumn* inputColumnToDelete = inDf->getColumn(deletedNodeCol);
        // TODO: Make exception?
        bioassert(inputColumnToDelete);

        // Skip if the column already exists for cases like MATCH (n) DELETE n,n
        if (outDf->getColumn(inputColumnToDelete->getTag())) {
            continue;
        }
        outDf->addColumn(inputColumnToDelete);
    }
    processor->setDeletedNodes(nodeColumnsToDelete);

    for (const ColumnTag deletedEdgeCol : edgeColumnsToDelete) {
        NamedColumn* inputColumnToDelete = inDf->getColumn(deletedEdgeCol);
        // TODO: Make exception?
        bioassert(inputColumnToDelete);

        // Skip if the column already exists for cases like MATCH (n) DELETE n,n
        if (outDf->getColumn(inputColumnToDelete->getTag())) {
            continue;
        }
        outDf->addColumn(inputColumnToDelete);
    }
    processor->setDeletedEdges(edgeColumnsToDelete);

    for (WriteProcessorTypes::PendingNode& node : pendingNodes) {
        Column* newCol = _mem->alloc<ColumnNodeIDs>();
        const ColumnTag newTag = _dfMan->allocTag();
        auto* newNamedCol = NamedColumn::create(_dfMan, newCol, newTag);
        newNamedCol->rename(node._name);

        node._tag = newTag;

        // XXX: Assumption that each "pendingNode" can expand to multiple nodes to be
        // created, and that there is not one "pendingNode" for each node that will
        // actually end up being created.
        outDf->addColumn(newNamedCol);
    }
    processor->setPendingNodes(pendingNodes);

    for (WriteProcessorTypes::PendingEdge& edge : pendingEdges) {
        Column* newCol = _mem->alloc<ColumnEdgeIDs>();
        const ColumnTag newTag = _dfMan->allocTag();
        auto* newNamedCol = NamedColumn::create(_dfMan, newCol, newTag);
        newNamedCol->rename(edge._name);

        edge._tag = newTag;

        // XXX: Assumption that each "pendingNode" can expand to multiple nodes to be
        // created, and that there is not one "pendingNode" for each node that will
        // actually end up being created.
        outDf->addColumn(newNamedCol);
    }
    processor->setPendingEdges(pendingEdges);

    // This function already handles duplicates, so call it after adding all our columns
    input.propagateColumns(output);

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
