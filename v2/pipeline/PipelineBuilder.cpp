#include "PipelineBuilder.h"

#include "EntityOutputStream.h"
#include "processors/CartesianProductProcessor.h"
#include "processors/ChangeProcessor.h"
#include "processors/CommitProcessor.h"
#include "processors/DatabaseProcedureProcessor.h"
#include "processors/ForkProcessor.h"
#include "processors/HashJoinProcessor.h"
#include "processors/ExprProgram.h"
#include "processors/ScanNodesProcessor.h"
#include "processors/ScanNodesByLabelProcessor.h"
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
#include "processors/ListGraphProcessor.h"
#include "processors/WriteProcessorTypes.h"
#include "processors/GetLabelSetIDProcessor.h"
#include "processors/GetEdgeTypeIDProcessor.h"
#include "processors/LoadGraphProcessor.h"
#include "processors/CreateGraphProcessor.h"
#include "processors/LoadGMLProcessor.h"
#include "processors/LoadNeo4jProcessor.h"
#include "processors/S3ConnectProcessor.h"
#include "processors/S3PullProcessor.h"
#include "processors/S3PushProcessor.h"
#include "processors/ComputeExprProcessor.h"
#include "processors/FilterProcessor.h"

#include "interfaces/PipelineBlockOutputInterface.h"
#include "interfaces/PipelineEdgeInputInterface.h"
#include "interfaces/PipelineValuesOutputInterface.h"
#include "processors/ComputeExprProcessor.h"
#include "processors/FilterProcessor.h"

#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "columns/ColumnEdgeTypes.h"

#include "dataframe/ColumnTag.h"
#include "dataframe/NamedColumn.h"
#include "versioning/ChangeID.h"

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
                                  const Dataframe* leftSrc,
                                  const Dataframe* rightSrc,
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

    // Stream does not change when adding CartesianProduct
    output.setStream(_pendingOutput.getInterface()->getStream());
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

PipelineBlockOutputInterface& PipelineBuilder::addHashJoin(PipelineOutputInterface* rhs,
                                                           ColumnTag leftJoinKey,
                                                           ColumnTag rightJoinKey) {
    HashJoinProcessor* join = HashJoinProcessor::create(_pipeline,
                                                        leftJoinKey,
                                                        rightJoinKey);

    _pendingOutput.connectTo(join->leftInput());
    rhs->connectTo(join->rightInput());

    const Dataframe* leftDf = join->leftInput().getDataframe();
    const Dataframe* rightDf = join->rightInput().getDataframe();


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

    auto joinTag = outDf->cols().back()->getTag();

    outInterface.setStream(EntityOutputStream::createNodeStream(joinTag));
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

PipelineBlockOutputInterface& PipelineBuilder::addChangeOp(ChangeOp op) {
    ChangeProcessor* proc = ChangeProcessor::create(_pipeline, op);
    auto& output = proc->output();

    Dataframe* df = output.getDataframe();
    NamedColumn* changeIDCol = allocColumn<ColumnVector<ChangeID>>(df);
    changeIDCol->rename("changeID");
    proc->setColumn(static_cast<ColumnVector<ChangeID>*>(changeIDCol->getColumn()));

    _pendingOutput.updateInterface(&output);

    return output;
}

PipelineBlockOutputInterface& PipelineBuilder::addCommit() {
    CommitProcessor* proc = CommitProcessor::create(_pipeline);
    auto& output = proc->output();

    _pendingOutput.updateInterface(&output);

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

PipelineValuesOutputInterface& PipelineBuilder::addComputeExpr(ExprProgram* exprProg) {
    ComputeExprProcessor* compExpr = ComputeExprProcessor::create(_pipeline, exprProg);

    PipelineBlockInputInterface& input = compExpr->input();
    PipelineValuesOutputInterface& output = compExpr->output();

    _pendingOutput.connectTo(compExpr->input());
    input.propagateColumns(output);

    _pendingOutput.updateInterface(&output);

    return output;
}

PipelineValuesOutputInterface& PipelineBuilder::addGetLabelSetID() {
    GetLabelSetIDProcessor* proc = GetLabelSetIDProcessor::create(_pipeline);

    PipelineNodeInputInterface& input = proc->input();
    PipelineValuesOutputInterface& output = proc->output();
    output.setStream(_pendingOutput.getInterface()->getStream());

    _pendingOutput.connectTo(input);
    input.propagateColumns(output);

    Dataframe* df = output.getDataframe();
    NamedColumn* labelsetIDsValues = allocColumn<ColumnVector<LabelSetID>>(df);
    output.setValues(labelsetIDsValues);

    MaterializeData& matData = _matProc->getMaterializeData();
    matData.addToStep<ColumnVector<LabelSetID>>(labelsetIDsValues);

    _pendingOutput.updateInterface(&output);

    return output;
}

PipelineValuesOutputInterface& PipelineBuilder::addGetEdgeTypeID() {
    GetEdgeTypeIDProcessor* proc = GetEdgeTypeIDProcessor::create(_pipeline);

    PipelineEdgeInputInterface& input = proc->input();
    PipelineValuesOutputInterface& output = proc->output();
    output.setStream(_pendingOutput.getInterface()->getStream());

    _pendingOutput.connectTo(input);
    input.propagateColumns(output);

    Dataframe* df = output.getDataframe();
    NamedColumn* edgeTypeIDValues = allocColumn<ColumnVector<EdgeTypeID>>(df);
    output.setValues(edgeTypeIDValues);

    MaterializeData& matData = _matProc->getMaterializeData();
    matData.addToStep<ColumnVector<EdgeTypeID>>(edgeTypeIDValues);

    _pendingOutput.updateInterface(&output);
    return output;
}

PipelineNodeOutputInterface& PipelineBuilder::addScanNodesByLabel(const LabelSet* labelset) {
    ScanNodesByLabelProcessor* proc = ScanNodesByLabelProcessor::create(_pipeline, labelset);
    PipelineNodeOutputInterface& outNodeIDs = proc->outNodeIDs();

    // Allocate output node IDs column
    NamedColumn* nodeIDs = allocColumn<ColumnNodeIDs>(outNodeIDs.getDataframe());
    outNodeIDs.setNodeIDs(nodeIDs);

    // Register output in materialize data
    _matProc->getMaterializeData().addToStep<ColumnNodeIDs>(nodeIDs);

    _pendingOutput.updateInterface(&outNodeIDs);

    return outNodeIDs;
}

PipelineValueOutputInterface& PipelineBuilder::addLoadGraph(std::string_view graphName) {
    LoadGraphProcessor* loadGraph = LoadGraphProcessor::create(_pipeline, graphName);
    
    PipelineValueOutputInterface& output = loadGraph->output();

    Dataframe* df = output.getDataframe();
    NamedColumn* graphNameValue = allocColumn<ColumnConst<types::String::Primitive>>(df);
    graphNameValue->rename("graphName");
    output.setValue(graphNameValue);

    _pendingOutput.setInterface(&output);

    return output;
}

PipelineValueOutputInterface& PipelineBuilder::addLoadGML(std::string_view graphName, const fs::Path& filePath) {
    LoadGMLProcessor* loadGML = LoadGMLProcessor::create(_pipeline, graphName, filePath);
    
    PipelineValueOutputInterface& output = loadGML->output();

    Dataframe* df = output.getDataframe();
    NamedColumn* graphNameValue = allocColumn<ColumnConst<types::String::Primitive>>(df);
    graphNameValue->rename("graphName");
    output.setValue(graphNameValue);

    _pendingOutput.setInterface(&output);

    return output;
}

PipelineValueOutputInterface& PipelineBuilder::addLoadNeo4j(std::string_view graphName, const fs::Path& path) {
    LoadNeo4jProcessor* proc = LoadNeo4jProcessor::create(_pipeline, path, graphName);

    PipelineValueOutputInterface& output = proc->output();

    Dataframe* df = output.getDataframe();
    NamedColumn* graphNameValue = allocColumn<ColumnConst<types::String::Primitive>>(df);
    graphNameValue->rename("graphName");
    output.setValue(graphNameValue);

    _pendingOutput.setInterface(&output);

    return output;
}

PipelineBlockOutputInterface& PipelineBuilder::addFilter(ExprProgram* exprProg) {
    FilterProcessor* filterProc = FilterProcessor::create(_pipeline, exprProg);

    PipelineBlockInputInterface& input = filterProc->input();
    PipelineBlockOutputInterface& output = filterProc->output();

    _pendingOutput.connectTo(input);

    duplicateDataframeShape(_mem, _dfMan, input.getDataframe(), output.getDataframe());

    output.setStream(_pendingOutput.getInterface()->getStream());
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
    const bool hasInput = _pendingOutput.getInterface();
    auto* processor = WriteProcessor::create(_pipeline, hasInput);
    if (hasInput) {
        _pendingOutput.connectTo(processor->input());
    }

    PipelineBlockOutputInterface& output = processor->output();
    Dataframe* outDf = output.getDataframe();

    // Track node vars -> columns allocd to register edge src/tgt cols
    std::unordered_map<std::string_view, ColumnTag> varToCol;
    varToCol.reserve(pendingNodes.size());

    // Register columns for deleted nodes and edges
    if (hasInput) {
        PipelineBlockInputInterface& input = processor->input();
        Dataframe* inDf = input.getDataframe();

        for (const ColumnTag deletedNodeCol : nodeColumnsToDelete) {
            NamedColumn* inputColumnToDelete = inDf->getColumn(deletedNodeCol);
            // TODO: Make exception?
            bioassert(inputColumnToDelete, "input column not found");

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
            bioassert(inputColumnToDelete, "input column not found");

            // Skip if the column already exists for cases like MATCH (n) DELETE n, n
            if (outDf->getColumn(inputColumnToDelete->getTag())) {
                continue;
            }
            outDf->addColumn(inputColumnToDelete);
        }
        processor->setDeletedEdges(edgeColumnsToDelete);
    }

    // Register columns for newly created nodes/edges
    for (WriteProcessorTypes::PendingNode& node : pendingNodes) {
        // New column for each new node to create
        NamedColumn* newCol = allocColumn<ColumnNodeIDs>(outDf);
        // Rename our column with the variable name of the created node
        newCol->rename(node._name);
        // Set the tag so the WriteProcessor knows which column to write into
        node._tag = newCol->getTag();
        varToCol[node._name] = node._tag;
    }
    processor->setPendingNodes(pendingNodes);

    for (WriteProcessorTypes::PendingEdge& edge : pendingEdges) {
        // New column for each new edge to create
        NamedColumn* newCol = allocColumn<ColumnEdgeIDs>(outDf);
        // Rename our column with the variable name of the created edge
        newCol->rename(edge._name);
        // Set the tag so the WriteProcessor knows which column to write into
        edge._tag = newCol->getTag();
        edge._srcTag = edge._srcTag.isValid() ? edge._srcTag : varToCol[edge._srcName];
        edge._tgtTag = edge._tgtTag.isValid() ? edge._tgtTag : varToCol[edge._tgtName];
    }
    processor->setPendingEdges(pendingEdges);

    if (hasInput) {
        PipelineBlockInputInterface& input = processor->input();
        // This function already handles duplicates, so call it after adding all our columns
        input.propagateColumns(output);
    }

    _pendingOutput.updateInterface(&output);
    return output;
}

PipelineValueOutputInterface& PipelineBuilder::addListGraph() {
    ListGraphProcessor* loadGraph = ListGraphProcessor::create(_pipeline);

    PipelineValueOutputInterface& output = loadGraph->output();
    Dataframe* df = output.getDataframe();
    NamedColumn* graphNameValue = allocColumn<ColumnVector<types::String::Primitive>>(df);
    graphNameValue->rename("graphName");
    output.setValue(graphNameValue);

    _pendingOutput.setInterface(&output);

    return output;
}

PipelineValueOutputInterface& PipelineBuilder::addCreateGraph(std::string_view graphName) {
    CreateGraphProcessor* loadGraph = CreateGraphProcessor::create(_pipeline, graphName);

    PipelineValueOutputInterface& output = loadGraph->output();
    Dataframe* df = output.getDataframe();
    NamedColumn* graphNameValue = allocColumn<ColumnConst<types::String::Primitive>>(df);
    graphNameValue->rename("graphName");
    output.setValue(graphNameValue);

    _pendingOutput.setInterface(&output);

    return output;
}

void PipelineBuilder::addS3Connect(std::string_view accessId,
                                   std::string_view secretKey,
                                   std::string_view region) {
    auto* connectProc = S3ConnectProcessor::create(_pipeline,
                                                   accessId,
                                                   secretKey,
                                                   region);

    PipelineValueOutputInterface& output = connectProc->output();
    _pendingOutput.setInterface(&output);
}

void PipelineBuilder::addS3Pull(std::string_view s3Bucket,
                                std::string_view s3Prefix,
                                std::string_view s3File,
                                std::string_view localPath) {
    auto* pullProc = S3PullProcessor::create(_pipeline,
                                             s3Bucket,
                                             s3Prefix,
                                             s3File,
                                             localPath);

    PipelineValueOutputInterface& output = pullProc->output();

    _pendingOutput.setInterface(&output);
}

void PipelineBuilder::addS3Push(std::string_view s3Bucket,
                                std::string_view s3Prefix,
                                std::string_view s3File,
                                std::string_view localPath) {
    auto* pushProc = S3PushProcessor::create(_pipeline,
                                             s3Bucket,
                                             s3Prefix,
                                             s3File,
                                             localPath);

    PipelineValueOutputInterface& output = pushProc->output();

    _pendingOutput.setInterface(&output);
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
