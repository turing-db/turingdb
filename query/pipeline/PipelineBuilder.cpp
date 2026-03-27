#include "PipelineBuilder.h"

#include "PipelinePort.h"
#include "columns/AllowedKinds.h"
#include "columns/Column.h"
#include "columns/ColumnOperatorDispatcher.h"
#include "interfaces/PipelineOutputInterface.h"
#include "interfaces/PipelineValuesInputInterface.h"
#include "metadata/PropertyType.h"
#include "processors/CartesianProductProcessor.h"
#include "processors/ChangeProcessor.h"
#include "processors/CommitProcessor.h"
#include "processors/IndexLookupProcessor.h"
#include "processors/LoadCommitProcessor.h"
#include "processors/CallProcedureProcessor.h"
#include "processors/ForkProcessor.h"
#include "processors/HashJoinProcessor.h"
#include "processors/ExprProgram.h"
#include "processors/OrderByProcessor.h"
#include "processors/ScanNodesProcessor.h"
#include "processors/ScanNodesByLabelProcessor.h"
#include "processors/ConstScanProcessor.h"
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
#include "processors/ShowProceduresProcessor.h"
#include "processors/ShowExtensionsProcessor.h"
#include "processors/WriteProcessorTypes.h"
#include "processors/GetLabelSetIDProcessor.h"
#include "processors/GetEdgeTypeIDProcessor.h"
#include "processors/LoadGraphProcessor.h"
#include "processors/CreateGraphProcessor.h"
#include "processors/LoadGMLProcessor.h"
#include "processors/LoadJsonlProcessor.h"
#include "processors/S3ConnectProcessor.h"
#include "processors/S3PullProcessor.h"
#include "processors/S3PushProcessor.h"
#include "processors/ExprEvalProcessor.h"
#include "processors/FilterProcessor.h"
#include "processors/ShortestPathProcessor.h"
#include "processors/CreateVectorIndexProcessor.h"
#include "processors/LoadVectorProcessor.h"
#include "processors/VectorSearchProcessor.h"
#include "processors/DeleteVectorIndexProcessor.h"
#include "processors/ShowVectorIndexesProcessor.h"
#include "processors/InstallExtensionProcessor.h"
#include "processors/PathExplorerProcessor.h"
#include "processors/ConstWriteSourceProcessor.h"
#include "processors/CreateNodePropertyIndexProcessor.h"

#include "interfaces/PipelineBlockOutputInterface.h"
#include "interfaces/PipelineEdgeInputInterface.h"
#include "interfaces/PipelineValuesOutputInterface.h"
#include "EntityOutputStream.h"

#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "columns/ColumnEdgeTypes.h"
#include "columns/ColumnDispatcher.h"
#include "TypeUtils.h"

#include "dataframe/ColumnTag.h"
#include "dataframe/NamedColumn.h"
#include "versioning/ChangeID.h"

using namespace db;

namespace {

void populateStringTableShape(LocalMemory* mem,
                              Column* dest,
                              const Column* src) {
    if (dest->getKind() != ColumnStringTable::staticKind()) {
        return;
    }
    const auto* srcTable = static_cast<const ColumnStringTable*>(src);
    auto* dstTable = static_cast<ColumnStringTable*>(dest);
    for (size_t i = 0; i < srcTable->getFieldCount(); i++) {
        dstTable->addFieldColumn(
            mem->alloc<ColumnStringTable::StringColumn>());
    }
}

void duplicateDataframeShape(LocalMemory* mem,
                             DataframeManager* dfMan,
                             db::Dataframe* src,
                             db::Dataframe* dest) {
    for (const NamedColumn* col : src->cols()) {
        Column* newCol = mem->allocSame(col->getColumn());
        populateStringTableShape(mem, newCol, col->getColumn());
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
        populateStringTableShape(mem, newCol, col->getColumn());
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
        populateStringTableShape(mem, newCol, col->getColumn());
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
        populateStringTableShape(mem, newCol, col->getColumn());
        auto* newNamedCol = NamedColumn::create(dfMan, newCol, col->getTag());
        dest->addColumn(newNamedCol);
    }

    // allocate a join-key column tag in the output with a new column tag if the left
    // and right join column tags aren't the same
    const ColumnTag joinColTag = leftJoinKeyTag == rightJoinKeyTag ? leftJoinKeyTag : dfMan->allocTag();
    const Column* joinSrcCol = leftSrc->getColumn(leftJoinKeyTag)->getColumn();
    Column* newCol = mem->allocSame(joinSrcCol);
    populateStringTableShape(mem, newCol, joinSrcCol);
    auto* newNamedCol = NamedColumn::create(dfMan, newCol, joinColTag);
    dest->addColumn(newNamedCol);
}

//The left and right key  need to be of the same type unless they are string/string_view
template<typename LeftJoinKeyType,typename RightJoinKeyType>
concept isValidHashJoinTypes =
(std::is_same_v<LeftJoinKeyType, NodeID> ||
std::is_same_v<LeftJoinKeyType, EdgeID> ||
std::is_same_v<LeftJoinKeyType, LabelID> ||
std::is_same_v<LeftJoinKeyType, EdgeTypeID> ||
std::is_same_v<LeftJoinKeyType, PropertyTypeID> ||
std::is_same_v<LeftJoinKeyType, ValueType> ||
std::is_same_v<LeftJoinKeyType, int64_t> ||
std::is_same_v<LeftJoinKeyType, uint64_t> ||
std::is_same_v<LeftJoinKeyType, double> ||
std::is_same_v<LeftJoinKeyType, CustomBool> ||
std::is_same_v<LeftJoinKeyType, std::string> ||
std::is_same_v<LeftJoinKeyType, std::string_view>) &&
(std::is_same_v<LeftJoinKeyType,RightJoinKeyType> || Stringy<LeftJoinKeyType, RightJoinKeyType>);
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
    _lastProc = proc;

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
    _lastProc = getOutEdges;

    return output;
}

template <SupportedType T>
PipelineBlockOutputInterface& PipelineBuilder::addShortestPath(PipelineOutputInterface* rhs,
                                                               ColumnTag sourceKey,
                                                               ColumnTag targetKey,
                                                               const PropertyType& edgeType,
                                                               NamedColumn*& distCol,
                                                               NamedColumn*& pathCol) {

    ShortestPathProcessor<T>* shortestPath = ShortestPathProcessor<T>::create(_pipeline,
                                                                              _mem,
                                                                              sourceKey,
                                                                              targetKey,
                                                                              edgeType);

    if (_pendingOutput.getDataframe()->hasColumn(sourceKey)) {
        _pendingOutput.connectTo(shortestPath->leftHandSide());
        rhs->connectTo(shortestPath->rightHandSide());
    } else {
        _pendingOutput.connectTo(shortestPath->rightHandSide());
        rhs->connectTo(shortestPath->leftHandSide());
    }

    PipelineBlockOutputInterface& output = shortestPath->output();
    Dataframe* outDf = output.getDataframe();
    distCol = allocColumn<ColumnVector<typename T::Primitive>>(outDf);
    pathCol = allocColumn<ColumnVector<Path>>(outDf);

    shortestPath->addDistVarTag(distCol->getTag());
    shortestPath->addPathVarTag(pathCol->getTag());

    _pendingOutput.updateInterface(&output);

    _lastProc = shortestPath;
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

    _lastProc = cartProd;
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

    _lastProc = getInEdges;
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

    _lastProc = getEdges;
    return output;
}

template <PathExplorationDir Dir>
PipelineBlockOutputInterface& PipelineBuilder::addPathExplorer(uint64_t minHops, uint64_t maxHops) {

    auto* proc = PathExplorerProcessor<Dir>::create(_pipeline, minHops, maxHops);

    PipelineNodeInputInterface& input = proc->input();
    PipelineBlockOutputInterface& output = proc->output();

    _pendingOutput.connectTo(input);
    input.propagateColumns(output);

    Dataframe* outDf = output.getDataframe();

    NamedColumn* targetNodes = allocColumn<ColumnNodeIDs>(outDf);
    NamedColumn* pathCol = allocColumn<ColumnVector<EntityList>>(outDf);
    const NamedColumn* indices = allocColumn<ColumnIndices>(outDf);

    proc->setOutputIndicesColumn(indices->as<ColumnIndices>());
    proc->setOutputTargetsColumn(targetNodes);
    proc->setOutputPathsColumn(pathCol);
    proc->setBfsIndicesColumn(_mem->alloc<ColumnIndices>());
    proc->setBfsEdgesColumn(_mem->alloc<ColumnEdgeIDs>());
    proc->setBfsIntermediatesColumn(_mem->alloc<ColumnNodeIDs>());
    proc->setBfsSourcesColumn(_mem->alloc<ColumnNodeIDs>());

    output.setStream(EntityOutputStream::createNodeStream(targetNodes->getTag()));

    MaterializeData& matData = _matProc->getMaterializeData();
    matData.createStep(indices);
    matData.addToStep<ColumnNodeIDs>(targetNodes);
    matData.addToStep<ColumnVector<EntityList>>(pathCol);

    _pendingOutput.updateInterface(&output);

    _lastProc = proc;
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

    _lastProc = fork;
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
    _lastProc = lambda;
}

PipelineBlockOutputInterface& PipelineBuilder::addSkip(size_t count) {
    SkipProcessor* skip = SkipProcessor::create(_pipeline, count);

    auto& input = skip->input();
    auto& output = skip->output();

    _pendingOutput.connectTo(input);
    output.setStream(input.getStream());
    duplicateDataframeShape(_mem, _dfMan, input.getDataframe(), output.getDataframe());

    _pendingOutput.updateInterface(&output);

    _lastProc = skip;
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

    _lastProc = limit;
    return limit->output();
}

PipelineValueOutputInterface& PipelineBuilder::addCount(ColumnTag colTag) {
    if (!_pendingOutput.getInterface()) {
        throw FatalException("COUNT had no input.");
    }

    CountProcessor* count = CountProcessor::create(_pipeline, colTag);

    PipelineBlockInputInterface countIn = count->input();
    PipelineValueOutputInterface countOut = count->output();
    Dataframe* countOutDf = countOut.getDataframe();

    _pendingOutput.connectTo(countIn);

    countIn.propagateColumns(countOut);

    NamedColumn* countColumn =
        allocColumn<ColumnConst<CountProcessor::CountType>>(countOutDf);
    count->output().setValue(countColumn);

    _pendingOutput.updateInterface(&count->output());
    _lastProc = count;
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

        if (!col) {
            throw FatalException(fmt::format("projection variable {} not found in output column", item._name));
        }

        if (NamedColumn* existingCol = outDf->getColumn(col->getTag())) {
            // Column is already present, duplicate it under another name
            col = NamedColumn::create(_dfMan, existingCol->getColumn(), _dfMan->allocTag());
        }

        outDf->addColumn(col);

        if (!item._name.empty()) {
            col->rename(item._name);
        }
    }

    _pendingOutput.updateInterface(&output);

    _lastProc = projection;
    return output;
}

PipelineBlockOutputInterface& PipelineBuilder::addOrderBy(std::span<const OrderByProcessor::OrderByKey> keys) {
    OrderByProcessor* orderby = OrderByProcessor::create(_pipeline, keys);

    PipelineBlockInputInterface& input = orderby->input();
    PipelineBlockOutputInterface& output = orderby->output();

    _pendingOutput.connectTo(input);

    Dataframe* inputDf = input.getDataframe();
    Dataframe* outputDf = output.getDataframe();

    // Do not sort in place: input remains immutable, copy sorted columns into output of
    // same shape
    duplicateDataframeShape(_mem, _dfMan, inputDf, outputDf);

    // Allocate a column to store the indices for sort projection. No need to store it in
    // any dataframe.
    auto* indicesCol = _mem->alloc<ColumnVector<size_t>>();
    orderby->setIndicesCol(indicesCol);

    // Duplicate the shape of the output dataframe to the processors "memory"
    // TODO: Can we only store the returned/needed columns in memory?
    duplicateDataframeShape(_mem, _dfMan, outputDf, &orderby->memory());

    // Stream does not change when adding ORDER BY
    output.setStream(_pendingOutput.getInterface()->getStream());
    _pendingOutput.updateInterface(&output);

    return orderby->output();
}

PipelineBlockOutputInterface& PipelineBuilder::addHashJoin(PipelineOutputInterface* rhs,
                                                           ColumnTag leftJoinKey,
                                                           ColumnTag rightJoinKey) {
    const Dataframe* pendingDf = _pendingOutput.getDataframe();
    Column* leftKeyCol = pendingDf->getColumn(leftJoinKey)->getColumn();

    const Dataframe* rhsDf = rhs->getDataframe();
    Column* rightKeyCol = rhsDf->getColumn(rightJoinKey)->getColumn();

    {

        if (leftKeyCol->getContainerKind() != ContainerKind::code<ColumnVector>()) {
            throw PipelineException("Can only join on ColumnVectors");
        }

        if (rightKeyCol->getContainerKind() != ContainerKind::code<ColumnVector>()) {
            throw PipelineException("Can only join on ColumnVectors");
        }
    }

    // Do the hash join connectivity and setup interfaces
    auto setupJoin = [&](auto* join) -> PipelineBlockOutputInterface& {
        _pendingOutput.connectTo(join->leftInput());
        rhs->connectTo(join->rightInput());

        const Dataframe* leftDf = join->leftInput().getDataframe();
        const Dataframe* rightDf = join->rightInput().getDataframe();

        PipelineBlockOutputInterface& outInterface = join->output();
        Dataframe* outDf = outInterface.getDataframe();

        createHashJoinDataFrameShape(_mem, _dfMan, leftDf, rightDf, outDf,
                                     leftJoinKey, rightJoinKey);

        _pendingOutput.setInterface(&outInterface);

        _lastProc = join;
        return outInterface;
    };

    // Create the join depending on the type combination of the keys
    PipelineBlockOutputInterface* result = nullptr;

    dispatchColumnVector(leftKeyCol, [&](auto* typedCol) {
        using LeftJoinKeyType = typename std::decay_t<decltype(*typedCol)>::ValueType;
        using UnwrappedLeftKey = TypeUtils::unwrap_optional_t<LeftJoinKeyType>;

        dispatchColumnVector(rightKeyCol, [&](auto* rightTypedCol) {
            using RightJoinKeyType = typename std::decay_t<decltype(*rightTypedCol)>::ValueType;
            using UnwrappedRightKey = TypeUtils::unwrap_optional_t<RightJoinKeyType>;

            if constexpr ((isValidHashJoinTypes<UnwrappedLeftKey, UnwrappedRightKey>)) {
                auto* join = HashJoinProcessor<LeftJoinKeyType, RightJoinKeyType>::create(_pipeline,
                                                                                          leftJoinKey,
                                                                                          rightJoinKey);
                result = &setupJoin(join);
            } else {
                throw PipelineException("Incompatible right key type for string hash join");
            }
        });
    });

    bioassert(result, "HashJoin Processor Result Is A Null Pointer");
    return *result;
}

PipelineBlockOutputInterface& PipelineBuilder::addLambdaSource(const LambdaSourceProcessor::Callback& callback) {
    LambdaSourceProcessor* source = LambdaSourceProcessor::create(_pipeline, callback);
    _pendingOutput.updateInterface(&source->output());
    _lastProc = source;
    return source->output();
}

PipelineBlockOutputInterface& PipelineBuilder::addCallProcedure(const Procedure* procedure,
                                                                std::span<const Procedure::Argument> args,
                                                                std::span<Procedure::YieldItem> yield) {
    const bool hasInput = _pendingOutput.getInterface() != nullptr;

    CallProcedureProcessor* proc = CallProcedureProcessor::create(_pipeline, procedure, hasInput);
    auto& output = proc->output();

    if (hasInput) {
        auto& input = proc->input();
        _pendingOutput.connectTo(input);
        input.propagateColumns(output);
    }

    _pendingOutput.updateInterface(&output);

    proc->setInputValues(args);
    proc->allocReturnValues(_mem, _dfMan, yield);

    _lastProc = proc;
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

    _lastProc = proc;
    return output;
}

PipelineBlockOutputInterface& PipelineBuilder::addCommit() {
    CommitProcessor* proc = CommitProcessor::create(_pipeline);
    auto& output = proc->output();

    _pendingOutput.updateInterface(&output);

    _lastProc = proc;
    return output;
}

PipelineBlockOutputInterface& PipelineBuilder::addLoadCommit(std::string_view hashStr) {
    LoadCommitProcessor* proc = LoadCommitProcessor::create(_pipeline, hashStr);
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

    _lastProc = transf;
    return output;
}

PipelineBlockOutputInterface& PipelineBuilder::addExprEval(ExprProgram* exprProg) {
    const bool hasInput = _pendingOutput.getInterface();
    ExprEvalProcessor* proc = ExprEvalProcessor::create(_pipeline, exprProg, hasInput);

    PipelineBlockOutputInterface& output = proc->output();

    if (hasInput) {
        PipelineBlockInputInterface& input = proc->input();
        _pendingOutput.connectTo(proc->input());
        input.propagateColumns(output);
    }

    _pendingOutput.updateInterface(&output);

    _lastProc = proc;
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

    _lastProc = proc;
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
    _lastProc = proc;
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

    _lastProc = proc;
    return outNodeIDs;
}

struct AddMaterializeStep {
    template <typename T>
    void operator()(const ColumnVector<T>*) {
        bioassert(_matProc, "Null MaterializeProcessor.");
        bioassert(_namedCol, "Null NamedColumn.");
        _matProc->getMaterializeData().addToStep<ColumnVector<T>>(_namedCol);
    }

    MaterializeProcessor* _matProc {nullptr};
    NamedColumn* _namedCol {nullptr};
};

struct db::AllocOutputColumn {
    template <typename T>
    void operator()(const ColumnVector<T>*) {
        bioassert(_outputDF, "Null output dataframe.");
        bioassert(_builder, "Null PipelineBuilder.");
        
        _newCol = _builder->allocColumn<ColumnVector<T>>(_outputDF);
    }

    PipelineBuilder* _builder {nullptr};
    Dataframe* _outputDF {nullptr};
    NamedColumn*& _newCol;
};

struct SetStream {
    void operator()(const ColumnVector<NodeID>*) {
        const EntityOutputStream stream = EntityOutputStream::createNodeStream(_tag);
        _out->setStream(stream);
    }

    // NOTE: If we support const edge scan, we need to set a node stream here

    template <typename T>
    void operator()(const ColumnVector<T>*) {}

    PipelineOutputInterface* _out {nullptr};
    ColumnTag _tag;
};

PipelineValuesOutputInterface& PipelineBuilder::addConstScan(Column* values) {

    ConstScanProcessor* proc = ConstScanProcessor::create(_pipeline, values);

    PipelineValuesOutputInterface& outValues = proc->output();

    // Allocate output column
    NamedColumn* outputCol {nullptr};
    {
        using Types = ConstScanTypes;
        using Dispatcher =
            ColumnSingleDispatcher<Types::Allowed, AllocOutputColumn, Types::Excluded>;
        AllocOutputColumn allocator {._builder = this,
                                     ._outputDF = outValues.getDataframe(),
                                     ._newCol = outputCol};
        Dispatcher::dispatch(values, allocator);
    }
    outValues.setValues(outputCol);

    { // Register output in materialize data
        using Types = ConstScanTypes;
        using Dispatcher =
            ColumnSingleDispatcher<Types::Allowed, AddMaterializeStep, Types::Excluded>;
        AddMaterializeStep matStep {._matProc = _matProc, ._namedCol = outputCol};
        Dispatcher::dispatch(values, matStep);
    }

    { // Set the stream if we need to (e.g. a ConstScan of NodeIDs)
        using Types = ConstScanTypes;
        using Dispatcher =
            ColumnSingleDispatcher<Types::Allowed, SetStream, Types::Excluded>;

        SetStream stream {._out = &outValues, ._tag = outputCol->getTag()};

        Dispatcher::dispatch(values, stream);
    }

    _pendingOutput.updateInterface(&outValues);
    _lastProc = proc;
    return outValues;
}

PipelineBuilder::ConstWriteSourceOutput PipelineBuilder::addConstWriteSource(Column* nodeIDs,
                                                                             Column* values) {
    auto* proc = ConstWriteSourceProcessor::create(_pipeline, nodeIDs, values);
    PipelineBlockOutputInterface& out = proc->output();

    // Allocate NodeID output column
    NamedColumn* nodeIDCol = allocColumn<ColumnNodeIDs>(out.getDataframe());

    // Allocate values output column (dispatch on input type)
    NamedColumn* valuesCol {nullptr};
    {
        using Types = WriteProcessorPropertyTypes;
        using Dispatcher = ColumnSingleDispatcher<Types::AllowedVector, AllocOutputColumn, Types::ExcludedVector>;
        AllocOutputColumn allocator {._builder = this,
                                     ._outputDF = out.getDataframe(),
                                     ._newCol = valuesCol};
        Dispatcher::dispatch(values, allocator);
    }

    proc->_nodeIDOutputCol = nodeIDCol;
    proc->_valuesOutputCol = valuesCol;

    // Set stream as a node stream
    const EntityOutputStream stream = EntityOutputStream::createNodeStream(nodeIDCol->getTag());
    out.setStream(stream);

    _pendingOutput.updateInterface(&out);
    _lastProc = proc;
    return {nodeIDCol, valuesCol};
}

PipelineValueOutputInterface& PipelineBuilder::addLoadGraph(std::string_view graphName) {
    LoadGraphProcessor* loadGraph = LoadGraphProcessor::create(_pipeline, graphName);
    
    PipelineValueOutputInterface& output = loadGraph->output();

    Dataframe* df = output.getDataframe();
    NamedColumn* graphNameValue = allocColumn<ColumnConst<types::String::Primitive>>(df);
    graphNameValue->rename("graphName");
    output.setValue(graphNameValue);

    _pendingOutput.setInterface(&output);

    _lastProc = loadGraph;
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

    _lastProc = loadGML;
    return output;
}

PipelineValueOutputInterface& PipelineBuilder::addLoadJsonl(std::string_view graphName, const fs::Path& path) {
    LoadJsonlProcessor* proc = LoadJsonlProcessor::create(_pipeline, path, graphName);

    PipelineValueOutputInterface& output = proc->output();

    Dataframe* df = output.getDataframe();
    NamedColumn* graphNameValue = allocColumn<ColumnConst<types::String::Primitive>>(df);
    graphNameValue->rename("graphName");
    output.setValue(graphNameValue);

    _pendingOutput.setInterface(&output);

    _lastProc = proc;
    return output;
}

PipelineBlockOutputInterface& PipelineBuilder::addFilter(PredicateProgram* predProg) {
    FilterProcessor* filterProc = FilterProcessor::create(_pipeline, predProg);

    PipelineBlockInputInterface& input = filterProc->input();
    PipelineBlockOutputInterface& output = filterProc->output();

    _pendingOutput.connectTo(input);

    duplicateDataframeShape(_mem, _dfMan, input.getDataframe(), output.getDataframe());

    output.setStream(_pendingOutput.getInterface()->getStream());
    _pendingOutput.updateInterface(&output);

    _lastProc = filterProc;
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

    _lastProc = getProps;
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

    _lastProc = getProps;
    return output;
}

PipelineBlockOutputInterface& PipelineBuilder::addWrite(ExprProgram* exprProg,
                                                        const WriteProcessor::DeletedNodes& nodeColumnsToDelete,
                                                        const WriteProcessor::DeletedEdges& edgeColumnsToDelete,
                                                        WriteProcessor::PendingNodes& pendingNodes,
                                                        WriteProcessor::PendingEdges& pendingEdges,
                                                        const WriteProcessor::UpdatedNodes& nodeUpdates,
                                                        const WriteProcessor::UpdatedEdges& edgeUpdates) {
    const bool hasInput = _pendingOutput.getInterface();
    auto* processor = WriteProcessor::create(_pipeline, exprProg, hasInput);
    if (hasInput) {
        _pendingOutput.connectTo(processor->tryGetInput());
    }

    PipelineBlockOutputInterface& output = processor->output();
    Dataframe* outDf = output.getDataframe();

    // Track node vars -> columns allocd to register edge src/tgt cols
    std::unordered_map<std::string_view, ColumnTag> varToCol;
    varToCol.reserve(pendingNodes.size());

    // Register columns for deleted nodes and edges
    if (hasInput) {
        const PipelineBlockInputInterface& input = processor->tryGetInput();
        const Dataframe* inDf = input.getDataframe();

        for (const ColumnTag deletedNodeCol : nodeColumnsToDelete) {
            NamedColumn* inputColumnToDelete = inDf->getColumn(deletedNodeCol);
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
            bioassert(inputColumnToDelete, "input column not found");

            // Skip if the column already exists for cases like MATCH (n) DELETE n, n
            if (outDf->getColumn(inputColumnToDelete->getTag())) {
                continue;
            }
            outDf->addColumn(inputColumnToDelete);
        }
        processor->setDeletedEdges(edgeColumnsToDelete);

        for (const auto& [propUpdate, srcTag] : nodeUpdates) {
            const auto& [propName, vt, valueCol, pid] = propUpdate;

            const NamedColumn* updateCol = inDf->getColumn(srcTag);
            bioassert(updateCol, "Failed to get node column to update.");

            bioassert(valueCol, "Failed to get value column for node update");
        }

        for (const auto& [propUpdate, srcTag] : edgeUpdates) {
            const auto& [propName, vt, valueCol, pid] = propUpdate;

            const NamedColumn* updateCol = inDf->getColumn(srcTag);
            bioassert(updateCol, "Failed to get edge column to update.");

            bioassert(valueCol, "Failed to get value column for edge update");
        }

        processor->setNodeUpdates(nodeUpdates);
        processor->setEdgeUpdates(edgeUpdates);
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
        PipelineBlockInputInterface& input = processor->tryGetInput();
        // This function already handles duplicates, so call it after adding all our columns
        input.propagateColumns(output);
    }

    _pendingOutput.updateInterface(&output);
    _lastProc = processor;
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

    _lastProc = loadGraph;
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

    _lastProc = loadGraph;
    return output;
}

PipelineValueOutputInterface& PipelineBuilder::addInstallExtension(std::string_view extensionName) {
    auto* proc = InstallExtensionProcessor::create(_pipeline, extensionName);

    PipelineValueOutputInterface& output = proc->output();
    Dataframe* df = output.getDataframe();
    NamedColumn* nameValue = allocColumn<ColumnConst<types::String::Primitive>>(df);
    nameValue->rename("extensionName");
    output.setValue(nameValue);

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
    _lastProc = connectProc;
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
    _lastProc = pullProc;
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
    _lastProc = pushProc;
}

PipelineBlockOutputInterface& PipelineBuilder::addShowProcedures() {
    ShowProceduresProcessor* proc = ShowProceduresProcessor::create(_pipeline);

    PipelineBlockOutputInterface& output = proc->output();
    Dataframe* df = output.getDataframe();

    NamedColumn* nameCol = allocColumn<ColumnVector<types::String::Primitive>>(df);
    nameCol->rename("name");
    proc->setNameColumn(nameCol);

    NamedColumn* signatureCol = allocColumn<ColumnVector<std::string>>(df);
    signatureCol->rename("signature");
    proc->setSignatureColumn(signatureCol);

    _pendingOutput.setInterface(&output);

    _lastProc = proc;
    return output;
}

PipelineBlockOutputInterface& PipelineBuilder::addShowExtensions() {
    ShowExtensionsProcessor* proc = ShowExtensionsProcessor::create(_pipeline);

    PipelineBlockOutputInterface& output = proc->output();
    Dataframe* df = output.getDataframe();

    NamedColumn* nameCol = allocColumn<ColumnVector<types::String::Primitive>>(df);
    nameCol->rename("name");
    proc->setNameColumn(nameCol);

    _pendingOutput.setInterface(&output);

    return output;
}

PipelineValueOutputInterface& PipelineBuilder::addCreateVectorIndex(std::string_view indexName,
                                                                    vec::Dimension dimension,
                                                                    vec::DistanceMetric metric) {
    CreateVectorIndexProcessor* proc =
        CreateVectorIndexProcessor::create(_pipeline,
                                           indexName,
                                           dimension,
                                           metric);

    PipelineValueOutputInterface& output = proc->output();
    Dataframe* df = output.getDataframe();

    NamedColumn* indexNameCol = allocColumn<ColumnConst<types::String::Primitive>>(df);
    indexNameCol->rename("indexName");
    output.setValue(indexNameCol);

    _pendingOutput.setInterface(&output);
    return output;
}

PipelineValueOutputInterface& PipelineBuilder::addLoadVector(std::string_view filePath,
                                                              std::string_view indexName) {
    LoadVectorProcessor* proc = LoadVectorProcessor::create(_pipeline, filePath, indexName);

    PipelineValueOutputInterface& output = proc->output();
    Dataframe* df = output.getDataframe();

    NamedColumn* countCol = allocColumn<ColumnConst<types::UInt64::Primitive>>(df);
    countCol->rename("count");
    output.setValue(countCol);

    _pendingOutput.setInterface(&output);
    return output;
}

PipelineValuesOutputInterface& PipelineBuilder::addVectorSearch(std::string_view indexName,
                                                                uint64_t k,
                                                                const std::vector<float>& queryVector) {
    VectorSearchProcessor* proc = VectorSearchProcessor::create(_pipeline, indexName, k, queryVector);

    PipelineValuesOutputInterface& output = proc->outIds();
    Dataframe* df = output.getDataframe();

    NamedColumn* idsCol = allocColumn<ColumnVector<types::Int64::Primitive>>(df);
    idsCol->rename("ids");
    output.setValues(idsCol);

    _pendingOutput.setInterface(&output);
    return output;
}

PipelineValueOutputInterface& PipelineBuilder::addDeleteVectorIndex(std::string_view indexName) {
    DeleteVectorIndexProcessor* proc = DeleteVectorIndexProcessor::create(_pipeline, indexName);

    PipelineValueOutputInterface& output = proc->output();
    Dataframe* df = output.getDataframe();

    NamedColumn* nameCol = allocColumn<ColumnConst<types::String::Primitive>>(df);
    nameCol->rename("indexName");
    output.setValue(nameCol);

    _pendingOutput.setInterface(&output);
    return output;
}

PipelineValuesOutputInterface& PipelineBuilder::addShowVectorIndexes() {
    ShowVectorIndexesProcessor* proc = ShowVectorIndexesProcessor::create(_pipeline);

    PipelineValuesOutputInterface& output = proc->outNames();
    Dataframe* df = output.getDataframe();

    NamedColumn* namesCol = allocColumn<ColumnVector<types::String::Primitive>>(df);
    namesCol->rename("name");
    output.setValues(namesCol);

    // Add dimensions column to the same dataframe
    NamedColumn* dimsCol = allocColumn<ColumnVector<types::UInt64::Primitive>>(df);
    dimsCol->rename("dimension");

    // Set up the dimensions output with the same dataframe's column
    PipelineValuesOutputInterface& dimsOutput = proc->outDimensions();
    dimsOutput.setValues(dimsCol);

    _pendingOutput.setInterface(&output);
    return output;
}

PipelineBlockOutputInterface& PipelineBuilder::addCreateNodePropertyIndex(std::string_view indexName,
                                                                          std::string_view propName) {
    auto* proc = CreateNodePropertyIndexProcessor::create(_pipeline, indexName, propName);

    PipelineBlockOutputInterface& output = proc->output();

    _pendingOutput.setInterface(&output);
    _lastProc = proc;
    return output;
}

template <typename Q, typename R>
PipelineValuesOutputInterface& PipelineBuilder::addIndexLookup(const Index* index) {
    using ResultColumn = ColumnVector<R>;

    auto* proc = IndexLookupProcessor<Q, R>::create(_pipeline, index);

    PipelineValuesInputInterface& input = proc->input();
    PipelineValuesOutputInterface& output = proc->output();

    _pendingOutput.connectTo(input);
    input.propagateColumns(output);

    {
        Dataframe* outputDF = output.getDataframe();
        NamedColumn* result = allocColumn<ResultColumn>(outputDF);
        output.setValues(result);
    }

    _pendingOutput.updateInterface(&output);
    _lastProc = proc;
    return output;
}

void PipelineBuilder::setOutputDataframe(const Dataframe* df) {
    _pipeline->setOutputDataframe(df);
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
template PipelineValuesOutputInterface& PipelineBuilder::addGetProperties<EntityType::Node, db::types::Embedding>(PropertyType);
template PipelineValuesOutputInterface& PipelineBuilder::addGetProperties<EntityType::Edge, db::types::Embedding>(PropertyType);

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
template PipelineValuesOutputInterface& PipelineBuilder::addGetPropertiesWithNull<EntityType::Node, db::types::Embedding>(ColumnTag, PropertyType);
template PipelineValuesOutputInterface& PipelineBuilder::addGetPropertiesWithNull<EntityType::Edge, db::types::Embedding>(ColumnTag, PropertyType);

template PipelineBlockOutputInterface& PipelineBuilder::addShortestPath<db::types::Double>(PipelineOutputInterface*,
                                                                                           ColumnTag,
                                                                                           ColumnTag,
                                                                                           const PropertyType&,
                                                                                           NamedColumn*&,
                                                                                           NamedColumn*&);
template PipelineBlockOutputInterface& PipelineBuilder::addShortestPath<db::types::UInt64>(PipelineOutputInterface*,
                                                                                           ColumnTag,
                                                                                           ColumnTag,
                                                                                           const PropertyType&,
                                                                                           NamedColumn*&,
                                                                                           NamedColumn*&);
template PipelineBlockOutputInterface& PipelineBuilder::addShortestPath<db::types::Int64>(PipelineOutputInterface*,
                                                                                          ColumnTag,
                                                                                          ColumnTag,
                                                                                          const PropertyType&,
                                                                                          NamedColumn*&,
                                                                                          NamedColumn*&);

template PipelineBlockOutputInterface& PipelineBuilder::addPathExplorer<PathExplorationDir::BOTH>(uint64_t, uint64_t);
template PipelineBlockOutputInterface& PipelineBuilder::addPathExplorer<PathExplorationDir::FORWARD>(uint64_t, uint64_t);
template PipelineBlockOutputInterface& PipelineBuilder::addPathExplorer<PathExplorationDir::BACKWARD>(uint64_t, uint64_t);

template PipelineValuesOutputInterface& PipelineBuilder::addIndexLookup<types::Int64::Primitive, NodeID>(const Index*);
template PipelineValuesOutputInterface& PipelineBuilder::addIndexLookup<types::UInt64::Primitive, NodeID>(const Index*);
template PipelineValuesOutputInterface& PipelineBuilder::addIndexLookup<types::Double::Primitive, NodeID>(const Index*);
template PipelineValuesOutputInterface& PipelineBuilder::addIndexLookup<types::Bool::Primitive, NodeID>(const Index*);
template PipelineValuesOutputInterface& PipelineBuilder::addIndexLookup<types::String::Primitive, NodeID>(const Index*);
template PipelineValuesOutputInterface& PipelineBuilder::addIndexLookup<types::Embedding::Primitive, NodeID>(const Index*);

template PipelineValuesOutputInterface& PipelineBuilder::addIndexLookup<types::Int64::Primitive, EdgeID>(const Index*);
template PipelineValuesOutputInterface& PipelineBuilder::addIndexLookup<types::UInt64::Primitive, EdgeID>(const Index*);
template PipelineValuesOutputInterface& PipelineBuilder::addIndexLookup<types::Double::Primitive, EdgeID>(const Index*);
template PipelineValuesOutputInterface& PipelineBuilder::addIndexLookup<types::Bool::Primitive, EdgeID>(const Index*);
template PipelineValuesOutputInterface& PipelineBuilder::addIndexLookup<types::String::Primitive, EdgeID>(const Index*);
template PipelineValuesOutputInterface& PipelineBuilder::addIndexLookup<types::Embedding::Primitive, EdgeID>(const Index*);
