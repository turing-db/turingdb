#include "DBLowering.h"

#include <mlir/IR/Location.h>
#include <optional>
#include <type_traits>

#include "mlir/IR/Block.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/Types.h"
#include "mlir/IR/Verifier.h"

#include "NLOps.h"
#include "DBLoweringChunkTypes.h"
#include "DBLoweringFunctionEmitters.h"
#include "DBLoweringRowFlow.h"

#include "DBColumnElement.h"
#include "IRConstantColumn.h"
#include "MergePatternShape.h"
#include "ProcedureManager.h"

#include "views/GraphView.h"
#include "metadata/GraphMetadata.h"
#include "metadata/PropertyType.h"

#include "IRValueTypes.h"

#include "IRException.h"
#include "llvm/Support/Casting.h"

using namespace db;

namespace nl = mlir::nl;
namespace storage = mlir::storage;

namespace {

bool isDefinedWithin(mlir::Value value, mlir::Operation* operation) {
    mlir::Operation* const definingOp = value.getDefiningOp();
    mlir::Operation* const definer = definingOp ? definingOp : value.getParentBlock()->getParentOp();

    return operation->isAncestor(definer);
}

template <typename Key>
void forgetValuesDefinedWithin(mlir::Operation* operation, llvm::DenseMap<Key, mlir::Value>& map) {
    map.remove_if([operation](const auto& entry) {
        return isDefinedWithin(entry.second, operation);
    });
}

// The nl sibling of each db system command. They are copied across one for one -
// same attributes under the same names, each db column result becoming the chunk
// the command fills - so the whole family lowers through one table instead of a
// branch apiece.
const llvm::StringMap<llvm::StringRef> systemCommandSiblings = {
    {mlir::db::LoadGraph::getOperationName(),           nl::LoadGraph::getOperationName()},
    {mlir::db::CreateGraph::getOperationName(),         nl::CreateGraph::getOperationName()},
    {mlir::db::ImportGraph::getOperationName(),         nl::ImportGraph::getOperationName()},
    {mlir::db::ListGraphs::getOperationName(),          nl::ListGraphs::getOperationName()},
    {mlir::db::ListAvailableGraphs::getOperationName(), nl::ListAvailableGraphs::getOperationName()},
    {mlir::db::ChangeCommand::getOperationName(),       nl::ChangeCommand::getOperationName()},
    {mlir::db::CommitChange::getOperationName(),        nl::CommitChange::getOperationName()},
    {mlir::db::LoadCommit::getOperationName(),          nl::LoadCommit::getOperationName()},
    {mlir::db::MergeDataParts::getOperationName(),      nl::MergeDataParts::getOperationName()},
    {mlir::db::S3Connect::getOperationName(),           nl::S3Connect::getOperationName()},
    {mlir::db::S3Transfer::getOperationName(),          nl::S3Transfer::getOperationName()},
    {mlir::db::ShowProcedures::getOperationName(),      nl::ShowProcedures::getOperationName()},
    {mlir::db::InstallExtension::getOperationName(),    nl::InstallExtension::getOperationName()},
    {mlir::db::ShowExtensions::getOperationName(),      nl::ShowExtensions::getOperationName()},
    {mlir::db::CreateVectorIndex::getOperationName(),   nl::CreateVectorIndex::getOperationName()},
    {mlir::db::DeleteVectorIndex::getOperationName(),   nl::DeleteVectorIndex::getOperationName()},
    {mlir::db::ShowVectorIndexes::getOperationName(),   nl::ShowVectorIndexes::getOperationName()},
    {mlir::db::LoadVector::getOperationName(),          nl::LoadVector::getOperationName()},
    {mlir::db::LoadEmbedding::getOperationName(),       nl::LoadEmbedding::getOperationName()},
    {mlir::db::CreatePropertyIndex::getOperationName(), nl::CreatePropertyIndex::getOperationName()},
    {mlir::db::DropIndex::getOperationName(),           nl::DropIndex::getOperationName()},
    {mlir::db::Explain::getOperationName(),             nl::Explain::getOperationName()},
};

}

DBLowering::DBLowering(mlir::MLIRContext* context,
                       const GraphView* view,
                       const ProcedureManager* procedures)
    : _builder(context),
    _view(view),
    _procedures(procedures)
{
}

DBLowering::~DBLowering() {
}

mlir::func::FuncOp DBLowering::lower(mlir::func::FuncOp dbFunction, mlir::ModuleOp module) {
    // Check that we didn't failed MLIR verifier
    if (mlir::failed(mlir::verify(dbFunction))) {
        throw IRException("db function failed MLIR verification");
    }

    mlir::Region& dbBody = dbFunction->getRegion(0);
    if (!dbBody.hasOneBlock()) {
        throw IRException("DBLowering expects a db function with a single block");
    }

    mlir::MLIRContext* context = _builder.getContext();
    const mlir::Location loc = _builder.getUnknownLoc();

    // Create nl target function
    llvm::SmallVector<mlir::Type> argumentTypes;
    for (const mlir::BlockArgument argument : dbFunction.getArguments()) {
        argumentTypes.push_back(nl::ChunkType::get(context, columnType(argument)));
    }

    _builder.setInsertionPointToEnd(module.getBody());
    const auto functionType = mlir::FunctionType::get(context, argumentTypes, {});
    auto nlFunction = _builder.create<mlir::func::FuncOp>(loc, dbFunction.getSymName(), functionType);
    if (const mlir::ArrayAttr argumentAttributes = dbFunction.getAllArgAttrs()) {
        nlFunction.setAllArgAttrs(argumentAttributes);
    }
    _entryBlock = nlFunction.addEntryBlock();

    // Create the ReturnOp of the target function right away
    _builder.setInsertionPointToStart(_entryBlock);
    _builder.create<mlir::func::ReturnOp>(loc);

    // Lower each operation of the db function. Top-level scans root their loop
    // in the entry block; a cross product retargets the root per factor.
    _valueMap.clear();
    _propertyTypes.clear();
    _edgeTypeSets.clear();
    _lastHoisted = nullptr;
    _constantColumns.clear();
    _rootBlock = _entryBlock;
    _innermostLoopBody = nullptr;
    _innermostRegionBody = nullptr;
    _innermostCardinality = mlir::Value();
    _limitHandles.clear();
    _loopLimits.clear();
    _optionalMatchLimits.clear();
    _sortTopK.clear();
    _fusedLimits.clear();
    _loweredAhead.clear();

    for (const auto [dbArgument, nlArgument] : llvm::zip_equal(dbFunction.getArguments(), nlFunction.getArguments())) {
        _valueMap[dbArgument] = nlArgument;
    }

    lowerBoundValuesAhead(dbFunction);

    // Find ORDER BY ... LIMIT k: a db.limit capping a db.sort's result fuses into
    // a bounded top-K, so the limit gets no streaming handle and the sort carries
    // the bound. Detect before the limit pre-scan so the fused ones are skipped.
    detectTopKFusion(dbFunction);

    hoistLimitHandles(dbFunction.getBody(), _entryBlock, nullptr);

    for (mlir::Operation& operation : dbBody.front()) {
        lowerOperation(operation);
    }

    // Peephole: a terminal LIMIT lowers to an nl.limit_truncate whose only
    // consumer is the nl.output right after it. Fold that pair into a single
    // limit-bearing nl.output, which emits the budgeted prefix off the handle
    // instead of copying it - the copy-free path for a LIMIT that feeds the sink.
    foldTruncatesIntoOutputs(nlFunction);

    // The skip sibling: a terminal SKIP folds its nl.skip_truncate into a
    // skip-bearing nl.output that emits the surviving suffix in place (at an
    // offset) instead of copying it to the front - the copy-free post-skip tail.
    foldSkipTruncatesIntoOutputs(nlFunction);

    foldBroadcastsIntoOutputs(nlFunction);

    // Run MLIR verifier on the nlFunction
    if (mlir::failed(mlir::verify(nlFunction))) {
        throw IRException("DBLowering produced an invalid nl function");
    }

    return nlFunction;
}

void DBLowering::lowerOperation(mlir::Operation& operation) {
    if (_loweredAhead.contains(&operation)) {
        return;
    }

    for (const LaidOutOperand& laidOut : llvm::reverse(_laidOutOperands)) {
        _valueMap[laidOut._operand] = laidOut._constantChunk;
    }

    if (computesOnceOverConstants(operation)) {
        lowerOnceOverConstants(operation);
    } else if (raisesOverConstants(operation)) {
        lowerOverReachingRows(operation);
    } else if (readsConstantsAsRows(operation)) {
        lowerWithConstantsLaidOut(operation, rowsReadBy(operation), [this](mlir::Value chunk) {
            return isConstantReadAsRows(chunk);
        });
    } else {
        lowerOperationByKind(operation);
    }

    for (const LaidOutOperand& laidOut : _laidOutOperands) {
        _valueMap[laidOut._operand] = laidOut._laidOutChunk;
    }
}

void DBLowering::eraseOp(mlir::Operation* operation) {
    forgetErasedValues(operation, _constantColumns);
    forgetValuesDefinedWithin(operation, _valueMap);
    forgetValuesDefinedWithin(operation, _limitHandles);

    operation->erase();
}

void DBLowering::lowerOperationByKind(mlir::Operation& operation) {
    if (mlir::db::ScanNodes scanNodes = mlir::dyn_cast<mlir::db::ScanNodes>(operation)) {
        lowerScanNodes(scanNodes);
    } else if (mlir::db::ScanNodesByLabel scanNodesByLabel = mlir::dyn_cast<mlir::db::ScanNodesByLabel>(operation)) {
        lowerScanNodesByLabel(scanNodesByLabel);
    } else if (mlir::db::ConstScanNodes constScanNodes = mlir::dyn_cast<mlir::db::ConstScanNodes>(operation)) {
        lowerConstScanNodes(constScanNodes);
    } else if (mlir::db::ScanNodesByPropertyValue scanNodesByPropertyValue = mlir::dyn_cast<mlir::db::ScanNodesByPropertyValue>(operation)) {
        lowerScanNodesByPropertyValue(scanNodesByPropertyValue);
    } else if (mlir::db::UnwindConst unwindConst = mlir::dyn_cast<mlir::db::UnwindConst>(operation)) {
        lowerUnwindConst(unwindConst);
    } else if (mlir::db::LoadCSV loadCSV = mlir::dyn_cast<mlir::db::LoadCSV>(operation)) {
        lowerLoadCSV(loadCSV);
    } else if (mlir::db::VectorSearch vectorSearch = mlir::dyn_cast<mlir::db::VectorSearch>(operation)) {
        lowerVectorSearch(vectorSearch);
    } else if (mlir::db::Unwind unwind = mlir::dyn_cast<mlir::db::Unwind>(operation)) {
        lowerUnwind(unwind);
    } else if (mlir::db::UnwindRange unwindRange = mlir::dyn_cast<mlir::db::UnwindRange>(operation)) {
        lowerUnwindRange(unwindRange);
    } else if (mlir::db::UnwindComputed unwindComputed = mlir::dyn_cast<mlir::db::UnwindComputed>(operation)) {
        lowerUnwindComputed(unwindComputed);
    } else if (mlir::db::MakeList makeList = mlir::dyn_cast<mlir::db::MakeList>(operation)) {
        lowerMakeList(makeList);
    } else if (mlir::db::MakeMap makeMap = mlir::dyn_cast<mlir::db::MakeMap>(operation)) {
        lowerMakeMap(makeMap);
    } else if (mlir::db::MergeMaps merge = mlir::dyn_cast<mlir::db::MergeMaps>(operation)) {
        lowerMergeMaps(merge);
    } else if (mlir::db::StaticMapKey mapKey = mlir::dyn_cast<mlir::db::StaticMapKey>(operation)) {
        lowerStaticMapKey(mapKey);
    } else if (mlir::db::DynamicMapKey dynamicMapKey = mlir::dyn_cast<mlir::db::DynamicMapKey>(operation)) {
        lowerDynamicMapKey(dynamicMapKey);
    } else if (mlir::db::ContainerIndex containerIndex = mlir::dyn_cast<mlir::db::ContainerIndex>(operation)) {
        lowerContainerIndex(containerIndex);
    } else if (mlir::db::Range range = mlir::dyn_cast<mlir::db::Range>(operation)) {
        lowerRange(range);
    } else if (mlir::db::Substring substring = mlir::dyn_cast<mlir::db::Substring>(operation)) {
        lowerSubstring(substring);
    } else if (mlir::db::Split split = mlir::dyn_cast<mlir::db::Split>(operation)) {
        lowerSplit(split);
    } else if (mlir::db::Left left = mlir::dyn_cast<mlir::db::Left>(operation)) {
        lowerStringEnd<nl::Left>(left);
    } else if (mlir::db::Right right = mlir::dyn_cast<mlir::db::Right>(operation)) {
        lowerStringEnd<nl::Right>(right);
    } else if (mlir::db::Replace replace = mlir::dyn_cast<mlir::db::Replace>(operation)) {
        lowerReplace(replace);
    } else if (mlir::isa<mlir::db::PointDistance>(operation)) {
        lowerPointFunction<nl::PointDistance>(&operation, _builder.getF64Type());
    } else if (mlir::isa<mlir::db::PointWithinBBox>(operation)) {
        lowerPointFunction<nl::PointWithinBBox>(&operation, _builder.getI1Type());
    } else if (mlir::db::ListSlice listSlice = mlir::dyn_cast<mlir::db::ListSlice>(operation)) {
        lowerListSlice(listSlice);
    } else if (mlir::db::SortMulti sortMulti = mlir::dyn_cast<mlir::db::SortMulti>(operation)) {
        lowerSortMulti(sortMulti);
    } else if (mlir::isa<mlir::db::PercentileCont, mlir::db::PercentileDisc>(operation)) {
        lowerPercentile(&operation);
    } else if (mlir::db::ListComprehension listComprehension = mlir::dyn_cast<mlir::db::ListComprehension>(operation)) {
        lowerListComprehension(listComprehension);
    } else if (mlir::db::ListPredicate listPredicate = mlir::dyn_cast<mlir::db::ListPredicate>(operation)) {
        lowerListPredicate(listPredicate);
    } else if (mlir::db::Reduce reduce = mlir::dyn_cast<mlir::db::Reduce>(operation)) {
        lowerReduce(reduce);
    } else if (mlir::db::AllReduce allReduce = mlir::dyn_cast<mlir::db::AllReduce>(operation)) {
        lowerAllReduce(allReduce);
    } else if (mlir::db::PatternComprehension patternComprehension = mlir::dyn_cast<mlir::db::PatternComprehension>(operation)) {
        lowerPatternComprehension(patternComprehension);
    } else if (mlir::db::ScanEdges scanEdges = mlir::dyn_cast<mlir::db::ScanEdges>(operation)) {
        lowerScanEdges(scanEdges);
    } else if (mlir::db::ConstScanEdges constScanEdges = mlir::dyn_cast<mlir::db::ConstScanEdges>(operation)) {
        lowerConstScanEdges(constScanEdges);
    } else if (mlir::db::ScanEdgesByType scanEdgesByType = mlir::dyn_cast<mlir::db::ScanEdgesByType>(operation)) {
        lowerScanEdgesByType(scanEdgesByType);
    } else if (mlir::db::ScanOutEdgesByLabelSrc scanOutEdgesByLabelSrc = mlir::dyn_cast<mlir::db::ScanOutEdgesByLabelSrc>(operation)) {
        lowerScanOutEdgesByLabelSrc(scanOutEdgesByLabelSrc);
    } else if (mlir::db::ScanInEdgesByLabelTgt scanInEdgesByLabelTgt = mlir::dyn_cast<mlir::db::ScanInEdgesByLabelTgt>(operation)) {
        lowerScanInEdgesByLabelTgt(scanInEdgesByLabelTgt);
    } else if (mlir::db::ScanOutEdgesByLabelTgt scanOutEdgesByLabelTgt = mlir::dyn_cast<mlir::db::ScanOutEdgesByLabelTgt>(operation)) {
        lowerScanOutEdgesByLabelTgt(scanOutEdgesByLabelTgt);
    } else if (mlir::db::ScanInEdgesByLabelSrc scanInEdgesByLabelSrc = mlir::dyn_cast<mlir::db::ScanInEdgesByLabelSrc>(operation)) {
        lowerScanInEdgesByLabelSrc(scanInEdgesByLabelSrc);
    } else if (mlir::db::ScanOutEdgesByLabelSrcTgt scanOutEdgesByLabelSrcTgt = mlir::dyn_cast<mlir::db::ScanOutEdgesByLabelSrcTgt>(operation)) {
        lowerScanOutEdgesByLabelSrcTgt(scanOutEdgesByLabelSrcTgt);
    } else if (mlir::db::ScanInEdgesByLabelSrcTgt scanInEdgesByLabelSrcTgt = mlir::dyn_cast<mlir::db::ScanInEdgesByLabelSrcTgt>(operation)) {
        lowerScanInEdgesByLabelSrcTgt(scanInEdgesByLabelSrcTgt);
    } else if (mlir::db::GetOutEdges getOutEdges = mlir::dyn_cast<mlir::db::GetOutEdges>(operation)) {
        lowerGetOutEdges(getOutEdges);
    } else if (mlir::db::GetInEdges getInEdges = mlir::dyn_cast<mlir::db::GetInEdges>(operation)) {
        lowerGetInEdges(getInEdges);
    } else if (mlir::db::GetEdges getEdges = mlir::dyn_cast<mlir::db::GetEdges>(operation)) {
        lowerGetEdges(getEdges);
    } else if (mlir::db::GetOutEdgesByType getOutEdgesByType = mlir::dyn_cast<mlir::db::GetOutEdgesByType>(operation)) {
        lowerGetOutEdgesByType(getOutEdgesByType);
    } else if (mlir::db::GetInEdgesByType getInEdgesByType = mlir::dyn_cast<mlir::db::GetInEdgesByType>(operation)) {
        lowerGetInEdgesByType(getInEdgesByType);
    } else if (mlir::db::GetOutEdgesByLabel getOutEdgesByLabel = mlir::dyn_cast<mlir::db::GetOutEdgesByLabel>(operation)) {
        lowerGetOutEdgesByLabel(getOutEdgesByLabel);
    } else if (mlir::db::GetInEdgesByLabel getInEdgesByLabel = mlir::dyn_cast<mlir::db::GetInEdgesByLabel>(operation)) {
        lowerGetInEdgesByLabel(getInEdgesByLabel);
    } else if (mlir::db::GetOutEdgesByTypeAndLabel getOutEdgesByTypeAndLabel = mlir::dyn_cast<mlir::db::GetOutEdgesByTypeAndLabel>(operation)) {
        lowerGetOutEdgesByTypeAndLabel(getOutEdgesByTypeAndLabel);
    } else if (mlir::db::GetInEdgesByTypeAndLabel getInEdgesByTypeAndLabel = mlir::dyn_cast<mlir::db::GetInEdgesByTypeAndLabel>(operation)) {
        lowerGetInEdgesByTypeAndLabel(getInEdgesByTypeAndLabel);
    } else if (mlir::db::ExplorePaths explorePaths = mlir::dyn_cast<mlir::db::ExplorePaths>(operation)) {
        lowerExplorePaths(explorePaths);
    } else if (mlir::db::ExpandPath expandPath = mlir::dyn_cast<mlir::db::ExpandPath>(operation)) {
        lowerExpandPath(expandPath);
    } else if (mlir::db::PathLength pathLength = mlir::dyn_cast<mlir::db::PathLength>(operation)) {
        lowerPathLength(pathLength);
    } else if (mlir::db::PathElements pathElements = mlir::dyn_cast<mlir::db::PathElements>(operation)) {
        lowerPathElements(pathElements);
    } else if (mlir::db::MakePath makePath = mlir::dyn_cast<mlir::db::MakePath>(operation)) {
        lowerMakePath(makePath);
    } else if (mlir::db::GetNodeProperties getNodeProperties = mlir::dyn_cast<mlir::db::GetNodeProperties>(operation)) {
        lowerGetNodeProperties(getNodeProperties);
    } else if (mlir::db::GetEdgeProperties getEdgeProperties = mlir::dyn_cast<mlir::db::GetEdgeProperties>(operation)) {
        lowerGetEdgeProperties(getEdgeProperties);
    } else if (mlir::db::GetNodeLabelSet getNodeLabelSet = mlir::dyn_cast<mlir::db::GetNodeLabelSet>(operation)) {
        lowerGetNodeLabelSet(getNodeLabelSet);
    } else if (mlir::db::GetEdgeTypes getEdgeTypes = mlir::dyn_cast<mlir::db::GetEdgeTypes>(operation)) {
        lowerGetEdgeTypes(getEdgeTypes);
    } else if (mlir::db::CheckLabelConstraint checkLabelConstraint = mlir::dyn_cast<mlir::db::CheckLabelConstraint>(operation)) {
        lowerCheckLabelConstraint(checkLabelConstraint);
    } else if (mlir::db::CheckEdgeTypeConstraint checkEdgeTypeConstraint = mlir::dyn_cast<mlir::db::CheckEdgeTypeConstraint>(operation)) {
        lowerCheckEdgeTypeConstraint(checkEdgeTypeConstraint);
    } else if (mlir::db::CreateNode createNode = mlir::dyn_cast<mlir::db::CreateNode>(operation)) {
        lowerCreateNode(createNode);
    } else if (mlir::db::CreateEdge createEdge = mlir::dyn_cast<mlir::db::CreateEdge>(operation)) {
        lowerCreateEdge(createEdge);
    } else if (mlir::db::Merge merge = mlir::dyn_cast<mlir::db::Merge>(operation)) {
        lowerMerge(merge);
    } else if (mlir::db::SetNodeProperty setNodeProperty = mlir::dyn_cast<mlir::db::SetNodeProperty>(operation)) {
        lowerSetNodeProperty(setNodeProperty);
    } else if (mlir::db::SetEdgeProperty setEdgeProperty = mlir::dyn_cast<mlir::db::SetEdgeProperty>(operation)) {
        lowerSetEdgeProperty(setEdgeProperty);
    } else if (mlir::db::SetNodeProperties setNodeProperties = mlir::dyn_cast<mlir::db::SetNodeProperties>(operation)) {
        lowerSetProperties<nl::SetNodeProperties>(setNodeProperties, setNodeProperties.getInputNodes());
    } else if (mlir::db::SetEdgeProperties setEdgeProperties = mlir::dyn_cast<mlir::db::SetEdgeProperties>(operation)) {
        lowerSetProperties<nl::SetEdgeProperties>(setEdgeProperties, setEdgeProperties.getInputEdges());
    } else if (mlir::db::DeleteNode deleteNode = mlir::dyn_cast<mlir::db::DeleteNode>(operation)) {
        lowerDeleteNode(deleteNode);
    } else if (mlir::db::DeleteEdge deleteEdge = mlir::dyn_cast<mlir::db::DeleteEdge>(operation)) {
        lowerDeleteEdge(deleteEdge);
    } else if (mlir::db::CrossProduct crossProduct = mlir::dyn_cast<mlir::db::CrossProduct>(operation)) {
        lowerCrossProduct(crossProduct);
    } else if (mlir::db::HashJoin hashJoin = mlir::dyn_cast<mlir::db::HashJoin>(operation)) {
        lowerHashJoin(hashJoin);
    } else if (mlir::db::Union unionOp = mlir::dyn_cast<mlir::db::Union>(operation)) {
        lowerUnion(unionOp);
    } else if (mlir::db::Conditional conditional = mlir::dyn_cast<mlir::db::Conditional>(operation)) {
        lowerConditional(conditional);
    } else if (mlir::db::DistinctSet distinctSet = mlir::dyn_cast<mlir::db::DistinctSet>(operation)) {
        lowerDistinctSet(distinctSet);
    } else if (mlir::db::Materialize materialize = mlir::dyn_cast<mlir::db::Materialize>(operation)) {
        lowerMaterialize(materialize);
    } else if (mlir::db::ScanMaterialized scan = mlir::dyn_cast<mlir::db::ScanMaterialized>(operation)) {
        lowerScanMaterialized(scan);
    } else if (mlir::db::OptionalMatch optionalMatch = mlir::dyn_cast<mlir::db::OptionalMatch>(operation)) {
        lowerOptionalMatch(optionalMatch);
    } else if (mlir::db::CallSubquery callSubquery = mlir::dyn_cast<mlir::db::CallSubquery>(operation)) {
        lowerCallSubquery(callSubquery);
    } else if (mlir::db::ExistsSubquery existsSubquery = mlir::dyn_cast<mlir::db::ExistsSubquery>(operation)) {
        lowerSubqueryExpression<mlir::db::ExistsSubquery, mlir::db::ExistsYield>(existsSubquery);
    } else if (mlir::db::CountSubquery countSubquery = mlir::dyn_cast<mlir::db::CountSubquery>(operation)) {
        lowerSubqueryExpression<mlir::db::CountSubquery, mlir::db::CountSubqueryYield>(countSubquery);
    } else if (mlir::db::CollectSubquery collectSubquery = mlir::dyn_cast<mlir::db::CollectSubquery>(operation)) {
        lowerSubqueryExpression<mlir::db::CollectSubquery, mlir::db::CollectSubqueryYield>(collectSubquery);
    } else if (mlir::db::Limit limit = mlir::dyn_cast<mlir::db::Limit>(operation)) {
        lowerLimit(limit);
    } else if (mlir::db::Skip skip = mlir::dyn_cast<mlir::db::Skip>(operation)) {
        lowerSkip(skip);
    } else if (mlir::db::Sort sort = mlir::dyn_cast<mlir::db::Sort>(operation)) {
        lowerSort(sort);
    } else if (mlir::db::RemoveDuplicates distinct = mlir::dyn_cast<mlir::db::RemoveDuplicates>(operation)) {
        lowerRemoveDuplicates(distinct);
    } else if (mlir::db::Count count = mlir::dyn_cast<mlir::db::Count>(operation)) {
        lowerCount(count);
    } else if (mlir::db::CountScanRows countScanRows = mlir::dyn_cast<mlir::db::CountScanRows>(operation)) {
        lowerCountScanRows(countScanRows);
    } else if (mlir::db::Sum sum = mlir::dyn_cast<mlir::db::Sum>(operation)) {
        lowerAggregate(sum.getInput(), sum.getResult(), storage::AggregateKind::Sum, sum.getDistinct());
    } else if (mlir::db::Min min = mlir::dyn_cast<mlir::db::Min>(operation)) {
        lowerAggregate(min.getInput(), min.getResult(), storage::AggregateKind::Min, min.getDistinct());
    } else if (mlir::db::Max max = mlir::dyn_cast<mlir::db::Max>(operation)) {
        lowerAggregate(max.getInput(), max.getResult(), storage::AggregateKind::Max, max.getDistinct());
    } else if (mlir::db::Avg avg = mlir::dyn_cast<mlir::db::Avg>(operation)) {
        lowerAggregate(avg.getInput(), avg.getResult(), storage::AggregateKind::Avg, avg.getDistinct());
    } else if (mlir::db::StDev stDev = mlir::dyn_cast<mlir::db::StDev>(operation)) {
        lowerAggregate(stDev.getInput(), stDev.getResult(), storage::AggregateKind::StDev, stDev.getDistinct());
    } else if (mlir::db::StDevP stDevP = mlir::dyn_cast<mlir::db::StDevP>(operation)) {
        lowerAggregate(stDevP.getInput(), stDevP.getResult(), storage::AggregateKind::StDevP, stDevP.getDistinct());
    } else if (mlir::db::ConstantOp constant = mlir::dyn_cast<mlir::db::ConstantOp>(operation)) {
        lowerConstant(constant);
    } else if (mlir::db::CurrentDateTime currentDateTime = mlir::dyn_cast<mlir::db::CurrentDateTime>(operation)) {
        lowerCurrentDateTime(currentDateTime);
    } else if (mlir::db::BroadcastConstant broadcast = mlir::dyn_cast<mlir::db::BroadcastConstant>(operation)) {
        lowerBroadcastConstant(broadcast);
    } else if (mlir::db::RowConstant rowConstant = mlir::dyn_cast<mlir::db::RowConstant>(operation)) {
        lowerRowConstant(rowConstant);
    } else if (mlir::db::ToNullable toNullable = mlir::dyn_cast<mlir::db::ToNullable>(operation)) {
        lowerToNullable(toNullable);
    } else if (mlir::db::CheckedCast checkedCast = mlir::dyn_cast<mlir::db::CheckedCast>(operation)) {
        lowerCheckedCast(checkedCast);
    } else if (mlir::isa<mlir::db::AddOp>(operation)) {
        lowerBinaryOp<nl::Add>(operation, BinaryResultKind::Numeric);
    } else if (mlir::isa<mlir::db::ConcatOp>(operation)) {
        lowerBinaryOp<nl::Concat>(operation, BinaryResultKind::Concat);
    } else if (mlir::isa<mlir::db::ListIndex>(operation)) {
        lowerBinaryOp<nl::ListIndex>(operation, BinaryResultKind::Index);
    } else if (mlir::isa<mlir::db::SubOp>(operation)) {
        lowerBinaryOp<nl::Sub>(operation, BinaryResultKind::Numeric);
    } else if (mlir::isa<mlir::db::MulOp>(operation)) {
        lowerBinaryOp<nl::Mul>(operation, BinaryResultKind::Numeric);
    } else if (mlir::isa<mlir::db::DivOp>(operation)) {
        lowerBinaryOp<nl::Div>(operation, BinaryResultKind::Numeric);
    } else if (mlir::isa<mlir::db::ModOp>(operation)) {
        lowerBinaryOp<nl::Mod>(operation, BinaryResultKind::Numeric);
    } else if (mlir::isa<mlir::db::PowOp>(operation)) {
        lowerBinaryOp<nl::Pow>(operation, BinaryResultKind::Double);
    } else if (mlir::isa<mlir::db::EqOp>(operation)) {
        lowerBinaryOp<nl::Eq>(operation, BinaryResultKind::Comparison);
    } else if (mlir::isa<mlir::db::NeqOp>(operation)) {
        lowerBinaryOp<nl::Neq>(operation, BinaryResultKind::Comparison);
    } else if (mlir::isa<mlir::db::GtOp>(operation)) {
        lowerBinaryOp<nl::Gt>(operation, BinaryResultKind::Comparison);
    } else if (mlir::isa<mlir::db::LtOp>(operation)) {
        lowerBinaryOp<nl::Lt>(operation, BinaryResultKind::Comparison);
    } else if (mlir::isa<mlir::db::GteOp>(operation)) {
        lowerBinaryOp<nl::Gte>(operation, BinaryResultKind::Comparison);
    } else if (mlir::isa<mlir::db::LteOp>(operation)) {
        lowerBinaryOp<nl::Lte>(operation, BinaryResultKind::Comparison);
    } else if (mlir::isa<mlir::db::StartsWithOp>(operation)) {
        lowerBinaryOp<nl::StartsWith>(operation, BinaryResultKind::Boolean);
    } else if (mlir::isa<mlir::db::EndsWithOp>(operation)) {
        lowerBinaryOp<nl::EndsWith>(operation, BinaryResultKind::Boolean);
    } else if (mlir::isa<mlir::db::ContainsOp>(operation)) {
        lowerBinaryOp<nl::Contains>(operation, BinaryResultKind::Boolean);
    } else if (mlir::isa<mlir::db::InOp>(operation)) {
        lowerBinaryOp<nl::In>(operation, BinaryResultKind::Membership);
    } else if (mlir::isa<mlir::db::IndexOf>(operation)) {
        lowerBinaryOp<nl::IndexOf>(operation, BinaryResultKind::Position);
    } else if (mlir::isa<mlir::db::AndOp>(operation)) {
        lowerBinaryOp<nl::And>(operation, BinaryResultKind::Boolean);
    } else if (mlir::isa<mlir::db::OrOp>(operation)) {
        lowerBinaryOp<nl::Or>(operation, BinaryResultKind::Boolean);
    } else if (mlir::isa<mlir::db::XorOp>(operation)) {
        lowerBinaryOp<nl::Xor>(operation, BinaryResultKind::Boolean);
    } else if (mlir::db::NotOp notOp = mlir::dyn_cast<mlir::db::NotOp>(operation)) {
        lowerNot(notOp);
    } else if (mlir::db::IsNullOp isNull = mlir::dyn_cast<mlir::db::IsNullOp>(operation)) {
        lowerNullTest<nl::IsNull>(isNull.getOperand(), isNull.getResult());
    } else if (mlir::db::IsNotNullOp isNotNull = mlir::dyn_cast<mlir::db::IsNotNullOp>(operation)) {
        lowerNullTest<nl::IsNotNull>(isNotNull.getOperand(), isNotNull.getResult());
    } else if (mlir::db::Case caseOp = mlir::dyn_cast<mlir::db::Case>(operation)) {
        lowerCase(caseOp);
    } else if (mlir::db::LazyCase lazyCase = mlir::dyn_cast<mlir::db::LazyCase>(operation)) {
        lowerLazyCase(lazyCase);
    } else if (mlir::db::Coalesce coalesce = mlir::dyn_cast<mlir::db::Coalesce>(operation)) {
        lowerCoalesce(coalesce);
    } else if (mlir::db::FilterOp filter = mlir::dyn_cast<mlir::db::FilterOp>(operation)) {
        lowerFilter(filter);
    } else if (mlir::db::FetchNodes fetch = mlir::dyn_cast<mlir::db::FetchNodes>(operation)) {
        lowerFetchNodes(fetch);
    } else if (mlir::db::FetchEdges fetch = mlir::dyn_cast<mlir::db::FetchEdges>(operation)) {
        lowerFetchEdges(fetch);
    } else if (mlir::db::EntityIDOf entityIDOf = mlir::dyn_cast<mlir::db::EntityIDOf>(operation)) {
        lowerEntityIDOf(entityIDOf);
    } else if (mlir::db::GroupAggregate groupAggregate = mlir::dyn_cast<mlir::db::GroupAggregate>(operation)) {
        lowerGroupAggregate(groupAggregate);
    } else if (mlir::db::Collect collect = mlir::dyn_cast<mlir::db::Collect>(operation)) {
        lowerCollect(collect);
    } else if (mlir::db::UnwindCollect unwindCollect = mlir::dyn_cast<mlir::db::UnwindCollect>(operation)) {
        lowerUnwindCollect(unwindCollect);
    } else if (mlir::db::ShortestPath shortestPath = mlir::dyn_cast<mlir::db::ShortestPath>(operation)) {
        lowerShortestPath(shortestPath);
    } else if (mlir::db::CallProcedure call = mlir::dyn_cast<mlir::db::CallProcedure>(operation)) {
        lowerCallProcedure(call);
    } else if (mlir::db::Output output = mlir::dyn_cast<mlir::db::Output>(operation)) {
        lowerOutput(output);
    } else if (mlir::isa<mlir::db::Head, mlir::db::Last>(operation)) {
        lowerHeadOrLast(operation);
    } else if (lookupUnaryFunctionLowering(operation)) {
        lowerUnaryFunction(&operation);
    } else if (lookupBinaryFunctionLowering(operation)) {
        lowerBinaryFunction(&operation);
    } else if (mlir::isa<mlir::func::ReturnOp>(operation)) {
        // We already added a ReturnOp to the nl function
    } else if (!lowerSystemCommand(operation)) {
        throw IRException("DBLowering cannot lower operation '"
                          + operation.getName().getStringRef().str() + "'");
    }
}

bool DBLowering::lowerSystemCommand(mlir::Operation& operation) {
    const auto siblingIt = systemCommandSiblings.find(operation.getName().getStringRef());
    if (siblingIt == systemCommandSiblings.end()) {
        return false;
    }

    // A system command reads no column, so nothing constrains where it sits, and it
    // opens no loop: it materializes its whole (small) result table in one step at
    // function scope, the way nl.count_result materializes its single row.
    setInsertionInto(_entryBlock);

    mlir::MLIRContext* const context = _builder.getContext();

    mlir::OperationState state(_builder.getUnknownLoc(), siblingIt->second);
    state.addAttributes(operation.getAttrs());

    for (const mlir::Value result : operation.getResults()) {
        const mlir::db::ColumnType columnType = mlir::cast<mlir::db::ColumnType>(result.getType());
        state.addTypes(nl::ChunkType::get(context, columnType.getType()));
    }

    mlir::Operation* const nlOperation = _builder.create(state);

    for (size_t resultIndex = 0; resultIndex < operation.getNumResults(); resultIndex++) {
        _valueMap[operation.getResult(resultIndex)] = nlOperation->getResult(resultIndex);
    }

    return true;
}

template <typename NLStringEndOp, typename StringEndOp>
void DBLowering::lowerStringEnd(StringEndOp stringEnd) {
    const mlir::Type stringChunkType = mapValue(stringEnd.getString()).getType();
    const bool stringMayBeNull = mayHoldANull(stringChunkType);

    llvm::SmallVector<mlir::Value, 2> arguments;
    containerCellChunks(stringEnd->getOperands(), arguments);

    mlir::MLIRContext* const context = _builder.getContext();
    const mlir::Type stringType = storage::StringType::get(context);
    const mlir::Type resultElement = stringMayBeNull ? storage::NullableType::get(context, stringType) : stringType;
    const nl::ChunkType resultType = nl::ChunkType::get(context, resultElement);

    setInsertionForNaryOp(arguments);

    NLStringEndOp ends = _builder.create<NLStringEndOp>(_builder.getUnknownLoc(),
                                                        resultType,
                                                        arguments[0],
                                                        arguments[1]);

    _valueMap[stringEnd.getResult()] = ends.getResult();
}

// Two points in different CRSs have no distance and no box, so the result is nullable
// whatever its arguments are
template <typename NLOp>
void DBLowering::lowerPointFunction(mlir::Operation* operation, mlir::Type valueType) {
    llvm::SmallVector<mlir::Value, 3> arguments;
    containerCellChunks(operation->getOperands(), arguments);

    mlir::MLIRContext* const context = _builder.getContext();
    const nl::ChunkType resultType = nl::ChunkType::get(context, storage::NullableType::get(context, valueType));

    setInsertionForNaryOp(arguments);

    NLOp lowered = _builder.create<NLOp>(_builder.getUnknownLoc(), mlir::TypeRange {resultType}, arguments);

    _valueMap[operation->getResult(0)] = lowered.getResult();
}

template <typename SubqueryOp, typename YieldOp>
void DBLowering::lowerSubqueryExpression(SubqueryOp subquery) {
    constexpr bool countsRows = std::is_same_v<SubqueryOp, mlir::db::CountSubquery>;
    constexpr bool collectsValues = std::is_same_v<SubqueryOp, mlir::db::CollectSubquery>;

    const mlir::Location loc = _builder.getUnknownLoc();
    mlir::MLIRContext* const context = _builder.getContext();

    llvm::SmallVector<mlir::Value, 4> inputChunks;
    for (const mlir::Value column : subquery.getInputColumns()) {
        inputChunks.push_back(mapValue(column));
    }

    // The step the accumulator covers is the one binding the columns the subquery answers
    // for, so it is emptied once per chunk of them - and the body's nest and the read of
    // the answers both go there, the read after the nest.
    mlir::Block* const stepBlock = deepestOwnerBlock(inputChunks, _rootBlock);
    const mlir::Value cardinality = inputChunks.empty() ? mlir::Value() : inputChunks.front();

    setInsertionInto(stepBlock);

    mlir::Value state;
    mlir::Value tag;

    if constexpr (collectsValues) {
        nl::RowListsBuffer buffer = _builder.create<nl::RowListsBuffer>(loc, cardinality);
        state = buffer.getState();
        tag = buffer.getTag();
    } else if constexpr (countsRows) {
        nl::CountSubqueryBuffer buffer = _builder.create<nl::CountSubqueryBuffer>(loc, inputChunks);
        state = buffer.getState();
        tag = buffer.getTag();
    } else {
        nl::ExistsBuffer buffer = _builder.create<nl::ExistsBuffer>(loc, inputChunks);
        state = buffer.getState();
        tag = buffer.getTag();
    }

    // A body run one row at a time is handed the step's rows a row at a time, each with its
    // tag, so what it holds is answered for under that row and the answers are read over the
    // whole step. A body carrying the scope, and one over the single empty row, run in the
    // step block as they otherwise would.
    const bool bodyRunsPerRow = runsPerRow(subquery);
    const bool perRow = bodyRunsPerRow && !inputChunks.empty();

    mlir::Block* bodyRoot = stepBlock;
    llvm::SmallVector<mlir::Value, 4> bodyChunks(inputChunks.begin(), inputChunks.end());
    mlir::Value rowTag;

    if (perRow) {
        llvm::SmallVector<mlir::Value, 4> rowChunks(inputChunks.begin(), inputChunks.end());
        rowChunks.push_back(tag);

        nl::EachRow eachRow = _builder.create<nl::EachRow>(loc, rowChunks);
        nl::For rowLoop = _builder.create<nl::For>(loc, eachRow.getResult(), mlir::Value());
        bodyRoot = rowLoop.getBody();

        bodyChunks.assign(bodyRoot->args_begin(), bodyRoot->args_begin() + inputChunks.size());
        rowTag = bodyRoot->getArgument(static_cast<unsigned>(inputChunks.size()));
    }

    // The body reads this step's rows through its block arguments, and the row tag through
    // the trailing one; a body run per row, or with nothing to join onto, takes no tag.
    mlir::Region& body = subquery.getBody();
    mlir::Block& bodyBlock = body.front();
    for (size_t inputIndex = 0; inputIndex < bodyChunks.size(); inputIndex++) {
        _valueMap[bodyBlock.getArgument(static_cast<unsigned>(inputIndex))] = bodyChunks[inputIndex];
    }

    if (bodyBlock.getNumArguments() > inputChunks.size()) {
        _valueMap[bodyBlock.getArgument(static_cast<unsigned>(inputChunks.size()))] = tag;
    }

    if (bodyRunsPerRow) {
        hoistLimitHandles(body, bodyRoot, subquery);
    }

    // A dataflow of its own, rooted where the accumulator sits so its loops nest inside
    // this step; the caller's root and innermost loop are restored once it is lowered.
    mlir::Block* const previousRoot = _rootBlock;
    mlir::Block* const previousInnermostLoopBody = _innermostLoopBody;
    const mlir::Value previousInnermostCardinality = _innermostCardinality;
    _rootBlock = bodyRoot;
    _innermostLoopBody = nullptr;
    _innermostCardinality = mlir::Value();

    for (mlir::Operation& operation : bodyBlock.without_terminator()) {
        lowerOperation(operation);
    }

    YieldOp yield = mlir::cast<YieldOp>(bodyBlock.getTerminator());

    llvm::SmallVector<mlir::Value, 4> heldChunks;
    for (const mlir::Value column : yield.getColumns()) {
        heldChunks.push_back(mapValue(column));
    }

    mlir::Value heldTag;
    if (const mlir::Value yieldedTag = yield.getTag()) {
        heldTag = mapValue(yieldedTag);
    }

    // A constant holds one cell standing for every row the body produced, so an untagged
    // body is answered off a chunk that carries those rows, where one does
    mlir::Value heldRows;
    if (!heldTag) {
        heldRows = cardinalityDriver(heldChunks);
        if (!heldRows) {
            heldRows = heldChunks.front();
        }
    }

    // The answer is taken where the body left its rows - its innermost loop body - which is
    // the deepest block the tag or the rows it holds are bound in.
    llvm::SmallVector<mlir::Value, 4> answeredChunks = heldChunks;
    answeredChunks.push_back(heldTag ? heldTag : heldRows);

    mlir::Value collectedValue;
    if constexpr (collectsValues) {
        collectedValue = rowListsValueChunk(mapValue(yield.getValue()), heldTag ? heldTag : heldRows);
        answeredChunks.push_back(collectedValue);
    }

    setInsertionInto(deepestOwnerBlock(answeredChunks, bodyRoot));

    if constexpr (collectsValues) {
        _builder.create<nl::RowListsCollect>(loc, state, heldTag, rowTag, collectedValue);
    } else if constexpr (countsRows) {
        _builder.create<nl::CountSubqueryTally>(loc, state, heldTag, rowTag, heldRows);
    } else {
        llvm::SmallVector<mlir::Value, 1> markedRows;
        if (heldRows) {
            markedRows.push_back(heldRows);
        }

        _builder.create<nl::ExistsMark>(loc, state, heldTag, rowTag, markedRows);
    }

    _rootBlock = previousRoot;
    _innermostLoopBody = previousInnermostLoopBody;
    _innermostCardinality = previousInnermostCardinality;

    setInsertionInto(stepBlock);

    mlir::Value answer;

    if constexpr (collectsValues) {
        const mlir::Type listType = storage::ListType::get(context, listedElementType(context, collectedValue));
        const nl::ChunkType listChunk = nl::ChunkType::get(context, listType);

        nl::RowListsResult result = _builder.create<nl::RowListsResult>(loc, listChunk, state, cardinality);
        answer = result.getResult();
    } else if constexpr (countsRows) {
        nl::CountSubqueryResult result = _builder.create<nl::CountSubqueryResult>(loc, state, cardinality);
        answer = result.getResult();
    } else {
        const nl::ChunkType boolChunk = nl::ChunkType::get(context, mlir::storage::BoolType::get(context));

        nl::ExistsResult result = _builder.create<nl::ExistsResult>(loc, boolChunk, state, cardinality);
        answer = result.getResult();
    }

    _valueMap[subquery.getResult()] = answer;
}

mlir::Type DBLowering::columnType(mlir::Value column) {
    return mlir::cast<mlir::db::ColumnType>(column.getType()).getType();
}

mlir::Value DBLowering::propertyWriteChunk(llvm::StringRef propertyName, mlir::Value chunk) {
    if (!_view || !_view->isValid()) {
        return chunk;
    }

    // if the property type is a double in the db, but an integer was provided, convert
    // that int to double
    const GraphMetadata& metadata = _view->metadata();
    const PropertyTypeMap& propTypes = metadata.propTypes();
    const std::optional<PropertyType> propertyType = propTypes.get(propertyName);
    if (!propertyType || propertyType->_valueType != ValueType::Double) {
        return chunk;
    }

    const mlir::Type element = chunkValueElement(_builder, chunk.getType());
    const auto integerElement = mlir::dyn_cast<mlir::IntegerType>(element);
    if (!integerElement || integerElement.getWidth() != 64) {
        return chunk;
    }

    return chunkAsElement(chunk, _builder.getF64Type());
}

void DBLowering::coercePropertyWriteChunks(mlir::ArrayAttr names,
                                           llvm::SmallVectorImpl<mlir::Value>& chunks) {
    for (size_t index = 0; index < chunks.size(); index++) {
        const llvm::StringRef name = mlir::cast<mlir::StringAttr>(names[index]).getValue();

        chunks[index] = propertyWriteChunk(name, chunks[index]);
    }
}

void DBLowering::coerceGroupedPropertyWriteChunks(mlir::ArrayAttr groupedNames,
                                                  llvm::SmallVectorImpl<mlir::Value>& chunks) {
    size_t index = 0;

    for (const mlir::Attribute group : groupedNames) {
        for (const mlir::Attribute name : mlir::cast<mlir::ArrayAttr>(group)) {
            chunks[index] = propertyWriteChunk(mlir::cast<mlir::StringAttr>(name).getValue(), chunks[index]);
            index++;
        }
    }
}

void DBLowering::lowerCreateNode(mlir::db::CreateNode createNode) {
    const mlir::Location loc = _builder.getUnknownLoc();

    llvm::SmallVector<mlir::Value, 4> propChunks;
    for (const mlir::Value propValue : createNode.getPropValues()) {
        propChunks.push_back(mapValue(propValue));
    }

    coercePropertyWriteChunks(createNode.getPropNames(), propChunks);

    // A constant stands for the rows in flight, which is how many nodes the op writes
    mlir::Value cardinalityChunk;
    if (createNode.getCardinality()) {
        cardinalityChunk = mapValue(createNode.getCardinality());

        const mlir::Value rows = rowsInFlight();
        if (rows) {
            cardinalityChunk = rowAlignedChunk(cardinalityChunk, rows);
        }
    }

    if (cardinalityChunk) {
        setInsertionInto(ownerBlock(cardinalityChunk));
    } else {
        mlir::Block* targetBlock = _rootBlock;
        for (const mlir::OpOperand& use : createNode.getResult().getUses()) {
            auto createEdge = mlir::dyn_cast<mlir::db::CreateEdge>(use.getOwner());
            if (!createEdge) {
                continue;
            }
            for (const mlir::Value candidate : {createEdge.getSrcIds(), createEdge.getTgtIds()}) {
                if (candidate == createNode.getResult()) {
                    continue;
                }
                const auto it = _valueMap.find(candidate);
                if (it == _valueMap.end()) {
                    continue;
                }
                mlir::Block* const candidateBlock = ownerBlock(it->second);
                if (candidateBlock != _rootBlock) {
                    targetBlock = candidateBlock;
                    break;
                }
            }
            if (targetBlock != _rootBlock) {
                break;
            }
        }

        setInsertionInto(targetBlock);
    }

    nl::CreateNode create = _builder.create<nl::CreateNode>(
        loc,
        createNode.getLabelsAttr(),
        createNode.getPropNamesAttr(),
        propChunks,
        cardinalityChunk);
    _valueMap[createNode.getResult()] = create.getResult();
}

void DBLowering::lowerCreateEdge(mlir::db::CreateEdge createEdge) {
    const mlir::Location loc = _builder.getUnknownLoc();
    const mlir::Value srcChunk = mapValue(createEdge.getSrcIds());
    const mlir::Value tgtChunk = mapValue(createEdge.getTgtIds());

    llvm::SmallVector<mlir::Value, 4> propChunks;
    for (const mlir::Value propValue : createEdge.getPropValues()) {
        propChunks.push_back(mapValue(propValue));
    }

    coercePropertyWriteChunks(createEdge.getPropNames(), propChunks);

    llvm::SmallVector<mlir::Value, 8> operandChunks {srcChunk, tgtChunk};
    operandChunks.append(propChunks.begin(), propChunks.end());

    const mlir::Value reference = deepestBoundChunk(operandChunks);
    setInsertionInto(ownerBlock(reference));

    nl::CreateEdge create = _builder.create<nl::CreateEdge>(
        loc,
        srcChunk,
        tgtChunk,
        createEdge.getEdgeTypeAttr(),
        createEdge.getPropNamesAttr(),
        propChunks,
        mapOptionalMask(createEdge.getSrcPending()),
        mapOptionalMask(createEdge.getTgtPending()),
        createEdge.getSrcAllPending(),
        createEdge.getTgtAllPending());
    _valueMap[createEdge.getResult()] = create.getResult();
}

mlir::Value DBLowering::mapOptionalMask(mlir::Value mask) {
    if (!mask) {
        return mlir::Value();
    }

    return pendingMaskChunk(mapValue(mask));
}

// An OPTIONAL CALL pads the mask of a merged entity it returns with null, and the row it
// pads names no entity, so none this change wrote
mlir::Value DBLowering::pendingMaskChunk(mlir::Value chunk) {
    if (!isNullableChunk(chunk.getType())) {
        return chunk;
    }

    const mlir::Location loc = _builder.getUnknownLoc();
    mlir::MLIRContext* const context = _builder.getContext();
    const nl::ChunkType maskType = nl::ChunkType::get(context, storage::BoolType::get(context));

    const mlir::Value notPending = _builder.create<nl::IsNotTrue>(loc, maskType, chunk).getResult();
    return _builder.create<nl::Not>(loc, maskType, notPending).getResult();
}

void DBLowering::mapColumns(mlir::OperandRange columns, llvm::SmallVectorImpl<mlir::Value>& chunks) {
    chunks.clear();
    for (const mlir::Value column : columns) {
        chunks.push_back(mapValue(column));
    }
}

void DBLowering::lowerMerge(mlir::db::Merge merge) {
    llvm::SmallVector<mlir::Value, 8> boundNodes;
    llvm::SmallVector<mlir::Value, 8> boundPending;
    llvm::SmallVector<mlir::Value, 8> nodePropValues;
    llvm::SmallVector<mlir::Value, 8> edgePropValues;
    llvm::SmallVector<mlir::Value, 8> carriedColumns;

    mapColumns(merge.getBoundNodes(), boundNodes);
    mapColumns(merge.getBoundPending(), boundPending);

    for (mlir::Value& pending : boundPending) {
        pending = rowAlignedChunk(pending, boundNodes.front());
    }
    mapColumns(merge.getNodePropValues(), nodePropValues);
    mapColumns(merge.getEdgePropValues(), edgePropValues);
    mapColumns(merge.getCarriedColumns(), carriedColumns);

    coerceGroupedPropertyWriteChunks(merge.getNodePropNames(), nodePropValues);
    coerceGroupedPropertyWriteChunks(merge.getEdgePropNames(), edgePropValues);

    llvm::SmallVector<mlir::Value, 8> operandChunks(boundNodes);
    operandChunks.append(boundPending.begin(), boundPending.end());
    operandChunks.append(nodePropValues.begin(), nodePropValues.end());
    operandChunks.append(edgePropValues.begin(), edgePropValues.end());
    operandChunks.append(carriedColumns.begin(), carriedColumns.end());

    // Inserted into the deepest block, where all the operands are defined. A merge whose
    // operands are all bound above the root - literals alone - opens in the root, as a
    // CREATE does, so a subquery branch that is not taken merges nothing.
    mlir::Block* targetBlock = _rootBlock;
    if (const mlir::Value insertionReference = deepestBoundChunk(operandChunks)) {
        mlir::Block* const referenceBlock = ownerBlock(insertionReference);
        if (enclosesBlock(_rootBlock, referenceBlock)) {
            targetBlock = referenceBlock;
        }
    }

    setInsertionInto(targetBlock);

    for (mlir::Value& pending : boundPending) {
        pending = pendingMaskChunk(pending);
    }

    mlir::MLIRContext* const context = _builder.getContext();
    const mlir::Type nodeChunkType = nl::ChunkType::get(context, storage::NodeIDType::get(context));
    const mlir::Type edgeChunkType = nl::ChunkType::get(context, storage::EdgeIDType::get(context));
    const mlir::Type maskChunkType = nl::ChunkType::get(context, storage::BoolType::get(context));

    const mlir::ArrayAttr nodeLabels = merge.getNodeLabels();
    const size_t hopCount = nodeLabels.size() - 1;
    const size_t matchedNodeCount = mlir::mergeMatchedNodeCount(nodeLabels);

    llvm::SmallVector<mlir::Type, 8> resultTypes;
    for (size_t nodeIndex = 0; nodeIndex < matchedNodeCount; nodeIndex++) {
        resultTypes.push_back(nodeChunkType);
        resultTypes.push_back(maskChunkType);
    }

    for (size_t hopIndex = 0; hopIndex < hopCount; hopIndex++) {
        resultTypes.push_back(edgeChunkType);
        resultTypes.push_back(maskChunkType);
    }

    resultTypes.push_back(maskChunkType);

    for (const mlir::Value carriedChunk : carriedColumns) {
        resultTypes.push_back(carriedChunk.getType());
    }

    nl::Merge nlMerge = _builder.create<nl::Merge>(_builder.getUnknownLoc(),
                                                   resultTypes,
                                                   merge.getNodeLabelsAttr(),
                                                   merge.getNodePropNamesAttr(),
                                                   merge.getEdgeTypesAttr(),
                                                   merge.getEdgePropNamesAttr(),
                                                   merge.getEdgeDirectionsAttr(),
                                                   merge.getPendingNodesAttr(),
                                                   boundNodes,
                                                   boundPending,
                                                   nodePropValues,
                                                   edgePropValues,
                                                   carriedColumns);

    const mlir::ResultRange dbResults = merge.getResults();
    const mlir::ResultRange nlResults = nlMerge.getResults();
    for (size_t index = 0; index < dbResults.size(); index++) {
        _valueMap[dbResults[index]] = nlResults[index];
    }

    // The merge grows or shrinks the rows in flight, so from here on what sizes a
    // projection of constants alone is its own node chunk
    _innermostCardinality = nlResults.front();
}

void DBLowering::lowerSetNodeProperty(mlir::db::SetNodeProperty setNodeProperty) {
    const mlir::Location loc = _builder.getUnknownLoc();
    const mlir::Value inputChunk = mapValue(setNodeProperty.getInputNodes());
    const mlir::Value valueChunk = propertyWriteChunk(setNodeProperty.getProperty(),
                                                      mapValue(setNodeProperty.getValue()));

    setWriteInsertion(inputChunk, valueChunk);

    _builder.create<nl::SetNodeProperty>(
        loc,
        inputChunk,
        setNodeProperty.getPropertyAttr(),
        valueChunk,
        mapOptionalMask(setNodeProperty.getPending()),
        mapOptionalMask(setNodeProperty.getRows()),
        setNodeProperty.getAllPending());
}

void DBLowering::lowerSetEdgeProperty(mlir::db::SetEdgeProperty setEdgeProperty) {
    const mlir::Location loc = _builder.getUnknownLoc();
    const mlir::Value inputChunk = mapValue(setEdgeProperty.getInputEdges());
    const mlir::Value valueChunk = propertyWriteChunk(setEdgeProperty.getProperty(),
                                                      mapValue(setEdgeProperty.getValue()));

    setWriteInsertion(inputChunk, valueChunk);

    _builder.create<nl::SetEdgeProperty>(
        loc,
        inputChunk,
        setEdgeProperty.getPropertyAttr(),
        valueChunk,
        mapOptionalMask(setEdgeProperty.getPending()),
        mapOptionalMask(setEdgeProperty.getRows()),
        setEdgeProperty.getAllPending());
}

template <typename NLSetProperties, typename DBSetProperties>
void DBLowering::lowerSetProperties(DBSetProperties setProperties, mlir::Value input) {
    const mlir::Location loc = _builder.getUnknownLoc();
    const mlir::Value inputChunk = mapValue(input);
    const mlir::Value valueChunk = mapValue(setProperties.getValue());

    setWriteInsertion(inputChunk, valueChunk);

    _builder.create<NLSetProperties>(loc,
                                     inputChunk,
                                     valueChunk,
                                     mapOptionalMask(setProperties.getPending()),
                                     mapOptionalMask(setProperties.getRows()),
                                     setProperties.getAllPending(),
                                     setProperties.getReplaces(),
                                     setProperties.getCreatedTypesAttr());
}

void DBLowering::lowerDeleteNode(mlir::db::DeleteNode deleteNode) {
    const mlir::Location loc = _builder.getUnknownLoc();
    const mlir::Value inputChunk = mapValue(deleteNode.getInputNodes());

    // The delete runs where its node chunk is live - the block that owns it.
    setInsertionInto(ownerBlock(inputChunk));

    _builder.create<nl::DeleteNode>(loc,
                                    inputChunk,
                                    deleteNode.getDetach(),
                                    mapOptionalMask(deleteNode.getPending()),
                                    deleteNode.getAllPending());
}

void DBLowering::lowerDeleteEdge(mlir::db::DeleteEdge deleteEdge) {
    const mlir::Location loc = _builder.getUnknownLoc();
    const mlir::Value inputChunk = mapValue(deleteEdge.getInputEdges());

    setInsertionInto(ownerBlock(inputChunk));

    _builder.create<nl::DeleteEdge>(loc,
                                    inputChunk,
                                    mapOptionalMask(deleteEdge.getPending()),
                                    deleteEdge.getAllPending());
}

template <typename NLOp>
void DBLowering::lowerBinaryOp(mlir::Operation& op, BinaryResultKind kind) {
    mlir::Value lhsChunk = mapValue(op.getOperand(0));
    mlir::Value rhsChunk = mapValue(op.getOperand(1));

    const mlir::Type lhsChunkType = lhsChunk.getType();
    const mlir::Type rhsChunkType = rhsChunk.getType();

    const bool readsScalarOperands = kind != BinaryResultKind::Index
                                  && kind != BinaryResultKind::Membership
                                  && kind != BinaryResultKind::Position;

    const bool lhsReadsItsOwnNull = mayHoldANull(lhsChunkType);
    const bool rhsReadsItsOwnNull = mayHoldANull(rhsChunkType);

    const bool nullAgainstRhs = readsScalarOperands
                             && isUntypedNullChunk(rhsChunkType)
                             && !lhsReadsItsOwnNull;

    const bool nullAgainstLhs = readsScalarOperands
                             && isUntypedNullChunk(lhsChunkType)
                             && !rhsReadsItsOwnNull;

    // The null literal meets kernels reading a nullable column on its other side, as a
    // logical operator takes it as an unknown truth value
    if (nullAgainstRhs) {
        lhsChunk = nullableValueChunk(lhsChunk);
    } else if (nullAgainstLhs) {
        rhsChunk = nullableValueChunk(rhsChunk);
    }

    // AND, OR and XOR read a cell's truth value against a nullable boolean
    constexpr bool CONNECTS_TRUTH_VALUES = std::is_same_v<NLOp, nl::And>
                                      || std::is_same_v<NLOp, nl::Or>
                                      || std::is_same_v<NLOp, nl::Xor>;

    if constexpr (CONNECTS_TRUTH_VALUES) {
        const bool lhsHoldsCells = isTaggedCellChunk(lhsChunkType);
        const bool rhsHoldsCells = isTaggedCellChunk(rhsChunkType);

        if (lhsHoldsCells && !rhsHoldsCells && !isUntypedNullChunk(rhsChunkType)) {
            rhsChunk = nullableValueChunk(rhsChunk);
        } else if (rhsHoldsCells && !lhsHoldsCells && !isUntypedNullChunk(lhsChunkType)) {
            lhsChunk = nullableValueChunk(lhsChunk);
        }
    }

    if constexpr (CONNECTS_TRUTH_VALUES) {
        lhsChunk = laidOutNullAs(lhsChunk, _builder.getI1Type());
        rhsChunk = laidOutNullAs(rhsChunk, _builder.getI1Type());
    } else if (readsScalarOperands) {
        const mlir::Type lhsElement = chunkValueElement(_builder, lhsChunk.getType());
        const mlir::Type rhsElement = chunkValueElement(_builder, rhsChunk.getType());

        lhsChunk = laidOutNullAs(lhsChunk, rhsElement);
        rhsChunk = laidOutNullAs(rhsChunk, lhsElement);
    }

    const mlir::Type resultElement = binaryResultElement(kind, lhsChunk.getType(), rhsChunk.getType());
    const nl::ChunkType resultType = nl::ChunkType::get(_builder.getContext(), resultElement);

    setInsertionForBinaryOp(lhsChunk, rhsChunk);

    NLOp nlOp = _builder.create<NLOp>(_builder.getUnknownLoc(), resultType, lhsChunk, rhsChunk);
    _valueMap[op.getResult(0)] = nlOp.getResult();
}

template <typename NLOp>
void DBLowering::lowerNullTest(mlir::Value operand, mlir::Value result) {
    const mlir::Value operandChunk = mapValue(operand);

    mlir::MLIRContext* const context = _builder.getContext();
    const nl::ChunkType resultType = nl::ChunkType::get(context, storage::BoolType::get(context));

    setInsertionForUnaryOp(operandChunk);

    NLOp test = _builder.create<NLOp>(_builder.getUnknownLoc(), resultType, operandChunk);
    _valueMap[result] = test.getResult();
}

mlir::Value DBLowering::mapValue(mlir::Value dbValue) const {
    const auto slotIt = _valueMap.find(dbValue);
    if (slotIt == _valueMap.end()) {
        throw IRException("db column used before the operation that produces it");
    }

    return slotIt->second;
}
