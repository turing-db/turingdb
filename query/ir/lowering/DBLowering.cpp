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

void DBLowering::lowerPatternComprehension(mlir::db::PatternComprehension comprehension) {
    const mlir::Location loc = _builder.getUnknownLoc();
    mlir::MLIRContext* const context = _builder.getContext();

    llvm::SmallVector<mlir::Value, 4> inputChunks;
    for (const mlir::Value column : comprehension.getInputColumns()) {
        inputChunks.push_back(mapValue(column));
    }

    // The step the accumulator covers is the one binding the columns the comprehension is
    // read beside, so it is emptied once per chunk of them - and the pattern's nest and
    // the build both go there, the build after the nest.
    mlir::Block* const stepBlock = deepestOwnerBlock(inputChunks, _rootBlock);

    // Any chunk of the step counts its rows; a comprehension read where nothing is in
    // flight has none, and covers the single empty row instead.
    const mlir::Value cardinality = inputChunks.empty() ? mlir::Value {} : cardinalityDriver(inputChunks);

    setInsertionInto(stepBlock);
    nl::RowListsBuffer buffer = _builder.create<nl::RowListsBuffer>(loc, cardinality);
    const mlir::Value state = buffer.getState();

    // The pattern reads this step's rows through its block arguments, and the row tag
    // through the trailing one; a pattern with nothing to join onto has neither.
    mlir::Block& patternBlock = comprehension.getPattern().front();
    for (size_t inputIndex = 0; inputIndex < inputChunks.size(); inputIndex++) {
        _valueMap[patternBlock.getArgument(static_cast<unsigned>(inputIndex))] = inputChunks[inputIndex];
    }

    if (!inputChunks.empty()) {
        _valueMap[patternBlock.getArgument(static_cast<unsigned>(inputChunks.size()))] = buffer.getTag();
    }

    // A dataflow of its own, rooted where the accumulator sits so its loops nest inside
    // this step; the caller's root and innermost loop are restored once it is lowered.
    mlir::Block* const previousRoot = _rootBlock;
    mlir::Block* const previousInnermostLoopBody = _innermostLoopBody;
    const mlir::Value previousInnermostCardinality = _innermostCardinality;
    _rootBlock = stepBlock;
    _innermostLoopBody = nullptr;
    _innermostCardinality = mlir::Value();

    mlir::Value rowTagChunk;
    mlir::Value valueChunk;

    for (mlir::Operation& operation : patternBlock) {
        mlir::db::ComprehensionYield yield = mlir::dyn_cast<mlir::db::ComprehensionYield>(operation);
        if (!yield) {
            lowerOperation(operation);
            continue;
        }

        if (yield.getRowTags()) {
            rowTagChunk = mapValue(yield.getRowTags());
        }

        valueChunk = mapValue(yield.getValue());
    }

    // PatternComprehension::verify guarantees the yield, so an empty one here means
    // unverified IR - the defensive backstop lowerOptionalMatch keeps too.
    if (!valueChunk) {
        throw IRException("db.pattern_comprehension yields no value");
    }

    // The matches the value is read against are the ones the tag counts, since a WHERE has
    // cut both together
    valueChunk = rowListsValueChunk(valueChunk, rowTagChunk ? rowTagChunk : rowsInFlight());

    llvm::SmallVector<mlir::Value, 2> stagedChunks {valueChunk};
    if (rowTagChunk) {
        stagedChunks.push_back(rowTagChunk);
    }

    setInsertionInto(deepestOwnerBlock(stagedChunks, stepBlock));
    _builder.create<nl::RowListsCollect>(loc, state, rowTagChunk, mlir::Value(), valueChunk);

    _rootBlock = previousRoot;
    _innermostLoopBody = previousInnermostLoopBody;
    _innermostCardinality = previousInnermostCardinality;

    // Every row gets a list, the empty one where the pattern matched nothing, so the
    // lists are a container chunk rather than the nullable one a list comprehension builds
    const mlir::Type listType = storage::ListType::get(context, listedElementType(context, valueChunk));
    const nl::ChunkType resultType = nl::ChunkType::get(context, listType);

    setInsertionInto(stepBlock);
    nl::RowListsResult lists = _builder.create<nl::RowListsResult>(loc, resultType, state, cardinality);

    _valueMap[comprehension.getResult()] = lists.getResult();
}

mlir::Value DBLowering::rowListsValueChunk(mlir::Value valueChunk, mlir::Value rows) {
    // A value computed from constants alone holds one cell standing for every row, so it is
    // laid out over the rows it is read against
    const mlir::Value aligned = rowAlignedChunk(valueChunk, rows);

    return listItemChunk(aligned);
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

void DBLowering::lowerOptionalMatch(mlir::db::OptionalMatch optionalMatch) {
    const mlir::Location loc = _builder.getUnknownLoc();

    llvm::SmallVector<mlir::Value, 4> inputChunks;
    for (const mlir::Value column : optionalMatch.getInputColumns()) {
        inputChunks.push_back(mapValue(column));
    }

    // The step the accumulator covers is the one binding the columns the pattern joins
    // onto, so it is emptied once per chunk of them - and the pattern's nest and the drain
    // loop both go there, the drain after the nest.
    mlir::Block* const stepBlock = deepestOwnerBlock(inputChunks, _rootBlock);

    setInsertionInto(stepBlock);
    nl::OptionalBuffer buffer = _builder.create<nl::OptionalBuffer>(loc, inputChunks);
    const mlir::Value state = buffer.getState();

    // Every row drained reaches the cut, so once the pattern has matched as many rows as
    // the cut still admits, the drain spends the budget before it reaches a missed row.
    mlir::Operation* const optionalMatchOp = optionalMatch.getOperation();
    mlir::Value patternLimit;
    if (mlir::Operation* const cut = _optionalMatchLimits.lookup(optionalMatchOp)) {
        patternLimit = _builder.create<nl::LimitRemaining>(loc, _limitHandles.lookup(cut)).getState();
        _limitHandles[optionalMatchOp] = patternLimit;
    }

    // The pattern reads this step's rows through its block arguments, and the row tag
    // through the trailing one; a pattern with nothing to join onto has neither.
    mlir::Block& patternBlock = optionalMatch.getPattern().front();
    for (size_t inputIndex = 0; inputIndex < inputChunks.size(); inputIndex++) {
        _valueMap[patternBlock.getArgument(static_cast<unsigned>(inputIndex))] = inputChunks[inputIndex];
    }

    if (!inputChunks.empty()) {
        _valueMap[patternBlock.getArgument(static_cast<unsigned>(inputChunks.size()))] = buffer.getTag();
    }

    // A dataflow of its own, rooted where the accumulator sits so its loops nest inside
    // this step; the caller's root and innermost loop are restored once it is lowered.
    mlir::Block* const previousRoot = _rootBlock;
    mlir::Block* const previousInnermostLoopBody = _innermostLoopBody;
    const mlir::Value previousInnermostCardinality = _innermostCardinality;
    _rootBlock = stepBlock;
    _innermostLoopBody = nullptr;
    _innermostCardinality = mlir::Value();

    if (patternLimit) {
        mlir::db::OptionalYield patternYield = mlir::cast<mlir::db::OptionalYield>(patternBlock.getTerminator());

        _producerWalkVisits.clear();
        for (const mlir::Value column : patternYield.getColumns()) {
            assignProducerLoops(column, optionalMatchOp, /*rowsDroppedBeforeTheCut=*/false, optionalMatchOp);
        }
    }

    llvm::SmallVector<mlir::Value, 4> matchedChunks;
    mlir::Value matchedTag;

    for (mlir::Operation& operation : patternBlock) {
        mlir::db::OptionalYield yield = mlir::dyn_cast<mlir::db::OptionalYield>(operation);
        if (!yield) {
            lowerOperation(operation);
            continue;
        }

        for (const mlir::Value column : yield.getColumns()) {
            matchedChunks.push_back(mapValue(column));
        }

        if (yield.getTag()) {
            matchedTag = mapValue(yield.getTag());
        }
    }

    // OptionalMatch::verify rejects a pattern yielding no column, so reaching this means
    // unverified IR - a defensive backstop, as in lowerFactor and lowerSort.
    if (matchedChunks.empty()) {
        throw IRException("db.optional_match pattern yields no column");
    }

    // The drain rebuilds a missed row's carried columns out of this step's input chunks, so
    // the two must be the same chunk type however the db types were spelled: a column a
    // CALL yielded enters type-erased and comes back refined, and the chunk behind it is
    // the same one either way.
    for (size_t inputIndex = 0; inputIndex < inputChunks.size(); inputIndex++) {
        if (matchedChunks[inputIndex].getType() != inputChunks[inputIndex].getType()) {
            throw IRException("db.optional_match carries an input column back as another chunk type");
        }
    }

    for (size_t chunkIndex = inputChunks.size(); chunkIndex < matchedChunks.size(); chunkIndex++) {
        matchedChunks[chunkIndex] = paddedColumnChunk(matchedChunks[chunkIndex]);
    }

    setInsertionInto(deepestOwnerBlock(matchedChunks, stepBlock));
    _builder.create<nl::OptionalCollect>(loc, state, matchedTag, matchedChunks);

    if (patternLimit) {
        _builder.create<nl::LimitUpdate>(loc, patternLimit, matchedChunks.front());
    }

    _rootBlock = previousRoot;
    _innermostLoopBody = previousInnermostLoopBody;
    _innermostCardinality = previousInnermostCardinality;

    llvm::SmallVector<mlir::Type, 4> chunkTypes;
    for (const mlir::Value chunk : matchedChunks) {
        chunkTypes.push_back(chunk.getType());
    }

    const nl::IteratorType iteratorType = nl::IteratorType::get(_builder.getContext(), chunkTypes);
    setInsertionInto(stepBlock);
    nl::OptionalDrain drain = _builder.create<nl::OptionalDrain>(loc, iteratorType, state);

    buildLoopForSource(drain.getResult(), optionalMatchOp);
}

void DBLowering::lowerCallSubquery(mlir::db::CallSubquery call) {
    llvm::SmallVector<mlir::Value, 4> inputChunks;
    for (const mlir::Value column : call.getInputColumns()) {
        inputChunks.push_back(mapValue(column));
    }

    // The step the body runs over is the one binding the columns it reads, so its loops
    // nest in that block and re-run once per chunk of them.
    mlir::Block* const stepBlock = deepestOwnerBlock(inputChunks, _rootBlock);

    const bool returning = !call.getUnit();
    if (returning && call.getOptional()) {
        lowerOptionalSubquery(call, stepBlock, inputChunks);
        return;
    } else if (runsPerRow(call)) {
        lowerSubqueryPerRow(call, stepBlock, inputChunks);
        return;
    }

    mlir::Block* const previousInnermostLoopBody = _innermostLoopBody;
    const mlir::Value previousInnermostCardinality = _innermostCardinality;

    llvm::SmallVector<mlir::Value, 4> yieldedChunks;
    mlir::Value yieldedTag;
    lowerSubqueryBody(call, stepBlock, inputChunks, yieldedChunks, yieldedTag);

    // A unit body leaves the rows in flight as they were, so what follows the op reads the
    // step's own chunks again. A body carrying its scope hands the rows on: its yielded
    // chunks are the results, bound in the body's innermost loop, which is where the rest
    // of the query goes on from - unless the body opened no loop and the step is still the
    // relation in flight.
    const bool bodyOpenedALoop = _innermostLoopBody != nullptr;
    if (!returning || !bodyOpenedALoop) {
        _innermostLoopBody = previousInnermostLoopBody;
        _innermostCardinality = previousInnermostCardinality;
    }

    if (!returning) {
        setInsertionInto(stepBlock);
        return;
    }

    const mlir::ResultRange results = call.getResults();
    for (size_t resultIndex = 0; resultIndex < results.size(); resultIndex++) {
        _valueMap[results[resultIndex]] = yieldedChunks[resultIndex];
    }
}

void DBLowering::lowerSubqueryBody(mlir::db::CallSubquery call,
                                   mlir::Block* stepBlock,
                                   llvm::ArrayRef<mlir::Value> inputChunks,
                                   llvm::SmallVectorImpl<mlir::Value>& yieldedChunks,
                                   mlir::Value& yieldedTag) {
    mlir::Block& bodyBlock = call.getBody().front();
    for (size_t inputIndex = 0; inputIndex < inputChunks.size(); inputIndex++) {
        _valueMap[bodyBlock.getArgument(static_cast<unsigned>(inputIndex))] = inputChunks[inputIndex];
    }

    // A dataflow of its own, rooted in the step so its loops nest inside it - and so its
    // accumulators, hoisted to the root, reset once per step. The caller's root comes back
    // once the body is lowered; the innermost loop is left as the body set it.
    mlir::Block* const previousRoot = _rootBlock;
    _rootBlock = stepBlock;
    _innermostLoopBody = nullptr;
    _innermostCardinality = mlir::Value();

    for (mlir::Operation& operation : bodyBlock) {
        mlir::db::SubqueryYield yield = mlir::dyn_cast<mlir::db::SubqueryYield>(operation);
        if (!yield) {
            lowerOperation(operation);
            continue;
        }

        for (const mlir::Value column : yield.getColumns()) {
            yieldedChunks.push_back(mapValue(column));
        }

        if (yield.getTag()) {
            yieldedTag = mapValue(yield.getTag());
        }
    }

    _rootBlock = previousRoot;
}

void DBLowering::lowerSubqueryPerRow(mlir::db::CallSubquery call,
                                     mlir::Block* stepBlock,
                                     llvm::ArrayRef<mlir::Value> inputChunks) {
    const mlir::Location loc = _builder.getUnknownLoc();
    const bool unit = call.getUnit();

    mlir::Block* const previousInnermostLoopBody = _innermostLoopBody;
    const mlir::Value previousInnermostCardinality = _innermostCardinality;

    llvm::SmallVector<mlir::Value, 4> yieldedChunks;

    // No input column means the step is the single empty row Cypher starts from: the body
    // runs once, in the step block, and what it yields is the result
    if (inputChunks.empty()) {
        hoistLimitHandles(call.getBody(), stepBlock, call);

        mlir::Value yieldedTag;
        lowerSubqueryBody(call, stepBlock, inputChunks, yieldedChunks, yieldedTag);

        if (unit) {
            _innermostLoopBody = nullptr;
            _innermostCardinality = mlir::Value();

            setInsertionInto(stepBlock);
        } else if (!_innermostLoopBody) {
            _innermostLoopBody = previousInnermostLoopBody;
            _innermostCardinality = previousInnermostCardinality;

            setInsertionInto(stepBlock);
        }

        const mlir::ResultRange results = call.getResults();
        for (size_t resultIndex = 0; resultIndex < results.size(); resultIndex++) {
            _valueMap[results[resultIndex]] = yieldedChunks[resultIndex];
        }

        return;
    }

    // The loop over the step's rows carries the handle of a limit downstream of the op,
    // as the loops feeding that limit do, so a spent budget stops the walk over the rows
    const mlir::Value limitHandle = loopLimitHandle(call.getOperation());

    setInsertionInto(stepBlock);
    nl::EachRow eachRow = _builder.create<nl::EachRow>(loc, inputChunks);
    nl::For rowLoop = _builder.create<nl::For>(loc, eachRow.getResult(), limitHandle);
    mlir::Block* const rowBody = rowLoop.getBody();

    llvm::SmallVector<mlir::Value, 4> rowChunks;
    for (const mlir::BlockArgument rowChunk : rowBody->getArguments()) {
        rowChunks.push_back(rowChunk);
    }

    // The body roots in the row loop, so its accumulators and the handles of its own
    // limits are hoisted into that body and cover one input row at a time
    hoistLimitHandles(call.getBody(), rowBody, call);

    mlir::Value yieldedTag;
    lowerSubqueryBody(call, rowBody, rowChunks, yieldedChunks, yieldedTag);

    // A unit body hands nothing back, so there is no chunk to pair the row with and no
    // relation for what follows to walk: the step's own rows are in flight still
    if (unit) {
        _innermostLoopBody = previousInnermostLoopBody;
        _innermostCardinality = previousInnermostCardinality;

        setInsertionInto(stepBlock);
        return;
    }

    // A constant the body returned stands for one value over the rows beside it rather
    // than for a row of its own, and the product reads its row count off the first chunk
    // it is handed, so the constants are laid out over those rows first.
    rowAlignFactorChunks(yieldedChunks);

    // One row against N pairs the input row with each of the N rows the body yielded for
    // it, which is the op's result: the inputs then the body's columns
    setInsertionInto(deepestOwnerBlock(yieldedChunks, rowBody));
    nl::CrossProduct cross = _builder.create<nl::CrossProduct>(loc, rowChunks, yieldedChunks);

    buildLoopForSource(cross.getResult(), call.getOperation());
}

void DBLowering::lowerOptionalSubquery(mlir::db::CallSubquery call,
                                       mlir::Block* stepBlock,
                                       llvm::ArrayRef<mlir::Value> inputChunks) {
    const mlir::Location loc = _builder.getUnknownLoc();

    // A body run one row at a time makes the accumulator's step one input row: the loop
    // over the rows is opened first and the body roots in it. A body carrying the scope,
    // and one over the single empty row, run in the step block as they otherwise would.
    const bool perRow = runsPerRow(call) && !inputChunks.empty();

    mlir::Block* bodyRoot = stepBlock;
    llvm::SmallVector<mlir::Value, 4> stepChunks(inputChunks.begin(), inputChunks.end());

    if (perRow) {
        const mlir::Value limitHandle = loopLimitHandle(call.getOperation());

        setInsertionInto(stepBlock);
        nl::EachRow eachRow = _builder.create<nl::EachRow>(loc, inputChunks);
        nl::For rowLoop = _builder.create<nl::For>(loc, eachRow.getResult(), limitHandle);
        bodyRoot = rowLoop.getBody();

        stepChunks.clear();
        for (const mlir::BlockArgument rowChunk : bodyRoot->getArguments()) {
            stepChunks.push_back(rowChunk);
        }
    }

    setInsertionInto(bodyRoot);
    nl::OptionalBuffer buffer = _builder.create<nl::OptionalBuffer>(loc, stepChunks);
    const mlir::Value state = buffer.getState();

    // A body carrying the scope reads the tag through its trailing argument and hands it
    // back; the others are tagged here
    mlir::Block& bodyBlock = call.getBody().front();
    if (bodyBlock.getNumArguments() > inputChunks.size()) {
        _valueMap[bodyBlock.getArgument(static_cast<unsigned>(inputChunks.size()))] = buffer.getTag();
    }

    if (runsPerRow(call)) {
        hoistLimitHandles(call.getBody(), bodyRoot, call);
    }

    llvm::SmallVector<mlir::Value, 4> yieldedChunks;
    mlir::Value yieldedTag;
    lowerSubqueryBody(call, bodyRoot, stepChunks, yieldedChunks, yieldedTag);

    // The accumulator appends one buffer per column and the drain reads them back by
    // position, so a constant the body returned is laid out over the rows beside it before
    // any of them is collected.
    rowAlignFactorChunks(yieldedChunks);

    // A body carrying the scope hands the input columns back ahead of its own. The drain
    // rebuilds those out of the step's chunks rather than padding them, so they stay the
    // chunk the accumulator recorded; only the body's own columns need a null to pad with.
    const size_t carriedInputs = call.getCarriesScope() ? stepChunks.size() : 0;

    for (size_t inputIndex = 0; inputIndex < carriedInputs; inputIndex++) {
        if (yieldedChunks[inputIndex].getType() != stepChunks[inputIndex].getType()) {
            throw IRException("db.call_subquery carries an input column back as another chunk type");
        }
    }

    for (size_t chunkIndex = carriedInputs; chunkIndex < yieldedChunks.size(); chunkIndex++) {
        yieldedChunks[chunkIndex] = paddedColumnChunk(yieldedChunks[chunkIndex]);
    }

    // What the collect appends: the inputs as the body left them then its own columns
    // when it carries the scope, and the row's chunks crossed with its columns when it
    // runs per row - one row against N, with the tag in the outer group so it lines up
    llvm::SmallVector<mlir::Value, 4> collected;
    mlir::Value collectedTag = yieldedTag;

    if (perRow) {
        llvm::SmallVector<mlir::Value, 4> outer(stepChunks.begin(), stepChunks.end());
        outer.push_back(buffer.getTag());

        setInsertionInto(deepestOwnerBlock(yieldedChunks, bodyRoot));
        nl::CrossProduct cross = _builder.create<nl::CrossProduct>(loc, outer, yieldedChunks);
        nl::For pairs = _builder.create<nl::For>(loc, cross.getResult(), mlir::Value {});

        const mlir::Block::BlockArgListType pairChunks = pairs.getBody()->getArguments();
        const size_t inputCount = stepChunks.size();

        collected.assign(pairChunks.begin(), pairChunks.begin() + inputCount);
        collectedTag = pairChunks[inputCount];
        collected.append(pairChunks.begin() + inputCount + 1, pairChunks.end());
    } else {
        collected.assign(yieldedChunks.begin(), yieldedChunks.end());
    }

    setInsertionInto(deepestOwnerBlock(collected, bodyRoot));
    _builder.create<nl::OptionalCollect>(loc, state, collectedTag, collected);

    llvm::SmallVector<mlir::Type, 4> chunkTypes;
    for (const mlir::Value chunk : collected) {
        chunkTypes.push_back(chunk.getType());
    }

    // The drain's loop binds the op's results: the inputs then the body's columns, padded
    // where it yielded nothing
    const nl::IteratorType iteratorType = nl::IteratorType::get(_builder.getContext(), chunkTypes);
    setInsertionInto(bodyRoot);
    nl::OptionalDrain drain = _builder.create<nl::OptionalDrain>(loc, iteratorType, state);

    buildLoopForSource(drain.getResult(), call.getOperation());
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
