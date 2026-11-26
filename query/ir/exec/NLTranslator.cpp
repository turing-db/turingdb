#include "NLTranslator.h"

#include <optional>
#include <type_traits>

#include <spdlog/fmt/bundled/format.h>

#include "NLOps.h"
#include "ParameterMap.h"

#include "IRRowAlignment.h"
#include "mlir/IR/Block.h"
#include "mlir/IR/BuiltinAttributes.h"
#include "mlir/IR/Verifier.h"
#include "llvm/ADT/DenseMap.h"
#include "llvm/ADT/SmallVector.h"

#include "ProcedureContext.h"
#include "columns/Functions.h"
#include "list/ListView.h"
#include "metadata/LabelSet.h"
#include "metadata/PropertyType.h"
#include "views/GraphView.h"
#include "writers/MetadataBuilder.h"

#include "NLRuntimeProperty.h"
#include "NLExecutor.h"
#include "NLSystemTranslator.h"
#include "NLFunctionSelectors.h"
#include "NLTranslatorChunkTypes.h"

#include "LocalMemory.h"
#include "IRException.h"

using namespace db;

namespace nl = mlir::nl;
namespace storage = mlir::storage;

NLTranslator::NLTranslator(NLProgram* program,
                           LocalMemory* memory,
                           const GraphView* view,
                           MetadataBuilder* metadataBuilder,
                           const ProcedureContext* procedureContext,
                           const ParameterMap* parameters)
    : _program(program),
    _memory(memory),
    _view(view),
    _metadataBuilder(metadataBuilder),
    _procedureContext(procedureContext),
    _parameters(parameters),
    _systemTranslator(std::make_unique<NLSystemTranslator>(program, memory, &_valueSlots))
{
}

NLTranslator::~NLTranslator() {
}

void NLTranslator::translate(const mlir::func::FuncOp& function) {
    // Check MLIR verifier on the function
    if (mlir::failed(mlir::verify(function))) {
        throw IRException("nl function failed MLIR verification");
    }

    mlir::Region& bodyRegion = function->getRegion(0);
    if (!bodyRegion.hasOneBlock()) {
        throw IRException("NLTranslator expects a function with a single block");
    }

    bindParameterArguments(function);

    translateBlock(bodyRegion.front(), _program->getStmts());
}

template <typename ScanOp>
void NLTranslator::bindScanEdgesByLabel(ScanOp scan, IteratorKind kind) {
    IteratorConfig config {kind, {}, {}};
    resolveLabelNames(scan.getLabels(), config._labels);

    _iteratorConfigs[scan.getResult()] = config;
}

template <typename HopOp>
void NLTranslator::bindGetEdgesByLabel(HopOp hop, IteratorKind kind) {
    IteratorConfig config {kind, hop.getInputNodes(), {}};

    const mlir::OperandRange carriedColumns = hop.getColumnsToFilter();
    config._carriedColumns.assign(carriedColumns.begin(), carriedColumns.end());

    resolveLabelNames(hop.getLabels(), config._labels);

    _iteratorConfigs[hop.getResult()] = config;
}

template <typename HopOp>
void NLTranslator::bindGetEdgesByTypeAndLabel(HopOp hop, IteratorKind kind) {
    IteratorConfig config {kind, hop.getInputNodes(), {}};

    const mlir::OperandRange carriedColumns = hop.getColumnsToFilter();
    config._carriedColumns.assign(carriedColumns.begin(), carriedColumns.end());

    resolveEdgeTypeNames(hop.getEdgeTypes(), config._edgeTypes);

    resolveLabelNames(hop.getLabels(), config._labels);

    _iteratorConfigs[hop.getResult()] = config;
}

void NLTranslator::translateBlock(mlir::Block& block, NLStmtContainer* body) {
    for (mlir::Operation& operation : block) {
        if (nl::ScanNodes scanNodes = mlir::dyn_cast<nl::ScanNodes>(operation)) {
            _iteratorConfigs[scanNodes.getResult()] = IteratorConfig {IteratorKind::ScanNodes, {}, {}};
        } else if (nl::ScanNodesByLabel scanNodesByLabel = mlir::dyn_cast<nl::ScanNodesByLabel>(operation)) {
            IteratorConfig config {IteratorKind::ScanNodesByLabel, {}, {}};
            resolveLabelNames(scanNodesByLabel.getLabels(), config._labels);
            _iteratorConfigs[scanNodesByLabel.getResult()] = config;
        } else if (nl::ConstScanNodes constScanNodes = mlir::dyn_cast<nl::ConstScanNodes>(operation)) {
            IteratorConfig config {IteratorKind::ConstScanNodes, {}, {}};
            config._listedIDs = constScanNodes.getNodeIDs();
            const mlir::OperandRange boundIDs = constScanNodes.getBoundIDs();
            config._boundIDs.assign(boundIDs.begin(), boundIDs.end());
            _iteratorConfigs[constScanNodes.getResult()] = config;
        } else if (nl::ScanNodesByPropertyValue scanByValue = mlir::dyn_cast<nl::ScanNodesByPropertyValue>(operation)) {
            IteratorConfig config {IteratorKind::ScanNodesByPropertyValue, {}, {}};
            config._property = scanByValue.getProperty();
            config._propertyValue = scanByValue.getValueAttr();
            config._boundPropertyValue = scanByValue.getBoundValue();
            if (const std::optional<mlir::ArrayAttr> labels = scanByValue.getLabels()) {
                resolveLabelNames(*labels, config._labels);
            }
            _iteratorConfigs[scanByValue.getResult()] = config;
        } else if (nl::ScanEdges scanEdges = mlir::dyn_cast<nl::ScanEdges>(operation)) {
            _iteratorConfigs[scanEdges.getResult()] = IteratorConfig {IteratorKind::ScanEdges, {}, {}};
        } else if (nl::ConstScanEdges constScanEdges = mlir::dyn_cast<nl::ConstScanEdges>(operation)) {
            IteratorConfig config {IteratorKind::ConstScanEdges, {}, {}};
            config._listedIDs = constScanEdges.getEdgeIDs();
            const mlir::OperandRange boundIDs = constScanEdges.getBoundIDs();
            config._boundIDs.assign(boundIDs.begin(), boundIDs.end());
            config._undirected = constScanEdges.getUndirected();
            _iteratorConfigs[constScanEdges.getResult()] = config;
        } else if (nl::ScanEdgesByType scanEdgesByType = mlir::dyn_cast<nl::ScanEdgesByType>(operation)) {
            IteratorConfig config {IteratorKind::ScanEdgesByType, {}, {}};
            resolveEdgeTypeNames(scanEdgesByType.getEdgeTypes(), config._edgeTypes);
            _iteratorConfigs[scanEdgesByType.getResult()] = config;
        } else if (nl::ScanOutEdgesByLabelSrc scanOutEdgesByLabelSrc = mlir::dyn_cast<nl::ScanOutEdgesByLabelSrc>(operation)) {
            bindScanEdgesByLabel(scanOutEdgesByLabelSrc, IteratorKind::ScanEdgesBySourceLabel);
        } else if (nl::ScanInEdgesByLabelSrc scanInEdgesByLabelSrc = mlir::dyn_cast<nl::ScanInEdgesByLabelSrc>(operation)) {
            bindScanEdgesByLabel(scanInEdgesByLabelSrc, IteratorKind::ScanEdgesBySourceLabel);
        } else if (nl::ScanInEdgesByLabelTgt scanInEdgesByLabelTgt = mlir::dyn_cast<nl::ScanInEdgesByLabelTgt>(operation)) {
            bindScanEdgesByLabel(scanInEdgesByLabelTgt, IteratorKind::ScanEdgesByTargetLabel);
        } else if (nl::ScanOutEdgesByLabelTgt scanOutEdgesByLabelTgt = mlir::dyn_cast<nl::ScanOutEdgesByLabelTgt>(operation)) {
            bindScanEdgesByLabel(scanOutEdgesByLabelTgt, IteratorKind::ScanEdgesByTargetLabel);
        } else if (nl::ScanOutEdgesByLabelSrcTgt scanOutEdgesByLabelSrcTgt = mlir::dyn_cast<nl::ScanOutEdgesByLabelSrcTgt>(operation)) {
            IteratorConfig config {IteratorKind::ScanEdgesByEndpointLabelsFromSource, {}, {}};
            resolveLabelNames(scanOutEdgesByLabelSrcTgt.getSrcLabels(), config._labels);
            resolveLabelNames(scanOutEdgesByLabelSrcTgt.getTgtLabels(), config._otherEndLabels);
            _iteratorConfigs[scanOutEdgesByLabelSrcTgt.getResult()] = config;
        } else if (nl::ScanInEdgesByLabelSrcTgt scanInEdgesByLabelSrcTgt = mlir::dyn_cast<nl::ScanInEdgesByLabelSrcTgt>(operation)) {
            IteratorConfig config {IteratorKind::ScanEdgesByEndpointLabelsFromTarget, {}, {}};
            resolveLabelNames(scanInEdgesByLabelSrcTgt.getTgtLabels(), config._labels);
            resolveLabelNames(scanInEdgesByLabelSrcTgt.getSrcLabels(), config._otherEndLabels);
            _iteratorConfigs[scanInEdgesByLabelSrcTgt.getResult()] = config;
        } else if (nl::GetOutEdges getOutEdges = mlir::dyn_cast<nl::GetOutEdges>(operation)) {
            IteratorConfig config {IteratorKind::GetOutEdges, getOutEdges.getInputNodes(), {}};
            const mlir::OperandRange carriedColumns = getOutEdges.getColumnsToFilter();
            config._carriedColumns.assign(carriedColumns.begin(), carriedColumns.end());
            _iteratorConfigs[getOutEdges.getResult()] = config;
        } else if (nl::GetInEdges getInEdges = mlir::dyn_cast<nl::GetInEdges>(operation)) {
            IteratorConfig config {IteratorKind::GetInEdges, getInEdges.getInputNodes(), {}};
            const mlir::OperandRange carriedColumns = getInEdges.getColumnsToFilter();
            config._carriedColumns.assign(carriedColumns.begin(), carriedColumns.end());
            _iteratorConfigs[getInEdges.getResult()] = config;
        } else if (nl::GetEdges getEdges = mlir::dyn_cast<nl::GetEdges>(operation)) {
            IteratorConfig config {IteratorKind::GetEdges, getEdges.getInputNodes(), {}};
            const mlir::OperandRange carriedColumns = getEdges.getColumnsToFilter();
            config._carriedColumns.assign(carriedColumns.begin(), carriedColumns.end());
            _iteratorConfigs[getEdges.getResult()] = config;
        } else if (nl::GetOutEdgesByType getOutEdgesByType = mlir::dyn_cast<nl::GetOutEdgesByType>(operation)) {
            IteratorConfig config {IteratorKind::GetOutEdgesByType, getOutEdgesByType.getInputNodes(), {}};
            const mlir::OperandRange carriedColumns = getOutEdgesByType.getColumnsToFilter();
            config._carriedColumns.assign(carriedColumns.begin(), carriedColumns.end());
            resolveEdgeTypeNames(getOutEdgesByType.getEdgeTypes(), config._edgeTypes);
            _iteratorConfigs[getOutEdgesByType.getResult()] = config;
        } else if (nl::GetInEdgesByType getInEdgesByType = mlir::dyn_cast<nl::GetInEdgesByType>(operation)) {
            IteratorConfig config {IteratorKind::GetInEdgesByType, getInEdgesByType.getInputNodes(), {}};
            const mlir::OperandRange carriedColumns = getInEdgesByType.getColumnsToFilter();
            config._carriedColumns.assign(carriedColumns.begin(), carriedColumns.end());
            resolveEdgeTypeNames(getInEdgesByType.getEdgeTypes(), config._edgeTypes);
            _iteratorConfigs[getInEdgesByType.getResult()] = config;
        } else if (nl::GetOutEdgesByLabel getOutEdgesByLabel = mlir::dyn_cast<nl::GetOutEdgesByLabel>(operation)) {
            bindGetEdgesByLabel(getOutEdgesByLabel, IteratorKind::GetOutEdgesByLabel);
        } else if (nl::GetInEdgesByLabel getInEdgesByLabel = mlir::dyn_cast<nl::GetInEdgesByLabel>(operation)) {
            bindGetEdgesByLabel(getInEdgesByLabel, IteratorKind::GetInEdgesByLabel);
        } else if (nl::GetOutEdgesByTypeAndLabel getOutEdgesByTypeAndLabel = mlir::dyn_cast<nl::GetOutEdgesByTypeAndLabel>(operation)) {
            bindGetEdgesByTypeAndLabel(getOutEdgesByTypeAndLabel, IteratorKind::GetOutEdgesByTypeAndLabel);
        } else if (nl::GetInEdgesByTypeAndLabel getInEdgesByTypeAndLabel = mlir::dyn_cast<nl::GetInEdgesByTypeAndLabel>(operation)) {
            bindGetEdgesByTypeAndLabel(getInEdgesByTypeAndLabel, IteratorKind::GetInEdgesByTypeAndLabel);
        } else if (nl::ExplorePaths explorePaths = mlir::dyn_cast<nl::ExplorePaths>(operation)) {
            IteratorConfig config {IteratorKind::ExplorePaths, explorePaths.getInputNodes(), {}};
            const mlir::OperandRange carriedColumns = explorePaths.getColumnsToFilter();
            config._carriedColumns.assign(carriedColumns.begin(), carriedColumns.end());
            config._explorePaths = explorePaths;
            config._minHops = explorePaths.getMinHops();
            config._maxHops = explorePaths.getMaxHops().value_or(std::numeric_limits<uint64_t>::max());
            const mlir::OperandRange hopImports = explorePaths.getHopImports();
            config._hopImports.assign(hopImports.begin(), hopImports.end());
            if (const std::optional<mlir::ArrayAttr> endLabels = explorePaths.getEndLabels()) {
                resolveLabelNames(*endLabels, config._labels);
            }
            config._endColumn = explorePaths.getEndColumn();
            config._endsOnSeed = explorePaths.getEndsOnSeed();
            if (const mlir::Value endNodes = explorePaths.getEndNodes()) {
                config._endNodeSet = nodeSetStateFor(endNodes);
            }
            config._distinctEnds = explorePaths.getDistinct();
            _iteratorConfigs[explorePaths.getResult()] = config;
        } else if (nl::Sort sort = mlir::dyn_cast<nl::Sort>(operation)) {
            _iteratorConfigs[sort.getResult()] = IteratorConfig {IteratorKind::Sort, {}, {}, sortStateFor(sort.getState())};
        } else if (nl::GroupAggregate groupAggregate = mlir::dyn_cast<nl::GroupAggregate>(operation)) {
            IteratorConfig config;
            config._kind = IteratorKind::GroupAggregate;
            config._groupAggregateState = groupAggregateStateFor(groupAggregate.getState());
            _iteratorConfigs[groupAggregate.getResult()] = config;
        } else if (nl::UnwindCollect unwind = mlir::dyn_cast<nl::UnwindCollect>(operation)) {
            IteratorConfig config;
            config._kind = IteratorKind::UnwindCollect;
            config._collectState = collectStateFor(unwind.getState());
            _iteratorConfigs[unwind.getResult()] = config;
        } else if (nl::Collect collect = mlir::dyn_cast<nl::Collect>(operation)) {
            IteratorConfig config;
            config._kind = IteratorKind::Collect;
            config._collectState = collectStateFor(collect.getState());
            _iteratorConfigs[collect.getResult()] = config;
        } else if (nl::ShortestPath shortestPath = mlir::dyn_cast<nl::ShortestPath>(operation)) {
            IteratorConfig config;
            config._kind = IteratorKind::ShortestPath;
            config._shortestPathState = shortestPathStateFor(shortestPath.getState());
            _iteratorConfigs[shortestPath.getResult()] = config;
        } else if (nl::UnwindConst unwindConst = mlir::dyn_cast<nl::UnwindConst>(operation)) {
            IteratorConfig config;
            config._kind = IteratorKind::UnwindConst;
            config._list = materializeListView(unwindConst.getElements());
            _iteratorConfigs[unwindConst.getResult()] = config;
        } else if (nl::LoadCSV loadCSV = mlir::dyn_cast<nl::LoadCSV>(operation)) {
            IteratorConfig config;
            config._kind = IteratorKind::LoadCSV;
            config._csvPath = loadCSV.getPath().value_or(llvm::StringRef());
            config._csvBoundPath = loadCSV.getBoundPath();
            config._csvFields = loadCSV.getFieldsAttr();
            config._csvHasHeaders = loadCSV.getWithHeaders();
            config._csvSkipOnError = loadCSV.getSkipOnError();
            _iteratorConfigs[loadCSV.getResult()] = config;
        } else if (nl::VectorSearch vectorSearch = mlir::dyn_cast<nl::VectorSearch>(operation)) {
            IteratorConfig config;
            config._kind = IteratorKind::VectorSearch;
            config._indexName = vectorSearch.getIndexName().value_or(llvm::StringRef());
            config._boundIndexName = vectorSearch.getBoundIndexName();
            config._neighbourCount = vectorSearch.getK().value_or(0);
            config._boundNeighbourCount = vectorSearch.getBoundK();
            config._queryVector = vectorSearch.getQueryVector().value_or(llvm::ArrayRef<float>());
            config._boundQueryVector = vectorSearch.getBoundQueryVector();
            _iteratorConfigs[vectorSearch.getResult()] = config;
        } else if (nl::Unwind unwind = mlir::dyn_cast<nl::Unwind>(operation)) {
            IteratorConfig config;
            config._kind = IteratorKind::Unwind;
            config._source = unwind.getSource();

            const mlir::OperandRange carriedColumns = unwind.getColumnsToFilter();
            config._carriedColumns.assign(carriedColumns.begin(), carriedColumns.end());

            _iteratorConfigs[unwind.getResult()] = config;
        } else if (nl::UnwindRange unwindRange = mlir::dyn_cast<nl::UnwindRange>(operation)) {
            IteratorConfig config;
            config._kind = IteratorKind::UnwindRange;
            config._rangeBounds = {unwindRange.getStart(),
                                   unwindRange.getEnd(),
                                   unwindRange.getStep(),
                                   unwindRange.getFrom(),
                                   unwindRange.getTo()};
            config._rangeReversed = unwindRange.getReversed();

            const mlir::OperandRange carriedColumns = unwindRange.getColumnsToFilter();
            config._carriedColumns.assign(carriedColumns.begin(), carriedColumns.end());

            _iteratorConfigs[unwindRange.getResult()] = config;
        } else if (nl::UnwindComputed unwindComputed = mlir::dyn_cast<nl::UnwindComputed>(operation)) {
            IteratorConfig config;
            config._kind = IteratorKind::UnwindComputed;
            config._unwindSource = &unwindComputed.getSource();

            const mlir::OperandRange carriedColumns = unwindComputed.getColumnsToFilter();
            config._carriedColumns.assign(carriedColumns.begin(), carriedColumns.end());

            _iteratorConfigs[unwindComputed.getResult()] = config;
        } else if (nl::OptionalDrain optionalDrain = mlir::dyn_cast<nl::OptionalDrain>(operation)) {
            IteratorConfig config;
            config._kind = IteratorKind::OptionalDrain;
            config._optionalState = optionalStateFor(optionalDrain.getState());
            _iteratorConfigs[optionalDrain.getResult()] = config;
        } else if (nl::RowDrain rowDrain = mlir::dyn_cast<nl::RowDrain>(operation)) {
            IteratorConfig config;
            config._kind = IteratorKind::RowDrain;
            config._rowState = rowStateFor(rowDrain.getState());
            _iteratorConfigs[rowDrain.getResult()] = config;
        } else if (nl::ProcedureInit procedureInit = mlir::dyn_cast<nl::ProcedureInit>(operation)) {
            IteratorConfig config;
            config._kind = IteratorKind::ProcedureInit;
            config._procedureState = procedureStateFor(procedureInit.getState());

            const mlir::OperandRange procedureInputs = procedureInit.getInputs();
            config._procedureInputs.assign(procedureInputs.begin(), procedureInputs.end());

            const mlir::OperandRange carriedColumns = procedureInit.getColumnsToFilter();
            config._carriedColumns.assign(carriedColumns.begin(), carriedColumns.end());

            _iteratorConfigs[procedureInit.getResult()] = config;
        } else if (nl::For forLoop = mlir::dyn_cast<nl::For>(operation)) {
            translateFor(forLoop, body);
        } else if (mlir::isa<nl::GetPropertyType, nl::GetEdgeTypeSet>(operation)) {
            // The handle carries only names; a fetch/hop resolves them on consumption
        } else if (nl::Constant constant = mlir::dyn_cast<nl::Constant>(operation)) {
            translateConstant(constant);
        } else if (nl::CurrentDateTime currentDateTime = mlir::dyn_cast<nl::CurrentDateTime>(operation)) {
            translateCurrentDateTime(currentDateTime);
        } else if (nl::BroadcastConstant broadcast = mlir::dyn_cast<nl::BroadcastConstant>(operation)) {
            translateBroadcastConstant(broadcast, body);
        } else if (nl::RowConstant rowConstant = mlir::dyn_cast<nl::RowConstant>(operation)) {
            translateRowConstant(rowConstant, body);
        } else if (nl::Add add = mlir::dyn_cast<nl::Add>(operation)) {
            translateBinaryOp<OP_ADD>(add, body);
        } else if (nl::Concat concat = mlir::dyn_cast<nl::Concat>(operation)) {
            translateBinaryOp<OP_CONCAT>(concat, body);
        } else if (nl::ListIndex index = mlir::dyn_cast<nl::ListIndex>(operation)) {
            translateListIndex(index, body);
        } else if (nl::Sub sub = mlir::dyn_cast<nl::Sub>(operation)) {
            translateBinaryOp<OP_SUB>(sub, body);
        } else if (nl::Mul mul = mlir::dyn_cast<nl::Mul>(operation)) {
            translateBinaryOp<OP_MUL>(mul, body);
        } else if (nl::Div div = mlir::dyn_cast<nl::Div>(operation)) {
            translateBinaryOp<OP_DIV>(div, body);
        } else if (nl::Mod mod = mlir::dyn_cast<nl::Mod>(operation)) {
            translateBinaryOp<OP_MOD>(mod, body);
        } else if (nl::Pow pow = mlir::dyn_cast<nl::Pow>(operation)) {
            translateBinaryOp<OP_POW>(pow, body);
        } else if (nl::Eq eq = mlir::dyn_cast<nl::Eq>(operation)) {
            translateBinaryOp<OP_EQUAL>(eq, body);
        } else if (nl::Neq neq = mlir::dyn_cast<nl::Neq>(operation)) {
            translateBinaryOp<OP_NOT_EQUAL>(neq, body);
        } else if (nl::Gt gt = mlir::dyn_cast<nl::Gt>(operation)) {
            translateBinaryOp<OP_GREATER_THAN>(gt, body);
        } else if (nl::Lt lt = mlir::dyn_cast<nl::Lt>(operation)) {
            translateBinaryOp<OP_LESS_THAN>(lt, body);
        } else if (nl::Gte gte = mlir::dyn_cast<nl::Gte>(operation)) {
            translateBinaryOp<OP_GREATER_THAN_OR_EQUAL>(gte, body);
        } else if (nl::Lte lte = mlir::dyn_cast<nl::Lte>(operation)) {
            translateBinaryOp<OP_LESS_THAN_OR_EQUAL>(lte, body);
        } else if (nl::StartsWith startsWith = mlir::dyn_cast<nl::StartsWith>(operation)) {
            translateBinaryOp<OP_STARTS_WITH>(startsWith, body);
        } else if (nl::EndsWith endsWith = mlir::dyn_cast<nl::EndsWith>(operation)) {
            translateBinaryOp<OP_ENDS_WITH>(endsWith, body);
        } else if (nl::Contains containsOp = mlir::dyn_cast<nl::Contains>(operation)) {
            translateBinaryOp<OP_CONTAINS>(containsOp, body);
        } else if (nl::In inOp = mlir::dyn_cast<nl::In>(operation)) {
            translateBinaryOp<OP_IN>(inOp, body);
        } else if (nl::IndexOf indexOf = mlir::dyn_cast<nl::IndexOf>(operation)) {
            translateBinaryOp<OP_INDEX_OF>(indexOf, body);
        } else if (nl::And andOp = mlir::dyn_cast<nl::And>(operation)) {
            translateBinaryOp<OP_AND>(andOp, body);
        } else if (nl::Or orOp = mlir::dyn_cast<nl::Or>(operation)) {
            translateBinaryOp<OP_OR>(orOp, body);
        } else if (nl::Xor xorOp = mlir::dyn_cast<nl::Xor>(operation)) {
            translateBinaryOp<OP_XOR>(xorOp, body);
        } else if (nl::Not notOp = mlir::dyn_cast<nl::Not>(operation)) {
            translateNot(notOp, body);
        } else if (nl::ToNullable toNullable = mlir::dyn_cast<nl::ToNullable>(operation)) {
            translateToNullable(toNullable, body);
        } else if (nl::CheckedCast checkedCast = mlir::dyn_cast<nl::CheckedCast>(operation)) {
            translateCheckedCast(checkedCast, body);
        } else if (nl::ToAny toAny = mlir::dyn_cast<nl::ToAny>(operation)) {
            translateToAny(toAny, body);
        } else if (nl::Case caseOp = mlir::dyn_cast<nl::Case>(operation)) {
            translateCase(caseOp, &NLExecutor::runCase, body);
        } else if (nl::CaseMerge caseMerge = mlir::dyn_cast<nl::CaseMerge>(operation)) {
            translateCase(caseMerge, &NLExecutor::runCaseMerge, body);
        } else if (nl::FillNulls fillNulls = mlir::dyn_cast<nl::FillNulls>(operation)) {
            translateFillNulls(fillNulls, body);
        } else if (nl::IsNotTrue isNotTrue = mlir::dyn_cast<nl::IsNotTrue>(operation)) {
            translateIsNotTrue(isNotTrue, body);
        } else if (nl::IsNull isNull = mlir::dyn_cast<nl::IsNull>(operation)) {
            translateNullTest(isNull, body);
        } else if (nl::IsNotNull isNotNull = mlir::dyn_cast<nl::IsNotNull>(operation)) {
            translateNullTest(isNotNull, body);
        } else if (nl::MakeList makeList = mlir::dyn_cast<nl::MakeList>(operation)) {
            translateMakeList(makeList, body);
        } else if (nl::ListSlice listSlice = mlir::dyn_cast<nl::ListSlice>(operation)) {
            translateListSlice(listSlice, body);
        } else if (nl::SortMulti sortMulti = mlir::dyn_cast<nl::SortMulti>(operation)) {
            translateSortMulti(sortMulti, body);
        } else if (mlir::isa<nl::PercentileCont, nl::PercentileDisc>(operation)) {
            translatePercentile(&operation, body);
        } else if (nl::MakeMap makeMap = mlir::dyn_cast<nl::MakeMap>(operation)) {
            translateMakeMap(makeMap, body);
        } else if (nl::MergeMaps merge = mlir::dyn_cast<nl::MergeMaps>(operation)) {
            translateMergeMaps(merge, body);
        } else if (nl::StaticMapKey mapKey = mlir::dyn_cast<nl::StaticMapKey>(operation)) {
            translateStaticMapKey(mapKey, body);
        } else if (nl::DynamicMapKey dynamicMapKey = mlir::dyn_cast<nl::DynamicMapKey>(operation)) {
            translateDynamicMapKey(dynamicMapKey, body);
        } else if (nl::ContainerIndex containerIndex = mlir::dyn_cast<nl::ContainerIndex>(operation)) {
            translateContainerIndex(containerIndex, body);
        } else if (nl::Range range = mlir::dyn_cast<nl::Range>(operation)) {
            translateRange(range, body);
        } else if (nl::Substring substring = mlir::dyn_cast<nl::Substring>(operation)) {
            translateSubstring(substring, body);
        } else if (nl::Split split = mlir::dyn_cast<nl::Split>(operation)) {
            translateSplit(split, body);
        } else if (nl::Left left = mlir::dyn_cast<nl::Left>(operation)) {
            translateStringEnd<LeftFunction>(left, body);
        } else if (nl::Right right = mlir::dyn_cast<nl::Right>(operation)) {
            translateStringEnd<RightFunction>(right, body);
        } else if (nl::Replace replace = mlir::dyn_cast<nl::Replace>(operation)) {
            translateReplace(replace, body);
        } else if (mlir::isa<nl::PointDistance>(operation)) {
            translatePointFunction(&operation, &NLExecutor::runPointDistance, body);
        } else if (mlir::isa<nl::PointWithinBBox>(operation)) {
            translatePointFunction(&operation, &NLExecutor::runPointWithinBBox, body);
        } else if (nl::ListComprehension listComprehension = mlir::dyn_cast<nl::ListComprehension>(operation)) {
            translateListComprehension(listComprehension, body);
        } else if (nl::ListPredicate listPredicate = mlir::dyn_cast<nl::ListPredicate>(operation)) {
            translateListPredicate(listPredicate, body);
        } else if (nl::Reduce reduce = mlir::dyn_cast<nl::Reduce>(operation)) {
            translateReduce(reduce, body);
        } else if (nl::AllReduce allReduce = mlir::dyn_cast<nl::AllReduce>(operation)) {
            translateAllReduce(allReduce, body);
        } else if (lookupUnaryFunctionSelector(operation)) {
            translateUnaryFunction(&operation, body);
        } else if (lookupBinaryFunctionSelector(operation)) {
            translateBinaryFunction(&operation, body);
        } else if (nl::Filter filter = mlir::dyn_cast<nl::Filter>(operation)) {
            translateFilter(filter, body);
        } else if (nl::FetchNodes fetch = mlir::dyn_cast<nl::FetchNodes>(operation)) {
            translateFetchNodes(fetch, body);
        } else if (nl::FetchEdges fetch = mlir::dyn_cast<nl::FetchEdges>(operation)) {
            translateFetchEdges(fetch, body);
        } else if (nl::GetNodeProperties getNodeProperties = mlir::dyn_cast<nl::GetNodeProperties>(operation)) {
            translatePropertyFetch(getNodeProperties.getInputNodes(),
                                   getNodeProperties.getPropertyType(),
                                   getNodeProperties.getPending(),
                                   getNodeProperties.getAllPending(),
                                   getNodeProperties.getValues(),
                                   /*isNode=*/true,
                                   body);
        } else if (nl::GetEdgeProperties getEdgeProperties = mlir::dyn_cast<nl::GetEdgeProperties>(operation)) {
            translatePropertyFetch(getEdgeProperties.getInputEdges(),
                                   getEdgeProperties.getPropertyType(),
                                   getEdgeProperties.getPending(),
                                   getEdgeProperties.getAllPending(),
                                   getEdgeProperties.getValues(),
                                   /*isNode=*/false,
                                   body);
        } else if (nl::ExpandPath expandPath = mlir::dyn_cast<nl::ExpandPath>(operation)) {
            translateExpandPath(expandPath, body);
        } else if (nl::MakePath makePath = mlir::dyn_cast<nl::MakePath>(operation)) {
            translateMakePath(makePath, body);
        } else if (nl::PathLength pathLength = mlir::dyn_cast<nl::PathLength>(operation)) {
            translatePathLength(pathLength, body);
        } else if (nl::PathElements pathElements = mlir::dyn_cast<nl::PathElements>(operation)) {
            translatePathElements(pathElements, body);
        } else if (nl::GetNodeLabelSet getNodeLabelSet = mlir::dyn_cast<nl::GetNodeLabelSet>(operation)) {
            translateGetNodeLabelSet(getNodeLabelSet, body);
        } else if (nl::GetEdgeTypes getEdgeTypes = mlir::dyn_cast<nl::GetEdgeTypes>(operation)) {
            translateGetEdgeTypes(getEdgeTypes, body);
        } else if (nl::CheckLabelConstraint checkLabelConstraint = mlir::dyn_cast<nl::CheckLabelConstraint>(operation)) {
            translateCheckLabelConstraint(checkLabelConstraint, body);
        } else if (nl::CheckEdgeTypeConstraint checkEdgeTypeConstraint = mlir::dyn_cast<nl::CheckEdgeTypeConstraint>(operation)) {
            translateCheckEdgeTypeConstraint(checkEdgeTypeConstraint, body);
        } else if (nl::EachRow eachRow = mlir::dyn_cast<nl::EachRow>(operation)) {
            IteratorConfig config;
            config._kind = IteratorKind::EachRow;

            const mlir::OperandRange columns = eachRow.getColumns();
            config._eachRowColumns.assign(columns.begin(), columns.end());

            _iteratorConfigs[eachRow.getResult()] = config;
        } else if (nl::CrossProduct crossProduct = mlir::dyn_cast<nl::CrossProduct>(operation)) {
            IteratorConfig config;
            config._kind = IteratorKind::CrossProduct;

            const mlir::OperandRange outerColumns = crossProduct.getOuterColumns();
            config._crossOuterColumns.assign(outerColumns.begin(), outerColumns.end());

            const mlir::OperandRange innerColumns = crossProduct.getInnerColumns();
            config._crossInnerColumns.assign(innerColumns.begin(), innerColumns.end());

            _iteratorConfigs[crossProduct.getResult()] = config;
        } else if (nl::Limit limit = mlir::dyn_cast<nl::Limit>(operation)) {
            translateLimit(limit, body);
        } else if (nl::LimitRemaining remaining = mlir::dyn_cast<nl::LimitRemaining>(operation)) {
            translateLimitRemaining(remaining, body);
        } else if (nl::LimitUpdate update = mlir::dyn_cast<nl::LimitUpdate>(operation)) {
            translateLimitUpdate(update, body);
        } else if (nl::LimitTruncate truncate = mlir::dyn_cast<nl::LimitTruncate>(operation)) {
            translateLimitTruncate(truncate, body);
        } else if (nl::Skip skip = mlir::dyn_cast<nl::Skip>(operation)) {
            translateSkip(skip, body);
        } else if (nl::SkipUpdate update = mlir::dyn_cast<nl::SkipUpdate>(operation)) {
            translateSkipUpdate(update, body);
        } else if (nl::SkipTruncate truncate = mlir::dyn_cast<nl::SkipTruncate>(operation)) {
            translateSkipTruncate(truncate, body);
        } else if (nl::SortBuffer sortBuffer = mlir::dyn_cast<nl::SortBuffer>(operation)) {
            translateSortBuffer(sortBuffer, body);
        } else if (nl::RowBuffer rowBuffer = mlir::dyn_cast<nl::RowBuffer>(operation)) {
            translateRowBuffer(rowBuffer, body);
        } else if (nl::RowCollect rowCollect = mlir::dyn_cast<nl::RowCollect>(operation)) {
            translateRowCollect(rowCollect, body);
        } else if (nl::SortCollect sortCollect = mlir::dyn_cast<nl::SortCollect>(operation)) {
            translateSortCollect(sortCollect, body);
        } else if (nl::HashJoinBuffer hashJoinBuffer = mlir::dyn_cast<nl::HashJoinBuffer>(operation)) {
            translateHashJoinBuffer(hashJoinBuffer, body);
        } else if (nl::HashJoinCollect hashJoinCollect = mlir::dyn_cast<nl::HashJoinCollect>(operation)) {
            translateHashJoinCollect(hashJoinCollect, body);
        } else if (nl::HashJoinProbe hashJoinProbe = mlir::dyn_cast<nl::HashJoinProbe>(operation)) {
            IteratorConfig config;
            config._kind = IteratorKind::HashJoinProbe;
            config._hashJoinState = hashJoinProbe.getState();

            const mlir::OperandRange probeColumns = hashJoinProbe.getColumns();
            config._probeColumns.assign(probeColumns.begin(), probeColumns.end());

            _iteratorConfigs[hashJoinProbe.getResult()] = config;
        } else if (nl::Distinct distinct = mlir::dyn_cast<nl::Distinct>(operation)) {
            translateDistinctState(distinct, body);
        } else if (nl::DistinctFilter distinctFilter = mlir::dyn_cast<nl::DistinctFilter>(operation)) {
            translateDistinctFilter(distinctFilter, body);
        } else if (nl::Count count = mlir::dyn_cast<nl::Count>(operation)) {
            translateCountState(count, body);
        } else if (nl::CountUpdate countUpdate = mlir::dyn_cast<nl::CountUpdate>(operation)) {
            translateCountUpdate(countUpdate, body);
        } else if (nl::CountResult countResult = mlir::dyn_cast<nl::CountResult>(operation)) {
            translateCountResult(countResult, body);
        } else if (nl::CountScanRows countScanRows = mlir::dyn_cast<nl::CountScanRows>(operation)) {
            translateCountScanRows(countScanRows, body);
        } else if (nl::Aggregate aggregate = mlir::dyn_cast<nl::Aggregate>(operation)) {
            translateAggregateState(aggregate, body);
        } else if (nl::AggregateUpdate aggregateUpdate = mlir::dyn_cast<nl::AggregateUpdate>(operation)) {
            translateAggregateUpdate(aggregateUpdate, body);
        } else if (nl::AggregateResult aggregateResult = mlir::dyn_cast<nl::AggregateResult>(operation)) {
            translateAggregateResult(aggregateResult, body);
        } else if (nl::GroupAggregateBuffer groupBuffer = mlir::dyn_cast<nl::GroupAggregateBuffer>(operation)) {
            translateGroupAggregateBuffer(groupBuffer, body);
        } else if (nl::GroupAggregateUpdate groupUpdate = mlir::dyn_cast<nl::GroupAggregateUpdate>(operation)) {
            translateGroupAggregateUpdate(groupUpdate, body);
        } else if (nl::CollectBuffer collectBuffer = mlir::dyn_cast<nl::CollectBuffer>(operation)) {
            translateCollectBuffer(collectBuffer, body);
        } else if (nl::CollectUpdate collectUpdate = mlir::dyn_cast<nl::CollectUpdate>(operation)) {
            translateCollectUpdate(collectUpdate, body);
        } else if (nl::NodeSetBuffer nodeSetBuffer = mlir::dyn_cast<nl::NodeSetBuffer>(operation)) {
            translateNodeSetBuffer(nodeSetBuffer, body);
        } else if (nl::NodeSetCollect nodeSetCollect = mlir::dyn_cast<nl::NodeSetCollect>(operation)) {
            translateNodeSetCollect(nodeSetCollect, body);
        } else if (nl::ShortestPathBuffer shortestPathBuffer = mlir::dyn_cast<nl::ShortestPathBuffer>(operation)) {
            translateShortestPathBuffer(shortestPathBuffer, body);
        } else if (nl::ShortestPathUpdate shortestPathUpdate = mlir::dyn_cast<nl::ShortestPathUpdate>(operation)) {
            translateShortestPathUpdate(shortestPathUpdate, body);
        } else if (nl::OptionalBuffer optionalBuffer = mlir::dyn_cast<nl::OptionalBuffer>(operation)) {
            translateOptionalBuffer(optionalBuffer, body);
        } else if (nl::OptionalCollect optionalCollect = mlir::dyn_cast<nl::OptionalCollect>(operation)) {
            translateOptionalCollect(optionalCollect, body);
        } else if (nl::ExistsBuffer existsBuffer = mlir::dyn_cast<nl::ExistsBuffer>(operation)) {
            translateExistsBuffer(existsBuffer, body);
        } else if (nl::ExistsMark existsMark = mlir::dyn_cast<nl::ExistsMark>(operation)) {
            translateExistsMark(existsMark, body);
        } else if (nl::ExistsResult existsResult = mlir::dyn_cast<nl::ExistsResult>(operation)) {
            translateExistsResult(existsResult, body);
        } else if (nl::CountSubqueryBuffer countBuffer = mlir::dyn_cast<nl::CountSubqueryBuffer>(operation)) {
            translateCountSubqueryBuffer(countBuffer, body);
        } else if (nl::CountSubqueryTally countTally = mlir::dyn_cast<nl::CountSubqueryTally>(operation)) {
            translateCountSubqueryTally(countTally, body);
        } else if (nl::CountSubqueryResult countResult = mlir::dyn_cast<nl::CountSubqueryResult>(operation)) {
            translateCountSubqueryResult(countResult, body);
        } else if (nl::RowListsBuffer rowListsBuffer = mlir::dyn_cast<nl::RowListsBuffer>(operation)) {
            translateRowListsBuffer(rowListsBuffer, body);
        } else if (nl::RowListsCollect rowListsCollect = mlir::dyn_cast<nl::RowListsCollect>(operation)) {
            translateRowListsCollect(rowListsCollect, body);
        } else if (nl::RowListsResult rowListsResult = mlir::dyn_cast<nl::RowListsResult>(operation)) {
            translateRowListsResult(rowListsResult, body);
        } else if (nl::CreateNode createNode = mlir::dyn_cast<nl::CreateNode>(operation)) {
            translateCreateNode(createNode, body);
        } else if (nl::CreateEdge createEdge = mlir::dyn_cast<nl::CreateEdge>(operation)) {
            translateCreateEdge(createEdge, body);
        } else if (nl::Merge merge = mlir::dyn_cast<nl::Merge>(operation)) {
            translateMerge(merge, body);
        } else if (nl::SetNodeProperty setNodeProperty = mlir::dyn_cast<nl::SetNodeProperty>(operation)) {
            translateSetNodeProperty(setNodeProperty, body);
        } else if (nl::SetEdgeProperty setEdgeProperty = mlir::dyn_cast<nl::SetEdgeProperty>(operation)) {
            translateSetEdgeProperty(setEdgeProperty, body);
        } else if (nl::SetNodeProperties setNodeProperties = mlir::dyn_cast<nl::SetNodeProperties>(operation)) {
            translateSetProperties<NLSetNodePropertiesData>(setNodeProperties, setNodeProperties.getInputNodes(), body);
        } else if (nl::SetEdgeProperties setEdgeProperties = mlir::dyn_cast<nl::SetEdgeProperties>(operation)) {
            translateSetProperties<NLSetEdgePropertiesData>(setEdgeProperties, setEdgeProperties.getInputEdges(), body);
        } else if (nl::DeleteNode deleteNode = mlir::dyn_cast<nl::DeleteNode>(operation)) {
            translateDeleteNode(deleteNode, body);
        } else if (nl::DeleteEdge deleteEdge = mlir::dyn_cast<nl::DeleteEdge>(operation)) {
            translateDeleteEdge(deleteEdge, body);
        } else if (nl::Procedure procedureOp = mlir::dyn_cast<nl::Procedure>(operation)) {
            translateProcedure(procedureOp, body);
        } else if (nl::Output output = mlir::dyn_cast<nl::Output>(operation)) {
            translateOutput(output, body);
        } else if (mlir::isa<nl::Yield, nl::ComprehensionYield, nl::ReduceYield, nl::AllReduceYield, nl::UnwindYield, mlir::func::ReturnOp>(operation)) {
            // Structural terminators carry no behavior
        } else if (!_systemTranslator->translate(operation, body)) {
            throw IRException(fmt::format("NLTranslator cannot translate operation '{}'",
                                          operation.getName().getStringRef().str()));
        }
    }
}

void NLTranslator::translateFor(nl::For forLoop, NLStmtContainer* body) {
    // Fetch the iterator associated to this loop
    const auto configIt = _iteratorConfigs.find(forLoop.getIterator());
    if (configIt == _iteratorConfigs.end()) {
        throw IRException("nl.for iterator must be produced by an nl source operation");
    }

    const IteratorConfig& config = configIt->second;
    mlir::Block& loopBody = forLoop->getRegion(0).front();

    // A loop that names a limit handle stops once that counter is spent; the
    // handle was produced by an nl.limit translated earlier, so its counter is
    // already mapped.
    NLLimitState* limit = limitStateFor(forLoop.getLimit());

    // Translate the loop differently depending on the kind of iterator associated
    if (config._kind == IteratorKind::ScanNodes) {
        translateScanLoop(loopBody, limit, body);
    } else if (config._kind == IteratorKind::ScanNodesByLabel) {
        translateScanByLabelLoop(config, loopBody, limit, body);
    } else if (config._kind == IteratorKind::ConstScanNodes) {
        translateConstScanLoop(config, loopBody, limit, body);
    } else if (config._kind == IteratorKind::ScanNodesByPropertyValue) {
        translateScanByPropertyValueLoop(config, loopBody, limit, body);
    } else if (config._kind == IteratorKind::ScanEdges) {
        translateScanEdgesLoop(loopBody, limit, body);
    } else if (config._kind == IteratorKind::ConstScanEdges) {
        translateConstScanEdgesLoop(config, loopBody, limit, body);
    } else if (config._kind == IteratorKind::ScanEdgesByType) {
        translateScanEdgesByTypeLoop(config, loopBody, limit, body);
    } else if (config._kind == IteratorKind::ScanEdgesBySourceLabel) {
        translateScanEdgesByLabelLoop(config, loopBody, limit, body, &NLExecutor::runScanEdgesBySourceLabelLoop);
    } else if (config._kind == IteratorKind::ScanEdgesByTargetLabel) {
        translateScanEdgesByLabelLoop(config, loopBody, limit, body, &NLExecutor::runScanEdgesByTargetLabelLoop);
    } else if (config._kind == IteratorKind::ScanEdgesByEndpointLabelsFromSource) {
        translateScanEdgesByEndpointLabelsLoop(config, loopBody, limit, body, &NLExecutor::runScanEdgesByEndpointLabelsFromSourceLoop);
    } else if (config._kind == IteratorKind::ScanEdgesByEndpointLabelsFromTarget) {
        translateScanEdgesByEndpointLabelsLoop(config, loopBody, limit, body, &NLExecutor::runScanEdgesByEndpointLabelsFromTargetLoop);
    } else if (config._kind == IteratorKind::Sort) {
        translateSortLoop(config, loopBody, limit, body);
    } else if (config._kind == IteratorKind::GroupAggregate) {
        translateGroupAggregateLoop(config, loopBody, limit, body);
    } else if (config._kind == IteratorKind::UnwindCollect) {
        // A collect drain emit loop is never limit-bounded: every row must be folded
        // before the first element is emitted.
        translateUnwindCollectLoop(config, loopBody, body);
    } else if (config._kind == IteratorKind::Collect) {
        translateCollectLoop(config, loopBody, body);
    } else if (config._kind == IteratorKind::ShortestPath) {
        translateShortestPathLoop(config, loopBody, body);
    } else if (config._kind == IteratorKind::UnwindConst) {
        // A const unwind is a plain source, so - like a scan - a downstream LIMIT can
        // bound its loop through the ordinary early-exit.
        translateUnwindConstLoop(config, loopBody, limit, body);
    } else if (config._kind == IteratorKind::LoadCSV) {
        // A CSV load is a plain source too, so a downstream LIMIT can bound its loop
        // through the ordinary early-exit - the file is then read only as far as the
        // budget reaches.
        translateLoadCSVLoop(config, loopBody, limit, body);
    } else if (config._kind == IteratorKind::VectorSearch) {
        // A vector search is a plain source too, so a downstream LIMIT can bound its
        // loop through the ordinary early-exit.
        translateVectorSearchLoop(config, loopBody, limit, body);
    } else if (config._kind == IteratorKind::Unwind) {
        // An unwind expands the rows it is given rather than accumulating them, so - like
        // a hop - a downstream LIMIT can bound its loop through the ordinary early-exit.
        translateUnwindLoop(config, loopBody, limit, body);
    } else if (config._kind == IteratorKind::UnwindRange) {
        translateUnwindRangeLoop(config, loopBody, limit, body);
    } else if (config._kind == IteratorKind::UnwindComputed) {
        translateUnwindComputedLoop(config, loopBody, limit, body);
    } else if (config._kind == IteratorKind::OptionalDrain) {
        translateOptionalDrainLoop(config, loopBody, limit, body);
    } else if (config._kind == IteratorKind::RowDrain) {
        translateRowLoop(config, loopBody, limit, body);
    } else if (config._kind == IteratorKind::ProcedureInit) {
        translateProcedureInitLoop(config, loopBody, limit, body);
    } else if (config._kind == IteratorKind::CrossProduct) {
        // A product expands the rows it is given rather than accumulating them, so -
        // like a hop - a downstream LIMIT can bound its loop through the ordinary
        // early-exit, and a step lays out only the prefix the budget can emit.
        translateCrossProductLoop(config, loopBody, limit, body);
    } else if (config._kind == IteratorKind::HashJoinProbe) {
        // A probe expands the rows it is given the way a product does, so a downstream
        // LIMIT bounds its loop through the same early-exit.
        translateHashJoinProbeLoop(config, loopBody, limit, body);
    } else if (config._kind == IteratorKind::EachRow) {
        translateEachRowLoop(config, loopBody, limit, body);
    } else if (config._kind == IteratorKind::ExplorePaths) {
        translateExplorePathsLoop(config, loopBody, limit, body);
    } else {
        translateEdgeLoop(config, loopBody, limit, body);
    }
}

template <typename LoopData>
void NLTranslator::bindCarriedColumns(llvm::ArrayRef<mlir::Value> carriedColumns,
                                      mlir::Block& loopBody,
                                      size_t firstCarriedArgument,
                                      LoopData* loopData) {
    for (size_t carriedIndex = 0; carriedIndex < carriedColumns.size(); carriedIndex++) {
        const mlir::Value carriedValue = carriedColumns[carriedIndex];

        const unsigned argumentIndex = static_cast<unsigned>(firstCarriedArgument + carriedIndex);
        Column* carriedOutput = allocColumn(loopBody.getArgument(argumentIndex));

        const NLCarriedColumn carriedColumn(getColumn(carriedValue),
                                            carriedOutput,
                                            selectGatherForChunkType(carriedValue.getType()));
        loopData->addCarriedColumn(carriedColumn);
    }
}

void NLTranslator::throwIfCarriedNotRowAligned(const IteratorConfig& config) {
    for (const mlir::Value carriedValue : config._carriedColumns) {
        if (!rowAlignedWith(carriedValue, config._inputNodes)) {
            throw IRException("Carried column is not row-aligned with the input chunk");
        }
    }
}

void NLTranslator::translateUnwindLoop(const IteratorConfig& config,
                                       mlir::Block& loopBody,
                                       NLLimitState* limit,
                                       NLStmtContainer* body) {
    const mlir::Value sourceValue = config._source;
    const Column* source = getColumn(sourceValue);

    const nl::ChunkType sourceChunk = mlir::cast<nl::ChunkType>(sourceValue.getType());
    const mlir::Type sourceElement = sourceChunk.getElementType();

    // An unwind binds the elements first, then one variable per carried column.
    const mlir::Value elementValue = loopBody.getArgument(0);

    NLUnwindElementCountFunction elementCount = nullptr;
    NLUnwindElementEmitFunction elementEmit = nullptr;
    selectElementDrain(sourceElement, elementValue.getType(), elementCount, elementEmit);

    const bool readsTheElements = !elementValue.use_empty();
    if (!readsTheElements) {
        elementEmit = nullptr;
    }

    Column* const elementOutput = elementEmit ? allocColumn(elementValue) : nullptr;

    NLUnwindLoopData* loopData = _program->allocFunctionData<NLUnwindLoopData>(source,
                                                                               elementCount,
                                                                               elementEmit,
                                                                               elementOutput);
    loopData->setLimit(limit);

    const size_t chunkSize = _program->getChunkSize();
    loopData->getRows()->reserve(chunkSize);
    loopData->getPositions()->reserve(chunkSize);

    if (readsTheElements && !elementEmit) {
        const NLCarriedColumn elementColumn(source,
                                            allocColumn(elementValue),
                                            selectGatherForChunkType(sourceChunk));
        loopData->addCarriedColumn(elementColumn);
    }

    bindCarriedColumns(config._carriedColumns, loopBody, 1, loopData);

    body->emplaceStmt(&NLExecutor::runUnwindLoop, loopData);

    translateBlock(loopBody, loopData->getStmts());
}

void NLTranslator::translateUnwindComputedLoop(const IteratorConfig& config,
                                               mlir::Block& loopBody,
                                               NLLimitState* limit,
                                               NLStmtContainer* body) {
    const llvm::ArrayRef<mlir::Value> carriedValues = config._carriedColumns;
    NLUnwindComputedLoopData* loopData = _program->allocFunctionData<NLUnwindComputedLoopData>(getColumn(carriedValues.front()));
    loopData->setLimit(limit);

    const size_t chunkSize = _program->getChunkSize();
    loopData->getRunRows()->reserve(chunkSize);
    loopData->getRows()->reserve(chunkSize);
    loopData->getPositions()->reserve(chunkSize);

    mlir::Block& sourceBlock = config._unwindSource->front();
    for (size_t carriedIndex = 0; carriedIndex < carriedValues.size(); carriedIndex++) {
        const mlir::Value argument = sourceBlock.getArgument(static_cast<unsigned>(carriedIndex));
        if (argument.use_empty()) {
            continue;
        }

        const NLCarriedColumn sourceArgument(getColumn(carriedValues[carriedIndex]),
                                             allocColumn(argument),
                                             selectGatherForChunkType(argument.getType()));
        loopData->addSourceArgument(sourceArgument);
    }

    translateBlock(sourceBlock, loopData->getSourceStmts());

    // The op's verifier guarantees the terminator, so a region without one here means
    // unverified IR.
    nl::UnwindYield yield = mlir::dyn_cast<nl::UnwindYield>(sourceBlock.back());
    if (!yield) {
        throw IRException("The source region of an nl.unwind_computed does not end with an nl.unwind_yield");
    }

    const mlir::Value sourceValue = yield.getSource();
    const Column* source = getColumn(sourceValue);
    const nl::ChunkType sourceChunk = mlir::cast<nl::ChunkType>(sourceValue.getType());

    const mlir::Value elementValue = loopBody.getArgument(0);

    NLUnwindElementCountFunction elementCount = nullptr;
    NLUnwindElementEmitFunction elementEmit = nullptr;
    selectElementDrain(sourceChunk.getElementType(), elementValue.getType(), elementCount, elementEmit);

    const bool readsTheElements = !elementValue.use_empty();
    if (!readsTheElements) {
        elementEmit = nullptr;
    }

    Column* const elementOutput = elementEmit ? allocColumn(elementValue) : nullptr;

    if (readsTheElements && !elementEmit) {
        const NLCarriedColumn elementCell(source, allocColumn(elementValue), selectGatherForChunkType(sourceChunk));
        loopData->setSource(source, elementCount, elementEmit, elementOutput, &elementCell);
    } else {
        loopData->setSource(source, elementCount, elementEmit, elementOutput, nullptr);
    }

    bindCarriedColumns(carriedValues, loopBody, 1, loopData);

    body->emplaceStmt(&NLExecutor::runUnwindComputedLoop, loopData);

    translateBlock(loopBody, loopData->getStmts());
}

void NLTranslator::translateUnwindRangeLoop(const IteratorConfig& config,
                                            mlir::Block& loopBody,
                                            NLLimitState* limit,
                                            NLStmtContainer* body) {
    const llvm::SmallVector<mlir::Value, 5>& bounds = config._rangeBounds;

    const NLRangeWindowBounds window {
        ._from = rangeBound(bounds[3]),
        ._to = rangeBound(bounds[4]),
        ._reversed = config._rangeReversed,
    };

    NLUnwindRangeLoopData* loopData = _program->allocFunctionData<NLUnwindRangeLoopData>(rangeBound(bounds[0]),
                                                                                         rangeBound(bounds[1]),
                                                                                         rangeBound(bounds[2]),
                                                                                         window,
                                                                                         allocColumnIfUsed(loopBody.getArgument(0)));
    loopData->setLimit(limit);
    loopData->getRows()->reserve(_program->getChunkSize());

    bindCarriedColumns(config._carriedColumns, loopBody, 1, loopData);

    body->emplaceStmt(&NLExecutor::runUnwindRangeLoop, loopData);

    translateBlock(loopBody, loopData->getStmts());
}

void NLTranslator::translateEdgeLoop(const IteratorConfig& config,
                                     mlir::Block& loopBody,
                                     NLLimitState* limit,
                                     NLStmtContainer* body) {
    // The four fixed chunks of an edge iterator step, in the block-argument
    // order established by getEdgeIteratorType: sources, edge IDs, edge type
    // IDs, targets.
    ColumnNodeIDs* sources = static_cast<ColumnNodeIDs*>(allocColumnIfUsed(loopBody.getArgument(0)));
    ColumnEdgeIDs* edgeIDs = static_cast<ColumnEdgeIDs*>(allocColumnIfUsed(loopBody.getArgument(1)));
    ColumnEdgeTypes* edgeTypes = static_cast<ColumnEdgeTypes*>(allocColumnIfUsed(loopBody.getArgument(2)));
    ColumnNodeIDs* targets = static_cast<ColumnNodeIDs*>(allocColumnIfUsed(loopBody.getArgument(3)));

    // Allocate for edge IDs even if they aren't read if we have to check tombstones
    if (!edgeIDs && (_view->hasDeletedEdges() || _view->followsChangeDeletions())) {
        edgeIDs = _memory->alloc<ColumnEdgeIDs>();
        edgeIDs->reserve(_program->getChunkSize());
    }

    const ColumnNodeIDs* inputNodeIDs = static_cast<const ColumnNodeIDs*>(getColumn(config._inputNodes));

    const bool byType = config._kind == IteratorKind::GetOutEdgesByType
                        || config._kind == IteratorKind::GetInEdgesByType;

    const bool byLabel = config._kind == IteratorKind::GetOutEdgesByLabel
                         || config._kind == IteratorKind::GetInEdgesByLabel;

    const bool byTypeAndLabel = config._kind == IteratorKind::GetOutEdgesByTypeAndLabel
                                || config._kind == IteratorKind::GetInEdgesByTypeAndLabel;

    // A by-type hop carries the resolved edge type in an NLEdgeByTypeLoopData and a
    // by-label one its label set in an NLEdgeByLabelLoopData; a plain hop uses the
    // base NLEdgeLoopData. The shared driver below (reserve, carried columns, body)
    // works through the base pointer whichever it is.
    NLEdgeLoopData* loopData = nullptr;
    if (byType) {
        // Resolve the edge type names against the schema, exactly as
        // translateScanByLabelLoop resolves its labels.
        llvm::SmallVector<EdgeTypeID, 4> requestedTypes;
        resolveEdgeTypes(config._edgeTypes, requestedTypes);

        loopData = _program->allocFunctionData<NLEdgeByTypeLoopData>(inputNodeIDs,
                                                                     sources,
                                                                     edgeIDs,
                                                                     edgeTypes,
                                                                     targets,
                                                                     requestedTypes);
    } else if (byLabel) {
        // An unmatchable hop emits nothing rather than dropping the absent name and
        // matching a weaker set, exactly as translateScanByLabelLoop does.
        LabelSet labelset;
        const bool matchable = resolveLabelSet(config._labels, labelset);

        loopData = _program->allocFunctionData<NLEdgeByLabelLoopData>(inputNodeIDs,
                                                                      sources,
                                                                      edgeIDs,
                                                                      edgeTypes,
                                                                      targets,
                                                                      labelset,
                                                                      matchable);
    } else if (byTypeAndLabel) {
        llvm::SmallVector<EdgeTypeID, 4> requestedTypes;
        resolveEdgeTypes(config._edgeTypes, requestedTypes);

        LabelSet labelset;
        const bool labelsMatchable = resolveLabelSet(config._labels, labelset);

        loopData = _program->allocFunctionData<NLEdgeByTypeAndLabelLoopData>(inputNodeIDs,
                                                                             sources,
                                                                             edgeIDs,
                                                                             edgeTypes,
                                                                             targets,
                                                                             requestedTypes,
                                                                             labelset,
                                                                             labelsMatchable);
    } else {
        loopData = _program->allocFunctionData<NLEdgeLoopData>(inputNodeIDs,
                                                               sources,
                                                               edgeIDs,
                                                               edgeTypes,
                                                               targets);
    }
    loopData->setLimit(limit);

    // Reserve scratch indices column
    loopData->getIndices()->reserve(_program->getChunkSize());

    throwIfCarriedNotRowAligned(config);
    bindCarriedColumns(config._carriedColumns, loopBody, 4, loopData);

    NLHandlerFunction handler = nullptr;
    if (config._kind == IteratorKind::GetOutEdges) {
        handler = &NLExecutor::runGetOutEdgesLoop;
    } else if (config._kind == IteratorKind::GetInEdges) {
        handler = &NLExecutor::runGetInEdgesLoop;
    } else if (config._kind == IteratorKind::GetEdges) {
        handler = &NLExecutor::runGetEdgesLoop;
    } else if (config._kind == IteratorKind::GetOutEdgesByType) {
        handler = &NLExecutor::runGetOutEdgesByTypeLoop;
    } else if (config._kind == IteratorKind::GetInEdgesByType) {
        handler = &NLExecutor::runGetInEdgesByTypeLoop;
    } else if (config._kind == IteratorKind::GetOutEdgesByLabel) {
        handler = &NLExecutor::runGetOutEdgesByLabelLoop;
    } else if (config._kind == IteratorKind::GetInEdgesByLabel) {
        handler = &NLExecutor::runGetInEdgesByLabelLoop;
    } else if (config._kind == IteratorKind::GetOutEdgesByTypeAndLabel) {
        handler = &NLExecutor::runGetOutEdgesByTypeAndLabelLoop;
    } else {
        handler = &NLExecutor::runGetInEdgesByTypeAndLabelLoop;
    }
    body->emplaceStmt(handler, loopData);

    translateBlock(loopBody, loopData->getStmts());
}

void NLTranslator::translateExplorePathsLoop(const IteratorConfig& config,
                                             mlir::Block& loopBody,
                                             NLLimitState* limit,
                                             NLStmtContainer* body) {
    // The three fixed chunks of an exploration step, in the block-argument order
    // established by getPathIteratorType: seeds, ends, paths
    ColumnNodeIDs* sources = static_cast<ColumnNodeIDs*>(allocColumnIfUsed(loopBody.getArgument(0)));
    ColumnNodeIDs* targets = static_cast<ColumnNodeIDs*>(allocColumnIfUsed(loopBody.getArgument(1)));
    ColumnVector<PathRef>* paths = static_cast<ColumnVector<PathRef>*>(allocColumnIfUsed(loopBody.getArgument(2)));

    const ColumnNodeIDs* inputNodeIDs = static_cast<const ColumnNodeIDs*>(getColumn(config._inputNodes));

    NLExplorePathsLoopData* loopData = _program->allocFunctionData<NLExplorePathsLoopData>(inputNodeIDs,
                                                                                         sources,
                                                                                         targets,
                                                                                         paths,
                                                                                         &_memory->pathTrie(),
                                                                                         config._minHops,
                                                                                         config._maxHops);
    loopData->setLimit(limit);
    loopData->getIndices()->reserve(_program->getChunkSize());

    // The end labels resolve as a label scan's do: a name absent from the schema is carried
    // by no node, so no path can end on one and the loop is marked unmatchable
    if (!config._labels.empty()) {
        LabelSet endLabels;
        const bool endMatchable = resolveLabelSet(config._labels, endLabels);
        loopData->setEndLabels(endLabels, endMatchable);
    }

    throwIfCarriedNotRowAligned(config);
    bindCarriedColumns(config._carriedColumns, loopBody, 3, loopData);

    // The bound end is the carried column's input, row-aligned with the seeds; a walk
    // ending where it began targets the seeds themselves
    if (config._endColumn) {
        const mlir::Value endValue = config._carriedColumns[*config._endColumn];
        loopData->setEndNodes(static_cast<const ColumnNodeIDs*>(getColumn(endValue)));
    } else if (config._endsOnSeed) {
        loopData->setEndNodes(static_cast<const ColumnNodeIDs*>(getColumn(config._inputNodes)));
    } else if (config._endNodeSet) {
        loopData->setEndNodeSet(config._endNodeSet);
    }

    if (config._distinctEnds) {
        loopData->setDistinctEnds();
    }

    nl::ExplorePaths explorePaths = config._explorePaths;
    const size_t stepCount = explorePaths.getStepCount();
    for (size_t step = 0; step < stepCount; step++) {
        translateExploreStep(explorePaths, step, config._hopImports, loopData);
    }

    body->emplaceStmt(&NLExecutor::runExplorePathsLoop, loopData);

    translateBlock(loopBody, loopData->getStmts());
}

template <typename SetPropertiesData, typename SetPropertiesOp>
void NLTranslator::translateSetProperties(SetPropertiesOp setProperties, mlir::Value input, NLStmtContainer* body) {
    if (!_metadataBuilder) {
        throw IRException("Cannot perform SET outside of a write transaction.");
    }

    createMapKeyProperties(setProperties.getCreatedTypesAttr());

    using IDColumn = typename SetPropertiesData::IDColumnType;
    constexpr bool WRITES_NODES = std::is_same_v<IDColumn, ColumnNodeIDs>;
    const NLHandlerFunction run = WRITES_NODES ? &NLExecutor::runSetNodeProperties : &NLExecutor::runSetEdgeProperties;

    const IDColumn* inputColumn = static_cast<const IDColumn*>(getColumn(input));
    const mlir::Value value = setProperties.getValue();

    SetPropertiesData* data = _program->allocFunctionData<SetPropertiesData>(inputColumn,
                                                                             getColumn(value),
                                                                             setProperties.getReplaces());

    data->setWritesNull(isUntypedNullChunkType(value.getType()));
    data->setPending(getMaskColumn(setProperties.getPending()));
    data->setAllPending(setProperties.getAllPending() || isPendingValue(input, WRITES_NODES));
    data->setRows(getMaskColumn(setProperties.getRows()));

    body->emplaceStmt(run, data);
}

template <ColumnOperator Op, typename OpType>
void NLTranslator::translateBinaryOp(OpType op, NLStmtContainer* body) {
    const Column* lhs = getColumn(op.getLhs());
    const Column* rhs = getColumn(op.getRhs());

    Column* result = nullptr;
    const NLBinaryFn fn = NLExecutor::selectBinary<Op>(lhs, rhs, _memory, result);
    bindKernelResult(op.getResult(), result);

    NLBinaryData* data = _program->allocFunctionData<NLBinaryData>(lhs, rhs, result, fn, _program->allocStepBuffers());
    body->emplaceStmt(&NLExecutor::runBinary, data);
}

template <typename Function, typename StringEndOp>
void NLTranslator::translateStringEnd(StringEndOp stringEnd, NLStmtContainer* body) {
    const mlir::Value resultValue = stringEnd.getResult();
    const mlir::Type resultType = resultValue.getType();

    Column* const result = allocColumnForChunkType(resultType);
    _valueSlots[resultValue] = result;

    const Column* const string = getColumn(stringEnd.getString());
    const Column* const length = getColumn(stringEnd.getLength());

    const NLStringEndData::StringArgument stringArgument {string, NLExecutor::selectStringArgumentRead(string)};
    const NLStringEndData::IntegerArgument lengthArgument {length, NLExecutor::selectIntegerArgumentRead(length)};

    NLStringEndData* data = _program->allocFunctionData<NLStringEndData>(result, stringArgument, lengthArgument);

    if (isNullableChunkType(resultType)) {
        body->emplaceStmt(&NLExecutor::runStringEnd<Function, std::optional<types::String::Primitive>>, data);
    } else {
        body->emplaceStmt(&NLExecutor::runStringEnd<Function, types::String::Primitive>, data);
    }
}

template <typename CaseOp>
void NLTranslator::translateCase(CaseOp caseOp, NLHandlerFunction run, NLStmtContainer* body) {
    const mlir::Value resultValue = caseOp.getResult();
    const mlir::Type resultChunkType = resultValue.getType();

    Column* const result = allocColumnForChunkType(resultChunkType);
    _valueSlots[resultValue] = result;

    const mlir::OperandRange conditions = caseOp.getConditions();
    const mlir::OperandRange values = caseOp.getValues();

    // The first condition holds one row per row this step writes
    NLCaseData* data = _program->allocFunctionData<NLCaseData>(getColumn(conditions.front()),
                                                               result,
                                                               selectCaseResetForChunkType(resultChunkType),
                                                               selectCaseFinishForChunkType(resultChunkType),
                                                               _program->allocStepBuffers());

    for (size_t branchIndex = 0; branchIndex < conditions.size(); branchIndex++) {
        const mlir::Value condition = conditions[branchIndex];
        const mlir::Type valueChunkType = values[branchIndex].getType();

        NLCaseData::Branch branch;
        branch._condition = getColumn(condition);
        branch._value = getColumn(values[branchIndex]);
        branch._test = NLExecutor::selectCaseTest(truthColumnKindOf(condition.getType(), branch._condition));
        branch._write = selectCellWriteForChunkType(resultChunkType, branch._value, valueChunkType);

        data->addBranch(branch);
    }

    const mlir::Value defaultValue = caseOp.getDefaultValue();
    if (defaultValue) {
        const mlir::Type defaultType = defaultValue.getType();
        const Column* const defaultColumn = getColumn(defaultValue);

        data->setDefault(defaultColumn,
                         selectCellWriteForChunkType(resultChunkType, defaultColumn, defaultType));
    }

    body->emplaceStmt(run, data);
}

template <typename NullTestOp>
void NLTranslator::translateNullTest(NullTestOp nullTest, NLStmtContainer* body) {
    constexpr bool TESTS_NULL = std::is_same_v<NullTestOp, nl::IsNull>;
    const Column* operand = getColumn(nullTest.getOperand());

    Column* result = nullptr;
    const NLUnaryFn fn = NLExecutor::selectNullTest<TESTS_NULL>(operand, _memory, result);
    bindKernelResult(nullTest.getResult(), result);

    NLUnaryData* data = _program->allocFunctionData<NLUnaryData>(operand, result, fn);
    body->emplaceStmt(&NLExecutor::runUnary, data);
}
