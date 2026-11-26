#include "NLTranslator.h"

#include <optional>
#include <vector>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>

#include <spdlog/fmt/bundled/format.h>

#include "NLOps.h"
#include "ParameterMap.h"
#include "StorageEnums.h"

#include "IRRowAlignment.h"
#include "IRValueTypes.h"
#include "mlir/IR/Block.h"
#include "mlir/IR/BuiltinAttributes.h"
#include "mlir/IR/Verifier.h"
#include "llvm/ADT/DenseMap.h"
#include "llvm/ADT/SmallVector.h"

#include "ProcedureContext.h"
#include "columns/Functions.h"
#include "list/ListView.h"
#include "metadata/GraphMetadata.h"
#include "metadata/LabelSet.h"
#include "metadata/LabelSetHandle.h"
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
#include "BioAssert.h"

using namespace db;

namespace nl = mlir::nl;
namespace storage = mlir::storage;

namespace {

// Whether the grouped reduction charges each of a group's values once, through a tally
bool chargesDistinctValues(GroupAggregateKind kind) {
    switch (kind) {
        case GroupAggregateKind::CountDistinct:
        case GroupAggregateKind::SumDistinct:
        case GroupAggregateKind::AvgDistinct:
        case GroupAggregateKind::StDevDistinct:
        case GroupAggregateKind::StDevPDistinct:
            return true;
        break;

        case GroupAggregateKind::Count:
        case GroupAggregateKind::Sum:
        case GroupAggregateKind::Min:
        case GroupAggregateKind::Max:
        case GroupAggregateKind::Avg:
        case GroupAggregateKind::CountRows:
        case GroupAggregateKind::StDev:
        case GroupAggregateKind::StDevP:
        case GroupAggregateKind::First:
            return false;
        break;
    }

    throw IRException("Unhandled group aggregate kind");
}

// The runtime grouped reduction the interpreter dispatches on, from the MLIR one
// the op carries. Like toRuntimeAggregateKind, this is the one place the two enums
// meet, keeping the runtime free of the MLIR dialect headers.
GroupAggregateKind toRuntimeGroupAggregateKind(storage::GroupAggregateKind kind) {
    switch (kind) {
        case storage::GroupAggregateKind::Count:
            return GroupAggregateKind::Count;
        break;

        case storage::GroupAggregateKind::Sum:
            return GroupAggregateKind::Sum;
        break;

        case storage::GroupAggregateKind::Min:
            return GroupAggregateKind::Min;
        break;

        case storage::GroupAggregateKind::Max:
            return GroupAggregateKind::Max;
        break;

        case storage::GroupAggregateKind::Avg:
            return GroupAggregateKind::Avg;
        break;

        case storage::GroupAggregateKind::CountDistinct:
            return GroupAggregateKind::CountDistinct;
        break;

        case storage::GroupAggregateKind::SumDistinct:
            return GroupAggregateKind::SumDistinct;
        break;

        case storage::GroupAggregateKind::AvgDistinct:
            return GroupAggregateKind::AvgDistinct;
        break;

        case storage::GroupAggregateKind::CountRows:
            return GroupAggregateKind::CountRows;
        break;

        case storage::GroupAggregateKind::StDev:
            return GroupAggregateKind::StDev;
        break;

        case storage::GroupAggregateKind::StDevP:
            return GroupAggregateKind::StDevP;
        break;

        case storage::GroupAggregateKind::StDevDistinct:
            return GroupAggregateKind::StDevDistinct;
        break;

        case storage::GroupAggregateKind::StDevPDistinct:
            return GroupAggregateKind::StDevPDistinct;
        break;

        case storage::GroupAggregateKind::First:
            return GroupAggregateKind::First;
        break;
    }

    throw IRException("Unhandled group aggregate kind");
}

NLMergeDirection mergeDirectionFromValue(int64_t raw) {
    const std::optional<storage::EdgeDirection> direction =
        storage::symbolizeEdgeDirection(static_cast<uint64_t>(raw));

    if (!direction) {
        throw IRException("nl.merge carries an unknown edge direction");
    }

    switch (*direction) {
        case storage::EdgeDirection::Undirected:
            return NLMergeDirection::Undirected;
        break;

        case storage::EdgeDirection::Backward:
            return NLMergeDirection::Backward;
        break;

        case storage::EdgeDirection::Forward:
            return NLMergeDirection::Forward;
        break;
    }

    bioassert(false, "Unhandled edge direction");
    return NLMergeDirection::Forward;
}

// A property written from tagged cells that has no type yet is told apart by its name,
// behind the invalid ID no typed property carries
void appendMergePropertySignature(const std::vector<NLMergeProperty>& properties,
                                  std::string& signature) {
    for (const NLMergeProperty& property : properties) {
        const PropertyTypeID::Type id = property._propertyType._id.getValue();
        const ValueType valueType = property._propertyType._valueType;

        signature.append(reinterpret_cast<const char*>(&id), sizeof(id));
        signature.append(reinterpret_cast<const char*>(&valueType), sizeof(valueType));

        if (!property._propertyType.isValid()) {
            const std::string_view name = property._runtimeProperty->getName();
            const size_t length = name.size();

            signature.append(reinterpret_cast<const char*>(&length), sizeof(length));
            signature.append(name);
        }
    }
}

// What the pending log's keys for one chain-node spec start with: its label set and the
// property it keys on, in order. Two specs writing the same pattern share the prefix, so
// each binds what the other wrote; two that differ never collide.
void appendMergeNodeSignature(const LabelSet& labels,
                              const std::vector<NLMergeProperty>& properties,
                              std::string& signature) {
    signature.clear();

    for (const LabelSet::IntegerType integer : labels.integers()) {
        signature.append(reinterpret_cast<const char*>(&integer), sizeof(integer));
    }

    appendMergePropertySignature(properties, signature);
}

// A drain wires its emit loop's variables into the accumulator's one output slot per
// column, so an accumulator can serve exactly one nl.collect or nl.unwind_collect. A
// second drain would rebind the first's outputs to its own, incompatible column types
// (a per-group list cell against an unwound value), leaving the first to emit through
// the other's columns. Generated IR names each accumulator from a single drain, so a
// second one means malformed IR - the accumulate side rejects a second
// nl.collect_update the same way.
void throwIfAlreadyDrained(const NLCollectState* state) {
    const std::vector<NLCollectState::ValueColumn>& valueColumns = state->valueColumns();
    const bool isDrained = !valueColumns.empty() && valueColumns.front()._output;

    if (isDrained) {
        throw IRException("an nl.collect_buffer must be drained by a single nl.collect or nl.unwind_collect");
    }
}

}

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

bool NLTranslator::isPendingValue(mlir::Value value, bool isNode) const {
    const llvm::DenseSet<mlir::Value>& pendingValues = isNode ? _pendingNodeValues : _pendingEdgeValues;

    return pendingValues.contains(value);
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

void NLTranslator::translateCreateNode(nl::CreateNode createNode, NLStmtContainer* body) {
    if (!_metadataBuilder) {
        throw IRException("Cannot perform CREATE outside of a write transaction.");
    }

    llvm::SmallVector<llvm::StringRef, 4> names;
    resolveLabelNames(createNode.getLabels(), names);

    LabelSet labelset;
    for (const llvm::StringRef labelName : names) {
        const LabelID labelID = _metadataBuilder->getOrCreateLabel(labelName);
        labelset.set(labelID);
    }
    bioassert(not labelset.empty(), "Node must have at least one label.");

    const LabelSetHandle labelsetHandle = _metadataBuilder->getOrCreateLabelSet(labelset);

    ColumnNodeIDs* result = _memory->alloc<ColumnNodeIDs>();
    _valueSlots[createNode.getResult()] = result;
    _pendingNodeValues.insert(createNode.getResult());

    NLCreateNodeData* data = _program->allocFunctionData<NLCreateNodeData>(labelsetHandle, result);

    const mlir::OperandRange propValues = createNode.getPropValues();
    const mlir::ArrayAttr propNames = createNode.getPropNames();

    for (size_t propIndex = 0; propIndex < propNames.size(); propIndex++) {
        const llvm::StringRef propName = mlir::cast<mlir::StringAttr>(propNames[propIndex]).getValue();

        NLCreatedProperty property;
        translateCreatedProperty(propName, propValues[propIndex], property);
        data->addProperty(property);
    }

    if (const mlir::Value cardinality = createNode.getCardinality()) {
        data->setCardinality(getColumn(cardinality));
    }

    body->emplaceStmt(&NLExecutor::runCreateNode, data);
}

void NLTranslator::translateCreateEdge(nl::CreateEdge createEdge, NLStmtContainer* body) {
    if (!_metadataBuilder) {
        throw IRException("Cannot perform CREATE outside of a write transaction.");
    }

    const llvm::StringRef edgeTypeName = resolveEdgeTypeName(createEdge.getEdgeType());
    const EdgeTypeID edgeTypeID = _metadataBuilder->getOrCreateEdgeType(edgeTypeName);

    const mlir::Value srcValue = createEdge.getSrcIds();
    const mlir::Value tgtValue = createEdge.getTgtIds();
    const bool srcIsPending = createEdge.getSrcAllPending() || isPendingValue(srcValue, /*isNode=*/true);
    const bool tgtIsPending = createEdge.getTgtAllPending() || isPendingValue(tgtValue, /*isNode=*/true);

    const ColumnNodeIDs* srcColumn = static_cast<const ColumnNodeIDs*>(getColumn(srcValue));
    const ColumnNodeIDs* tgtColumn = static_cast<const ColumnNodeIDs*>(getColumn(tgtValue));

    ColumnEdgeIDs* result = _memory->alloc<ColumnEdgeIDs>();
    _valueSlots[createEdge.getResult()] = result;
    _pendingEdgeValues.insert(createEdge.getResult());

    NLCreateEdgeData* data = _program->allocFunctionData<NLCreateEdgeData>(
        edgeTypeID,
        srcColumn,
        srcIsPending,
        tgtColumn,
        tgtIsPending,
        result);

    data->setSrcPendingMask(getMaskColumn(createEdge.getSrcPending()));
    data->setTgtPendingMask(getMaskColumn(createEdge.getTgtPending()));

    const mlir::OperandRange propValues = createEdge.getPropValues();
    const mlir::ArrayAttr propNames = createEdge.getPropNames();

    for (size_t propIndex = 0; propIndex < propNames.size(); propIndex++) {
        const llvm::StringRef propName = mlir::cast<mlir::StringAttr>(propNames[propIndex]).getValue();

        NLCreatedProperty property;
        translateCreatedProperty(propName, propValues[propIndex], property);
        data->addProperty(property);
    }

    body->emplaceStmt(&NLExecutor::runCreateEdge, data);
}

void NLTranslator::translateCreatedProperty(llvm::StringRef propName,
                                            mlir::Value propValue,
                                            NLCreatedProperty& property) {
    const mlir::Type valueChunkType = propValue.getType();
    property._values = getColumn(propValue);

    if (isAnyChunk(valueChunkType)) {
        property._runtimeProperty = _program->getRuntimeProperty(propName);
    } else {
        const ValueType valueType = valueTypeFromChunkType(valueChunkType);
        property._propertyTypeID = _metadataBuilder->getOrCreatePropertyType(propName, valueType)._id;
    }
}

void NLTranslator::translateMerge(nl::Merge merge, NLStmtContainer* body) {
    if (!_metadataBuilder) {
        throw IRException("Cannot perform MERGE outside of a write transaction.");
    }

    const mlir::ArrayAttr nodeLabels = merge.getNodeLabels();
    const mlir::ArrayAttr nodePropNames = merge.getNodePropNames();
    const mlir::ArrayAttr edgeTypes = merge.getEdgeTypes();
    const mlir::ArrayAttr edgePropNames = merge.getEdgePropNames();
    const llvm::ArrayRef<int64_t> directions = merge.getEdgeDirections();

    const mlir::OperandRange boundNodes = merge.getBoundNodes();
    const mlir::OperandRange boundPending = merge.getBoundPending();
    const mlir::OperandRange nodePropValues = merge.getNodePropValues();
    const mlir::OperandRange edgePropValues = merge.getEdgePropValues();
    const mlir::OperandRange carriedColumns = merge.getCarriedColumns();
    const mlir::ResultRange results = merge.getResults();

    ColumnVector<CustomBool>* created = _memory->alloc<ColumnVector<CustomBool>>();
    created->reserve(_program->getChunkSize());

    NLMergeData* data = _program->allocFunctionData<NLMergeData>(_program->getMergePendingEdges(), created, _memory);

    const size_t nodeCount = nodeLabels.size();
    const size_t hopCount = nodeCount - 1;

    // A bound node has no result pair of its own: its rows come back through the carry set
    size_t resultIndex = 0;
    size_t boundIndex = 0;
    size_t nodeValueIndex = 0;
    for (size_t nodeIndex = 0; nodeIndex < nodeCount; nodeIndex++) {
        const mlir::ArrayAttr labels = mlir::cast<mlir::ArrayAttr>(nodeLabels[nodeIndex]);
        const mlir::ArrayAttr propNames = mlir::cast<mlir::ArrayAttr>(nodePropNames[nodeIndex]);

        NLMergeData::Node node;

        if (labels.empty()) {
            node._boundColumn = static_cast<const ColumnNodeIDs*>(getColumn(boundNodes[boundIndex]));
            boundIndex++;
        } else {
            translateMergeNodeSpec(labels, propNames, nodePropValues.slice(nodeValueIndex, propNames.size()), node);

            node._output = static_cast<ColumnNodeIDs*>(allocColumn(results[resultIndex]));
            node._outputPending = static_cast<ColumnVector<CustomBool>*>(allocColumn(results[resultIndex + 1]));
            resultIndex += 2;
        }

        nodeValueIndex += propNames.size();

        data->addNode(node);
    }

    // Only a bound node whose column a merge produced carries a mask, so each one names
    // the chain node it belongs to rather than leaving a hole in a per-node group
    const llvm::ArrayRef<int64_t> pendingNodes = merge.getPendingNodes();
    std::vector<NLMergeData::Node>& nodes = data->nodes();
    for (size_t index = 0; index < pendingNodes.size(); index++) {
        nodes[pendingNodes[index]]._boundPending = getMaskColumn(boundPending[index]);
    }

    size_t edgeValueIndex = 0;
    for (size_t hopIndex = 0; hopIndex < hopCount; hopIndex++) {
        const llvm::StringRef edgeTypeName = resolveEdgeTypeName(edgeTypes[hopIndex]);
        const mlir::ArrayAttr propNames = mlir::cast<mlir::ArrayAttr>(edgePropNames[hopIndex]);

        NLMergeData::Hop hop;
        hop._writeEdgeType = _metadataBuilder->getOrCreateEdgeType(edgeTypeName);
        hop._direction = mergeDirectionFromValue(directions[hopIndex]);

        // A type the graph's schema does not have is on no committed edge, so the match
        // reads the graph only when the name resolves there. It is created above all the
        // same, since a written hop needs it.
        const std::optional<EdgeTypeID> matchEdgeType = _view->metadata().edgeTypes().get(edgeTypeName);
        if (matchEdgeType) {
            hop._matchEdgeType = *matchEdgeType;
        }

        bool matchable = matchEdgeType.has_value();
        for (size_t propIndex = 0; propIndex < propNames.size(); propIndex++) {
            const llvm::StringRef propName = mlir::cast<mlir::StringAttr>(propNames[propIndex]).getValue();
            const mlir::Value propValue = edgePropValues[edgeValueIndex + propIndex];

            translateMergeProperty(propName, propValue, hop._properties, hop._scanProperties, matchable);
        }

        // A property the graph does not have is on no committed edge either, so the hop
        // reads no candidate rather than reading one it cannot key
        if (!matchable) {
            hop._matchEdgeType = EdgeTypeID();
            hop._scanProperties.clear();
        }

        edgeValueIndex += propNames.size();

        collectWrittenMergeProperties(hop._properties, hop._writtenProperties);

        hop._scanSources = _memory->alloc<ColumnNodeIDs>();
        hop._scanEdges = _memory->alloc<ColumnEdgeIDs>();

        hop._output = static_cast<ColumnEdgeIDs*>(allocColumn(results[resultIndex]));
        hop._outputPending = static_cast<ColumnVector<CustomBool>*>(allocColumn(results[resultIndex + 1]));
        resultIndex += 2;

        data->addHop(hop);
    }

    _valueSlots[results[resultIndex]] = created;
    resultIndex++;

    for (size_t index = 0; index < carriedColumns.size(); index++) {
        const mlir::Value input = carriedColumns[index];

        data->addCarriedColumn({getColumn(input),
                                allocColumn(results[resultIndex + index]),
                                selectGatherForChunkType(input.getType())});
    }

    // Every row the op runs over is a row of its inputs, so any of them holding rows of
    // its own gives their count. A pattern reading only constants runs over one row.
    for (const mlir::Value input : merge->getOperands()) {
        const Column* column = getColumn(input);
        if (!isConstantColumn(column)) {
            data->setRowCarrier(column);
            break;
        }
    }

    body->emplaceStmt(&NLExecutor::runMerge, data);
}

void NLTranslator::translateMergeNodeSpec(mlir::ArrayAttr labels,
                                          mlir::ArrayAttr propNames,
                                          mlir::OperandRange propValues,
                                          NLMergeData::Node& node) {
    // The labels the node is written with, created where the schema lacks them, and the
    // same set resolved against the graph for the match. A name the graph does not have
    // is on no committed node, so the conjunction matches nothing there - the way
    // nl.scan_nodes_by_label treats it - and only what this query wrote can match.
    LabelSet writeLabels;
    LabelSet matchLabels;
    bool matchable = true;

    llvm::SmallVector<llvm::StringRef, 4> names;
    resolveLabelNames(labels, names);

    const LabelMap& graphLabels = _view->metadata().labels();
    for (const llvm::StringRef labelName : names) {
        writeLabels.set(_metadataBuilder->getOrCreateLabel(labelName));

        const std::optional<LabelID> graphLabel = graphLabels.get(labelName);
        if (graphLabel) {
            matchLabels.set(*graphLabel);
        } else {
            matchable = false;
        }
    }

    node._labelSetHandle = _metadataBuilder->getOrCreateLabelSet(writeLabels);

    std::vector<NLMergeScanProperty> scanProperties;
    for (size_t propIndex = 0; propIndex < propNames.size(); propIndex++) {
        const llvm::StringRef propName = mlir::cast<mlir::StringAttr>(propNames[propIndex]).getValue();

        translateMergeProperty(propName, propValues[propIndex], node._properties, scanProperties, matchable);
    }

    if (!matchable) {
        scanProperties.clear();
    }

    std::string signature;
    appendMergeNodeSignature(writeLabels, node._properties, signature);

    // Every chain node of the same signature looks its candidates up in one index, so
    // the label set is scanned once however many times the query merges the pattern
    node._index = _program->findMergeNodeIndex(signature);
    if (node._index) {
        return;
    }

    ColumnNodeIDs* scanNodes = _memory->alloc<ColumnNodeIDs>();
    node._index = _program->addMergeNodeIndex(signature, matchLabels, writeLabels, matchable, scanNodes);
    for (const NLMergeScanProperty& scanProperty : scanProperties) {
        node._index->addScanProperty(scanProperty);
    }

    NLMergeScanProperties writtenProperties;
    collectWrittenMergeProperties(node._properties, writtenProperties);
    for (const NLMergeScanProperty& writtenProperty : writtenProperties) {
        node._index->addWrittenProperty(writtenProperty);
    }
}

void NLTranslator::collectWrittenMergeProperties(const std::vector<NLMergeProperty>& properties,
                                                 NLMergeScanProperties& writtenProperties) {
    for (const NLMergeProperty& property : properties) {
        const ValueType valueType = property._propertyType._valueType;

        if (!property._propertyType.isValid()) {
            writtenProperties.emplace_back();
        } else {
            writtenProperties.push_back({._propertyType=property._propertyType,
                                         ._values=allocOptColumnForValueType(valueType),
                                         ._keyAppend=NLExecutor::selectOptKeyAppendFunction(chunkKindFromValueType(valueType))});
        }
    }
}

void NLTranslator::translateMergeProperty(llvm::StringRef propName,
                                          mlir::Value propValue,
                                          std::vector<NLMergeProperty>& properties,
                                          std::vector<NLMergeScanProperty>& scanProperties,
                                          bool& matchable) {
    const Column* column = getColumn(propValue);
    const mlir::Type valueChunkType = propValue.getType();

    if (isAnyChunk(valueChunkType)) {
        // A property the change knows is typed here, so the merge keys it as a typed merge
        // of it does and shares that merge's index
        NLRuntimeProperty* runtimeProperty = _program->getRuntimeProperty(propName);
        const bool typed = runtimeProperty->findType(_metadataBuilder);

        properties.push_back({._propertyType=typed ? runtimeProperty->getType() : PropertyType {},
                              ._values=column,
                              ._runtimeProperty=runtimeProperty});
    } else {
        // Created where the schema lacks it, since a written pattern carries the property.
        // Both sides of the key serialize in its registered type: the analyzer lets an
        // integer constrain a double-typed property, and 5 keys like the 5.0 the graph side
        // reads back only once converted to it.
        const ValueType valueType = valueTypeFromChunkType(valueChunkType);
        const PropertyType writeType = _metadataBuilder->getOrCreatePropertyType(propName, valueType);
        properties.push_back({._propertyType=writeType,
                              ._values=column,
                              ._keyAppend=selectMergeKeyAppend(valueChunkType,
                                                               column,
                                                               writeType._valueType)});
    }

    // The graph's own property, which is what a candidate's value is read back through.
    // Absent means no committed entity carries it, so nothing there can match.
    const std::optional<PropertyType> graphType = _view->metadata().propTypes().get(propName);
    if (!graphType) {
        matchable = false;
        return;
    }

    scanProperties.push_back({._propertyType=*graphType,
                              ._values=allocOptColumnForValueType(graphType->_valueType),
                              ._keyAppend=NLExecutor::selectOptKeyAppendFunction(chunkKindFromValueType(graphType->_valueType))});
}

PropertyType NLTranslator::setPropertyType(llvm::StringRef propName, mlir::Type valueChunkType) const {
    const bool typedAsItRuns = isUntypedNullChunkType(valueChunkType) || isAnyChunk(valueChunkType);
    if (typedAsItRuns) {
        return PropertyType {};
    }

    const ValueType valueType = valueTypeFromChunkType(valueChunkType);

    return _metadataBuilder->getOrCreatePropertyType(propName, valueType);
}

void NLTranslator::translateSetNodeProperty(nl::SetNodeProperty setNodeProperty, NLStmtContainer* body) {
    if (!_metadataBuilder) {
        throw IRException("Cannot perform SET or REMOVE outside of a write transaction.");
    }

    const llvm::StringRef propName = setNodeProperty.getProperty();
    const mlir::Value inputValue = setNodeProperty.getInputNodes();
    const mlir::Value propValue = setNodeProperty.getValue();
    const mlir::Type valueChunkType = propValue.getType();
    const PropertyType propType = setPropertyType(propName, valueChunkType);

    const ColumnNodeIDs* inputColumn = static_cast<const ColumnNodeIDs*>(getColumn(inputValue));
    const Column* valueColumn = getColumn(propValue);

    NLSetNodePropertyData* data = _program->allocFunctionData<NLSetNodePropertyData>(
        propType._id,
        inputColumn,
        valueColumn);

    if (!propType.isValid()) {
        const bool writesNull = isUntypedNullChunkType(valueChunkType);

        data->setRuntimeProperty(_program->getRuntimeProperty(propName));
        data->setWritesNull(writesNull);

        if (!writesNull) {
            ColumnVector<CustomBool>* writtenRows = _memory->alloc<ColumnVector<CustomBool>>();
            writtenRows->reserve(_program->getChunkSize());
            data->setWrittenRows(writtenRows);
        }
    }

    data->setPending(getMaskColumn(setNodeProperty.getPending()));
    data->setAllPending(setNodeProperty.getAllPending() || isPendingValue(inputValue, /*isNode=*/true));
    data->setRows(getMaskColumn(setNodeProperty.getRows()));

    body->emplaceStmt(&NLExecutor::runSetNodeProperty, data);
}

void NLTranslator::translateSetEdgeProperty(nl::SetEdgeProperty setEdgeProperty, NLStmtContainer* body) {
    if (!_metadataBuilder) {
        throw IRException("Cannot perform SET outside of a write transaction.");
    }

    const llvm::StringRef propName = setEdgeProperty.getProperty();
    const mlir::Value inputValue = setEdgeProperty.getInputEdges();
    const mlir::Value propValue = setEdgeProperty.getValue();
    const mlir::Type valueChunkType = propValue.getType();
    const PropertyType propType = setPropertyType(propName, valueChunkType);

    const ColumnEdgeIDs* inputColumn = static_cast<const ColumnEdgeIDs*>(getColumn(inputValue));
    const Column* valueColumn = getColumn(propValue);

    NLSetEdgePropertyData* data = _program->allocFunctionData<NLSetEdgePropertyData>(
        propType._id,
        inputColumn,
        valueColumn);

    if (!propType.isValid()) {
        const bool writesNull = isUntypedNullChunkType(valueChunkType);

        data->setRuntimeProperty(_program->getRuntimeProperty(propName));
        data->setWritesNull(writesNull);

        if (!writesNull) {
            ColumnVector<CustomBool>* writtenRows = _memory->alloc<ColumnVector<CustomBool>>();
            writtenRows->reserve(_program->getChunkSize());
            data->setWrittenRows(writtenRows);
        }
    }

    data->setPending(getMaskColumn(setEdgeProperty.getPending()));
    data->setAllPending(setEdgeProperty.getAllPending() || isPendingValue(inputValue, /*isNode=*/false));
    data->setRows(getMaskColumn(setEdgeProperty.getRows()));

    body->emplaceStmt(&NLExecutor::runSetEdgeProperty, data);
}

void NLTranslator::createMapKeyProperties(mlir::DictionaryAttr createdTypes) const {
    if (!createdTypes) {
        return;
    }

    for (const mlir::NamedAttribute created : createdTypes) {
        const mlir::StringAttr name = created.getName();
        const llvm::StringRef key = name.getValue();
        if (findPropertyType(key)) {
            continue;
        }

        const mlir::TypeAttr typeAttr = mlir::cast<mlir::TypeAttr>(created.getValue());
        const mlir::Type elementType = typeAttr.getValue();
        _metadataBuilder->getOrCreatePropertyType(std::string_view {key.data(), key.size()},
                                                  elementTypeToValueType(elementType));
    }
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

void NLTranslator::translateDeleteNode(nl::DeleteNode deleteNode, NLStmtContainer* body) {
    if (!_metadataBuilder) {
        throw IRException("Cannot perform DELETE outside of a write transaction.");
    }

    const mlir::Value inputValue = deleteNode.getInputNodes();
    const ColumnNodeIDs* inputColumn = static_cast<const ColumnNodeIDs*>(getColumn(inputValue));

    NLDeleteNodeData* data = _program->allocFunctionData<NLDeleteNodeData>(
        inputColumn,
        deleteNode.getDetach(),
        _memory->alloc<ColumnNodeIDs>());

    data->setPending(getMaskColumn(deleteNode.getPending()));
    data->setAllPending(deleteNode.getAllPending() || isPendingValue(inputValue, /*isNode=*/true));

    body->emplaceStmt(&NLExecutor::runDeleteNode, data);
}

void NLTranslator::translateDeleteEdge(nl::DeleteEdge deleteEdge, NLStmtContainer* body) {
    if (!_metadataBuilder) {
        throw IRException("Cannot perform DELETE outside of a write transaction.");
    }

    const mlir::Value inputValue = deleteEdge.getInputEdges();
    const ColumnEdgeIDs* inputColumn = static_cast<const ColumnEdgeIDs*>(getColumn(inputValue));

    NLDeleteEdgeData* data = _program->allocFunctionData<NLDeleteEdgeData>(
        inputColumn,
        _memory->alloc<ColumnEdgeIDs>());

    data->setPending(getMaskColumn(deleteEdge.getPending()));
    data->setAllPending(deleteEdge.getAllPending() || isPendingValue(inputValue, /*isNode=*/false));

    body->emplaceStmt(&NLExecutor::runDeleteEdge, data);
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

void NLTranslator::translateOptionalBuffer(nl::OptionalBuffer buffer, NLStmtContainer* body) {
    NLOptionalState* state = _program->allocOptionalState();
    _optionalStates[buffer.getState()] = state;

    // This step's own chunks: the drain rebuilds the rows the pattern missed out of them,
    // and their row count is how many matched flags the reset clears.
    for (const mlir::Value column : buffer.getInputColumns()) {
        state->addInputColumn(getColumn(column));
    }

    ColumnVector<uint64_t>* tag = static_cast<ColumnVector<uint64_t>*>(allocColumn(buffer.getTag()));

    NLOptionalResetData* resetData = _program->allocFunctionData<NLOptionalResetData>(state, tag);
    body->emplaceStmt(&NLExecutor::runOptionalReset, resetData);
}

void NLTranslator::translateOptionalCollect(nl::OptionalCollect collect, NLStmtContainer* body) {
    NLOptionalState* state = optionalStateFor(collect.getState());

    // The buffers are allocated once, by the single collect feeding an accumulator.
    // Generated IR has exactly one collect per accumulator; a second would append to the
    // same buffers and double the rows, so it is rejected here as a sort's is.
    if (!state->buffers().empty()) {
        throw IRException("an nl.optional_buffer must be fed by a single nl.optional_collect");
    }

    NLOptionalCollectData* data = _program->allocFunctionData<NLOptionalCollectData>(state);

    if (const mlir::Value tag = collect.getTag()) {
        data->setTag(static_cast<const ColumnVector<uint64_t>*>(getColumn(tag)));
    }

    // One growing buffer per column the pattern contributes, row-aligned. The buffer keeps
    // the column's element type; the append copies a chunk's rows onto its tail.
    for (const mlir::Value column : collect.getColumns()) {
        Column* bufferColumn = allocColumnForChunkType(column.getType());
        state->addMatchedBuffer(bufferColumn);

        const NLOwnedAppendFunction appendOwned = NLExecutor::selectOwnedAppendFunction(bufferColumn);
        const NLAppendFunction append = appendOwned ? nullptr : selectAppendForChunkType(column.getType());

        data->addAppend(NLSortCollectData::Append {getColumn(column),
                                                   bufferColumn,
                                                   append,
                                                   appendOwned});
    }

    body->emplaceStmt(&NLExecutor::runOptionalCollect, data);
}

void NLTranslator::translateOptionalDrainLoop(const IteratorConfig& config,
                                              mlir::Block& loopBody,
                                              NLLimitState* limit,
                                              NLStmtContainer* body) {
    NLOptionalState* state = config._optionalState;
    if (!state) {
        throw IRException("nl.optional_drain iterator must carry an optional accumulator");
    }

    // For::verify binds one loop variable per iterator chunk, and the drain iterator's
    // chunk types are the collected column types, so the loop must take one per buffer.
    const size_t bufferCount = state->buffers().size();
    if (loopBody.getNumArguments() != bufferCount) {
        throw IRException("nl.optional_drain loop must bind one variable per collected column");
    }

    NLOptionalDrainLoopData* loopData = _program->allocFunctionData<NLOptionalDrainLoopData>(state);
    loopData->setLimit(limit);

    // Reserve the row-slice scratch so the per-step gather stays allocation-free, the same
    // as the edge loop's indices column.
    loopData->getIndices()->reserve(_program->getChunkSize());

    // The columns the pattern joined onto come first, in the order it yielded them back,
    // so the first inputColumns().size() of them are the ones a missed row is rebuilt from.
    const std::vector<const Column*>& inputColumns = state->inputColumns();

    for (size_t columnIndex = 0; columnIndex < bufferCount; columnIndex++) {
        const mlir::Value loopVariable = loopBody.getArgument(static_cast<unsigned>(columnIndex));
        const mlir::Type chunkType = loopVariable.getType();

        Column* output = allocColumnForChunkType(chunkType);
        _valueSlots[loopVariable] = output;

        const bool joinedOnto = columnIndex < inputColumns.size();

        NLOptionalDrainLoopData::DrainColumn column;
        column._buffer = state->buffers()[columnIndex];
        column._input = joinedOnto ? inputColumns[columnIndex] : nullptr;
        column._output = output;
        column._gather = selectGatherForChunkType(chunkType);
        column._fillNull = joinedOnto ? nullptr : selectFillNullForChunkType(chunkType);

        loopData->addColumn(column);
    }

    body->emplaceStmt(&NLExecutor::runOptionalDrainLoop, loopData);

    translateBlock(loopBody, loopData->getStmts());
}

void NLTranslator::translateExistsBuffer(nl::ExistsBuffer buffer, NLStmtContainer* body) {
    NLExistsState* state = _program->allocExistsState();
    _existsStates[buffer.getState()] = state;

    translateSubqueryExpressionReset(state, buffer.getInputColumns(), buffer.getTag(), body);
}

void NLTranslator::translateExistsMark(nl::ExistsMark mark, NLStmtContainer* body) {
    NLExistsState* state = existsStateFor(mark.getState());

    NLExistsMarkData* data = _program->allocFunctionData<NLExistsMarkData>(state);

    if (const mlir::Value tag = mark.getTag()) {
        data->setTag(static_cast<const ColumnVector<uint64_t>*>(getColumn(tag)));
    }

    if (const mlir::Value row = mark.getRow()) {
        data->setRow(static_cast<const ColumnVector<uint64_t>*>(getColumn(row)));
    }

    for (const mlir::Value column : mark.getColumns()) {
        data->addColumn(getColumn(column));
    }

    body->emplaceStmt(&NLExecutor::runExistsMark, data);
}

void NLTranslator::translateExistsResult(nl::ExistsResult result, NLStmtContainer* body) {
    NLExistsState* state = existsStateFor(result.getState());

    ColumnVector<CustomBool>* answer = static_cast<ColumnVector<CustomBool>*>(allocColumn(result.getResult()));

    NLExistsResultData* data = _program->allocFunctionData<NLExistsResultData>(state, answer);
    body->emplaceStmt(&NLExecutor::runExistsResult, data);
}

NLExistsState* NLTranslator::existsStateFor(mlir::Value handle) const {
    const auto stateIt = _existsStates.find(handle);
    if (stateIt == _existsStates.end()) {
        throw IRException("exists handle must be produced by an nl.exists_buffer");
    }

    return stateIt->second;
}

void NLTranslator::translateCountSubqueryBuffer(nl::CountSubqueryBuffer buffer, NLStmtContainer* body) {
    NLCountSubqueryState* state = _program->allocCountSubqueryState();
    _countSubqueryStates[buffer.getState()] = state;

    translateSubqueryExpressionReset(state, buffer.getInputColumns(), buffer.getTag(), body);
}

void NLTranslator::translateSubqueryExpressionReset(NLSubqueryExpressionState* state,
                                                    mlir::OperandRange inputColumns,
                                                    mlir::Value tag,
                                                    NLStmtContainer* body) {
    // This step's own chunks: their row count is how many answers the reset clears, and how
    // many the result lays out.
    for (const mlir::Value column : inputColumns) {
        state->addInputColumn(getColumn(column));
    }

    ColumnVector<uint64_t>* tagColumn = static_cast<ColumnVector<uint64_t>*>(allocColumn(tag));

    NLSubqueryExpressionResetData* resetData = _program->allocFunctionData<NLSubqueryExpressionResetData>(state, tagColumn);
    body->emplaceStmt(&NLExecutor::runSubqueryExpressionReset, resetData);
}

void NLTranslator::translateCountSubqueryTally(nl::CountSubqueryTally tally, NLStmtContainer* body) {
    NLCountSubqueryState* state = countSubqueryStateFor(tally.getState());

    const ColumnVector<uint64_t>* tag = nullptr;
    if (const mlir::Value tagValue = tally.getTag()) {
        tag = static_cast<const ColumnVector<uint64_t>*>(getColumn(tagValue));
    }

    const ColumnVector<uint64_t>* row = nullptr;
    if (const mlir::Value rowValue = tally.getRow()) {
        row = static_cast<const ColumnVector<uint64_t>*>(getColumn(rowValue));
    }

    const Column* rows = nullptr;
    if (const mlir::Value rowsValue = tally.getRows()) {
        rows = getColumn(rowsValue);
    }

    NLCountSubqueryTallyData* data = _program->allocFunctionData<NLCountSubqueryTallyData>(state, tag, row, rows);
    body->emplaceStmt(&NLExecutor::runCountSubqueryTally, data);
}

void NLTranslator::translateCountSubqueryResult(nl::CountSubqueryResult result, NLStmtContainer* body) {
    NLCountSubqueryState* state = countSubqueryStateFor(result.getState());

    ColumnVector<uint64_t>* counts = static_cast<ColumnVector<uint64_t>*>(allocColumn(result.getResult()));

    NLCountSubqueryResultData* data = _program->allocFunctionData<NLCountSubqueryResultData>(state, counts);
    body->emplaceStmt(&NLExecutor::runCountSubqueryResult, data);
}

NLCountSubqueryState* NLTranslator::countSubqueryStateFor(mlir::Value handle) const {
    const auto stateIt = _countSubqueryStates.find(handle);
    if (stateIt == _countSubqueryStates.end()) {
        throw IRException("count subquery handle must be produced by an nl.count_subquery_buffer");
    }

    return stateIt->second;
}

void NLTranslator::translateRowListsBuffer(nl::RowListsBuffer buffer, NLStmtContainer* body) {
    NLRowListsState* state = _program->allocRowListsState();
    _rowListsStates[buffer.getState()] = state;

    const mlir::Value cardinality = buffer.getCardinality();
    const Column* const rows = cardinality ? getColumn(cardinality) : nullptr;

    ColumnVector<uint64_t>* tag = static_cast<ColumnVector<uint64_t>*>(allocColumn(buffer.getTag()));

    NLRowListsResetData* resetData =
        _program->allocFunctionData<NLRowListsResetData>(state, rows, tag);
    body->emplaceStmt(&NLExecutor::runRowListsReset, resetData);
}

void NLTranslator::translateRowListsCollect(nl::RowListsCollect collect, NLStmtContainer* body) {
    NLRowListsState* state = rowListsStateFor(collect.getState());

    const mlir::Value value = collect.getValue();
    const mlir::Value tag = collect.getTag();
    const mlir::Value row = collect.getRow();

    const ColumnVector<uint64_t>* tagColumn = nullptr;
    if (tag) {
        tagColumn = static_cast<const ColumnVector<uint64_t>*>(getColumn(tag));
    }

    const ColumnVector<uint64_t>* rowColumn = nullptr;
    if (row) {
        rowColumn = static_cast<const ColumnVector<uint64_t>*>(getColumn(row));
    }

    NLRowListsCollectData* data =
        _program->allocFunctionData<NLRowListsCollectData>(state,
                                                           tagColumn,
                                                           rowColumn,
                                                           getColumn(value),
                                                           selectListItemRead(value.getType()),
                                                           _program->allocStepBuffers());
    body->emplaceStmt(&NLExecutor::runRowListsCollect, data);
}

void NLTranslator::translateRowListsResult(nl::RowListsResult result, NLStmtContainer* body) {
    NLRowListsState* state = rowListsStateFor(result.getState());

    const mlir::Value resultValue = result.getResult();
    Column* const lists = allocColumnForChunkType(resultValue.getType());
    _valueSlots[resultValue] = lists;

    NLRowListsResultData* data =
        _program->allocFunctionData<NLRowListsResultData>(state,
                                                          static_cast<ColumnVector<ListView>*>(lists),
                                                          _program->allocStepBuffers());
    body->emplaceStmt(&NLExecutor::runRowListsResult, data);
}

NLRowListsState* NLTranslator::rowListsStateFor(mlir::Value handle) const {
    const auto stateIt = _rowListsStates.find(handle);
    if (stateIt == _rowListsStates.end()) {
        throw IRException("row lists handle must be produced by an nl.row_lists_buffer");
    }

    return stateIt->second;
}

NLOptionalState* NLTranslator::optionalStateFor(mlir::Value handle) const {
    const auto stateIt = _optionalStates.find(handle);
    if (stateIt == _optionalStates.end()) {
        throw IRException("optional handle must be produced by an nl.optional_buffer");
    }

    return stateIt->second;
}

void NLTranslator::translateGroupAggregateBuffer(nl::GroupAggregateBuffer buffer, NLStmtContainer* body) {
    // Allocate the runtime accumulator and map the handle to it, so the update and
    // the emit loop that name the handle share the same group table and per-group
    // state. The key buffers and accumulators themselves are allocated by the
    // update, which knows their types - the grouped sibling of translateSortBuffer.
    NLGroupAggregateState* state = _program->allocGroupAggregateState();
    _groupAggregateStates[buffer.getState()] = state;

    // The reset empties the group table and per-group state each time the block
    // holding this nl.group_aggregate_buffer runs: once at function scope for a
    // top-level grouped RETURN.
    NLGroupAggregateResetData* resetData = _program->allocFunctionData<NLGroupAggregateResetData>(state);
    body->addStmt(NLFunctionDescriptor {&NLExecutor::runGroupAggregateReset, resetData});
}

void NLTranslator::buildGroupAggregate(mlir::storage::GroupAggregateKind mlirKind,
                                       mlir::Value column,
                                       NLGroupAggregates::Aggregate& aggregate) {
    const GroupAggregateKind kind = toRuntimeGroupAggregateKind(mlirKind);
    const mlir::Type chunkType = column.getType();

    aggregate._input = getColumn(column);

    if (chargesDistinctValues(kind)) {
        aggregate._distinct = allocGroupDistinctTally(chunkType);
    }

    // A switch (not an if/else) over every kind so a new one is a compile error
    // here rather than silently taking the value-reduction path.
    switch (kind) {
        case GroupAggregateKind::Count: {
            // count tallies rows, so it keeps no reduced value, only the per-group
            // tally. count(*) over an ID chunk charges every row; count(x) over a
            // nullable value chunk charges only the present values.
            NLExecutor::selectGroupAggregateState(kind, ValueType::Int64, aggregate._stateType);

            const auto chunk = mlir::cast<nl::ChunkType>(chunkType);
            const mlir::Type countElementType = chunk.getElementType();
            const auto nullable = mlir::dyn_cast<storage::NullableType>(countElementType);
            const mlir::Type countValueType = nullable ? nullable.getValueType() : mlir::Type {};
            if (isNullableList(countElementType)) {
                aggregate._fold = NLExecutor::selectGroupCountOptListFold();
            } else if (nullable) {
                aggregate._fold = NLExecutor::selectGroupCountOptFold(chunkKindFromNullableValue(countValueType));
            } else if (mlir::isa<storage::AnyType>(countElementType)) {
                aggregate._fold = NLExecutor::selectGroupCountCellFold();
            } else if (llvm::isa<storage::ListType>(countElementType)) {
                // No row of a list chunk is null, so a group's tally is its whole row
                // count, as it is for count(*).
                aggregate._fold = NLExecutor::selectGroupCountAllFold();
            } else if (isEntityIDElement(countElementType)) {
                // An invalid ID is the null an OPTIONAL MATCH leaves, so a group charges
                // its valid rows alone.
                const NLChunkKind countKind = chunkKindFromElementType(countElementType);
                aggregate._fold = NLExecutor::selectGroupCountValidIDFold(countKind);
            } else if (mlir::isa<storage::PathType>(countElementType)) {
                aggregate._fold = NLExecutor::selectGroupCountPresentPathFold();
            } else {
                // Every other non-nullable chunk holds no null to skip - a column a CALL
                // yielded - so every row of the group is charged.
                aggregate._fold = NLExecutor::selectGroupCountAllFold();
            }
        }
        break;

        case GroupAggregateKind::CountRows: {
            // count(*) charges every row of the group and reads no value, so the
            // chunk it is anchored on needs no type dispatch at all
            NLExecutor::selectGroupAggregateState(kind, ValueType::Int64, aggregate._stateType);
            aggregate._fold = NLExecutor::selectGroupCountAllFold();
        }
        break;

        case GroupAggregateKind::CountDistinct: {
            // count(DISTINCT x) keeps the same per-group tally as count, charged
            // once per distinct value instead of once per row, so it shares count's
            // state and differs only in the fold. count(DISTINCT n) over an
            // ID chunk keys on the ID; count(DISTINCT x) over a nullable value chunk
            // keys on the value and skips the nulls.
            NLExecutor::selectGroupAggregateState(kind, ValueType::Int64, aggregate._stateType);

            const auto chunk = mlir::cast<nl::ChunkType>(chunkType);
            const mlir::Type distinctElementType = chunk.getElementType();
            const auto nullable = mlir::dyn_cast<storage::NullableType>(distinctElementType);
            const mlir::Type distinctValueType = nullable ? nullable.getValueType() : mlir::Type {};
            if (isNullableList(distinctElementType)) {
                aggregate._fold = NLExecutor::selectGroupCountDistinctOptListFold();
            } else if (nullable) {
                aggregate._fold = NLExecutor::selectGroupCountDistinctOptFold(chunkKindFromNullableValue(distinctValueType));
            } else if (mlir::isa<storage::AnyType>(distinctElementType)) {
                aggregate._fold = NLExecutor::selectGroupCountDistinctCellFold();
            } else {
                aggregate._fold = NLExecutor::selectGroupCountDistinctChunkFold(getChunkKind(chunkType));
            }
        }
        break;

        case GroupAggregateKind::Sum:
        case GroupAggregateKind::SumDistinct:
        case GroupAggregateKind::Min:
        case GroupAggregateKind::Max:
        case GroupAggregateKind::Avg:
        case GroupAggregateKind::AvgDistinct:
        case GroupAggregateKind::StDev:
        case GroupAggregateKind::StDevP:
        case GroupAggregateKind::StDevDistinct:
        case GroupAggregateKind::StDevPDistinct:
        case GroupAggregateKind::First: {
            // sum/min/max/avg/stdev/first reduce the values themselves, so the input must be
            // a nullable value chunk; stdev and avg of numbers accumulate as f64, the rest in
            // the input's own type. nullableChunkValueType rejects an ID chunk here. The
            // distinct kinds keep the shape of the kind they mirror and differ only in the
            // fold, which charges each of a group's values once.
            //
            // A type-erased input carries a type per cell, so stdev reduces it by tag into an
            // f64, sum and avg keep a tagged sum - an integer until a float joins it, or a
            // duration - and min/max/first keep a cell.
            const mlir::Type reducedElement = mlir::cast<nl::ChunkType>(chunkType).getElementType();
            const bool reducesTaggedCells = mlir::isa<storage::AnyType>(reducedElement);

            const bool keepsAValue = (kind == GroupAggregateKind::Min)
                                  || (kind == GroupAggregateKind::Max)
                                  || (kind == GroupAggregateKind::First);
            const bool sums = (kind == GroupAggregateKind::Sum) || (kind == GroupAggregateKind::SumDistinct);
            const bool averages = (kind == GroupAggregateKind::Avg) || (kind == GroupAggregateKind::AvgDistinct);
            const bool keepsTaggedCells = reducesTaggedCells && (keepsAValue || sums || averages);

            const auto nullableReduced = mlir::dyn_cast<storage::NullableType>(reducedElement);
            const mlir::Type reducedValue = nullableReduced ? nullableReduced.getValueType() : reducedElement;
            const bool accumulatesAsDouble = storage::reducesToADouble(mlirKind, reducedValue);

            const ValueType inputType = reducesTaggedCells ? ValueType::Double
                                                           : nullableChunkValueType(chunkType);
            const ValueType accumulatorType = accumulatesAsDouble ? ValueType::Double : inputType;

            if (keepsTaggedCells) {
                NLExecutor::selectTaggedGroupAggregateState(kind, aggregate._stateType);
            } else {
                NLExecutor::selectGroupAggregateState(kind, accumulatorType, aggregate._stateType);
            }

            if (reducesTaggedCells) {
                aggregate._fold = NLExecutor::selectTaggedGroupAggregateFold(kind);
            } else {
                aggregate._fold = NLExecutor::selectGroupAggregateFold(kind, inputType);
            }
        }
        break;
    }
}

void NLTranslator::addGroupKeyColumn(NLGroupTable& groupTable, mlir::Value column) {
    const mlir::Type chunkType = column.getType();

    NLGroupHashKey hashKey;
    buildGroupHashKey(chunkType, hashKey);

    NLGroupTable::KeyColumn key;
    key._input = getColumn(column);
    key._buffer = allocColumnForChunkType(chunkType);
    key._gatherAppend = selectGroupKeyGatherForChunkType(chunkType);
    key._emitCopy = selectCopyForChunkType(chunkType);
    key._gather = selectGatherForChunkType(chunkType);

    groupTable.addKeyColumn(hashKey, key);
}

NLGroupDistinctTally* NLTranslator::allocGroupDistinctTally(mlir::Type valueChunkType) {
    NLGroupHashKey valueKey;
    buildGroupHashKey(valueChunkType, valueKey);

    NLGroupDistinctTally* tally = _program->allocGroupDistinctTally();
    tally->setValueKey(valueKey);

    return tally;
}

void NLTranslator::translateGroupAggregateUpdate(nl::GroupAggregateUpdate update, NLStmtContainer* body) {
    const mlir::Value updateState = update.getState();
    NLGroupAggregateState* state = groupAggregateStateFor(updateState);

    // The keyCount / aggregate-kind spec lives on the nl.group_aggregate_buffer that
    // produced the handle.
    nl::GroupAggregateBuffer buffer = updateState.getDefiningOp<nl::GroupAggregateBuffer>();
    if (!buffer) {
        throw IRException("group_aggregate_update state must come from nl.group_aggregate_buffer");
    }

    // The key buffers and aggregates are set up once, by the single update
    // feeding an accumulator. Generated IR has exactly one update per accumulator; a
    // second would fold the same rows twice, so it is rejected here.
    NLGroupTable& groupTable = state->groupTable();
    NLGroupAggregates& aggregates = state->aggregates();
    if (!groupTable.keyColumns().empty() || !aggregates.getAggregates().empty()) {
        throw IRException("an nl.group_aggregate_buffer must be fed by a single nl.group_aggregate_update");
    }

    const mlir::OperandRange columns = update.getColumns();
    const size_t keyCount = buffer.getKeyCount();
    const llvm::ArrayRef<int64_t> kinds = buffer.getKinds();

    // The collected columns are the grouping keys followed by the aggregate inputs,
    // one per aggregate; the two counts must partition them exactly.
    if (columns.size() != keyCount + kinds.size()) {
        throw IRException("group_aggregate collects one column per grouping key and aggregate");
    }

    NLGroupAggregateUpdateData* data = _program->allocFunctionData<NLGroupAggregateUpdateData>(state);

    for (size_t keyIndex = 0; keyIndex < keyCount; keyIndex++) {
        addGroupKeyColumn(groupTable, columns[keyIndex]);
    }

    groupTable.hashTable().chooseTable();

    // One state per group per aggregate, with the state type and the fold baked
    // from the kind and the input value type.
    for (size_t aggregateIndex = 0; aggregateIndex < kinds.size(); aggregateIndex++) {
        const auto kind = static_cast<storage::GroupAggregateKind>(kinds[aggregateIndex]);

        NLGroupAggregates::Aggregate aggregate;
        buildGroupAggregate(kind, columns[keyCount + aggregateIndex], aggregate);

        aggregates.add(aggregate);
    }

    body->addStmt(NLFunctionDescriptor {&NLExecutor::runGroupAggregateUpdate, data});
}

void NLTranslator::translateGroupAggregateLoop(const IteratorConfig& config,
                                               mlir::Block& loopBody,
                                               NLLimitState* limit,
                                               NLStmtContainer* body) {
    NLGroupAggregateState* state = config._groupAggregateState;
    if (!state) {
        throw IRException("nl.group_aggregate iterator must carry a group accumulator");
    }

    // For::verify binds one loop variable per iterator chunk, and the group
    // iterator's chunks are the grouping-key columns followed by the aggregate
    // results, so the loop must take exactly one variable per output column.
    std::vector<NLGroupTable::KeyColumn>& keyColumns = state->groupTable().keyColumns();
    const size_t keyCount = keyColumns.size();
    const std::span<NLGroupAggregates::Aggregate> aggregates = state->aggregates().getAggregates();
    const size_t aggregateCount = aggregates.size();
    if (loopBody.getNumArguments() != keyCount + aggregateCount) {
        throw IRException("nl.group_aggregate loop must bind one variable per output column");
    }

    NLGroupAggregateLoopData* loopData = _program->allocFunctionData<NLGroupAggregateLoopData>(state,
                                                                                          _program->allocStepBuffers());
    loopData->setLimit(limit);

    // Each loop variable is the emit output of one column: the first keyCount are the
    // grouping keys (filled by slicing their key buffers), the rest the aggregate
    // results (filled from the per-group state). Wire each into its column's slot on
    // the shared state, so runGroupAggregateLoop fills the loop variables directly.
    for (size_t keyIndex = 0; keyIndex < keyCount; keyIndex++) {
        const mlir::Value loopVariable = loopBody.getArgument(static_cast<unsigned>(keyIndex));

        Column* output = allocColumnForChunkType(loopVariable.getType());
        _valueSlots[loopVariable] = output;
        keyColumns[keyIndex]._output = output;
    }

    for (size_t aggregateIndex = 0; aggregateIndex < aggregateCount; aggregateIndex++) {
        const mlir::Value loopVariable = loopBody.getArgument(static_cast<unsigned>(keyCount + aggregateIndex));

        Column* output = allocColumnForChunkType(loopVariable.getType());
        _valueSlots[loopVariable] = output;
        aggregates[aggregateIndex]._output = output;
    }

    body->addStmt(NLFunctionDescriptor {&NLExecutor::runGroupAggregateLoop, loopData});

    translateBlock(loopBody, loopData->getStmts());
}

NLGroupAggregateState* NLTranslator::groupAggregateStateFor(mlir::Value handle) const {
    const auto stateIt = _groupAggregateStates.find(handle);
    if (stateIt == _groupAggregateStates.end()) {
        throw IRException("group aggregate handle must be produced by an nl.group_aggregate_buffer");
    }

    return stateIt->second;
}

void NLTranslator::translateCollectBuffer(nl::CollectBuffer buffer, NLStmtContainer* body) {
    // Allocate the runtime accumulator and map the handle to it, so the update (and
    // later the drain) that name the handle share the same group table and per-group
    // lists. The key buffers and the value buffer are allocated by the update, which
    // knows their types - the collect sibling of translateGroupAggregateBuffer.
    NLCollectState* state = _program->allocCollectState();
    _collectStates[buffer.getState()] = state;

    // The reset empties the group table and per-group lists each time the block
    // holding this nl.collect_buffer runs: once at function scope for a top-level
    // collect.
    NLCollectResetData* resetData = _program->allocFunctionData<NLCollectResetData>(state);
    body->emplaceStmt(&NLExecutor::runCollectReset, resetData);
}

void NLTranslator::translateCollectUpdate(nl::CollectUpdate update, NLStmtContainer* body) {
    const mlir::Value updateState = update.getState();
    NLCollectState* state = collectStateFor(updateState);

    // The keyCount lives on the nl.collect_buffer that produced the handle.
    nl::CollectBuffer buffer = updateState.getDefiningOp<nl::CollectBuffer>();
    if (!buffer) {
        throw IRException("collect_update state must come from nl.collect_buffer");
    }

    // The key buffers and the value buffers are allocated once, by the single update
    // feeding an accumulator. Generated IR has exactly one update per accumulator; a
    // second would append the same rows twice, so it is rejected here.
    NLGroupTable& groupTable = state->groupTable();
    if (!groupTable.keyColumns().empty() || !state->valueColumns().empty()) {
        throw IRException("an nl.collect_buffer must be fed by a single nl.collect_update");
    }

    const mlir::OperandRange columns = update.getColumns();
    const size_t keyCount = buffer.getKeyCount();
    const llvm::ArrayRef<int64_t> kinds = buffer.getKinds().value_or(llvm::ArrayRef<int64_t> {});

    // The collected columns are the grouping keys, the value columns, then one input per
    // aggregate reduced over the same groups: what the keys and the aggregates leave is
    // what the collect gathers, and it gathers at least one. Bound keyCount by the column
    // count before summing, as nl.collect_update's verifier does.
    const size_t columnCount = columns.size();
    if (keyCount >= columnCount || columnCount - keyCount <= kinds.size()) {
        throw IRException("collect collects at least one value column after the grouping keys, then one column per aggregate");
    }

    const size_t valueCount = columnCount - keyCount - kinds.size();

    const llvm::ArrayRef<int64_t> distinctValues = buffer.getDistinctValues().value_or(llvm::ArrayRef<int64_t> {});

    NLCollectUpdateData* data = _program->allocFunctionData<NLCollectUpdateData>(state);

    for (size_t keyIndex = 0; keyIndex < keyCount; keyIndex++) {
        addGroupKeyColumn(groupTable, columns[keyIndex]);
    }

    groupTable.hashTable().chooseTable();

    // Each collected column is either a nullable value chunk (a property fetch), whose
    // present values accumulate in a flat buffer of the same primitive with nulls
    // dropped to match Cypher collect, or an entity chunk, whose every row carries an
    // ID. The fold (append) and the drain emit handlers are baked from that type here,
    // so whichever drain the query uses reads a ready handler off the state.
    for (size_t valueIndex = 0; valueIndex < valueCount; valueIndex++) {
        const mlir::Value column = columns[keyCount + valueIndex];
        const mlir::Type element = mlir::cast<nl::ChunkType>(column.getType()).getElementType();

        const bool isDistinct = llvm::is_contained(distinctValues, static_cast<int64_t>(valueIndex));

        NLCollectState::ValueColumn value;
        value._input = getColumn(column);

        if (isDistinct) {
            value._distinct = allocGroupDistinctTally(column.getType());
        }

        if (isNullableList(element)) {
            NLCollectFoldFunction fold = nullptr;
            NLCollectListEmitFunction listEmit = nullptr;
            NLExecutor::selectCollectOptContainerHandlers<ListView>(isDistinct, fold, listEmit);

            value._buffer = allocListColumn();
            value._fold = fold;
            value._listEmit = listEmit;
        } else if (isNullableMap(element)) {
            NLCollectFoldFunction fold = nullptr;
            NLCollectListEmitFunction listEmit = nullptr;
            NLExecutor::selectCollectOptContainerHandlers<MapView>(isDistinct, fold, listEmit);

            value._buffer = allocColumnForKind(NLChunkKind::Map);
            value._fold = fold;
            value._listEmit = listEmit;
        } else if (mlir::isa<storage::NullableType>(element)) {
            const NLChunkKind kind = getNullableChunkKind(column.getType());

            value._buffer = allocColumnForKind(kind);
            value._fold = isDistinct ? NLExecutor::selectCollectDistinctFold(kind)
                                     : NLExecutor::selectCollectFold(kind);
            value._unwindCollectEmit = NLExecutor::selectUnwindCollectValueEmit(kind);
            value._listEmit = NLExecutor::selectCollectListEmit(kind);
        } else if (mlir::isa<storage::ListType>(element)) {
            NLCollectFoldFunction fold = nullptr;
            NLCollectListEmitFunction listEmit = nullptr;
            NLExecutor::selectCollectContainerHandlers<ListView>(isDistinct, fold, listEmit);

            value._buffer = allocListColumn();
            value._fold = fold;
            value._listEmit = listEmit;
        } else if (mlir::isa<storage::MapType>(element)) {
            NLCollectFoldFunction fold = nullptr;
            NLCollectListEmitFunction listEmit = nullptr;
            NLExecutor::selectCollectContainerHandlers<MapView>(isDistinct, fold, listEmit);

            value._buffer = allocColumnForKind(NLChunkKind::Map);
            value._fold = fold;
            value._listEmit = listEmit;
        } else if (mlir::isa<storage::AnyType>(element)) {
            NLCollectFoldFunction fold = nullptr;
            NLCollectListEmitFunction listEmit = nullptr;
            NLExecutor::selectCollectTaggedHandlers(isDistinct, fold, listEmit);

            value._buffer = allocCellColumn();
            value._fold = fold;
            value._listEmit = listEmit;
        } else if (mlir::isa<storage::PathType>(element)) {
            NLCollectFoldFunction fold = nullptr;
            NLCollectListEmitFunction listEmit = nullptr;
            NLExecutor::selectCollectPathHandlers(isDistinct, fold, listEmit);

            value._buffer = allocCellColumn();
            value._fold = fold;
            value._listEmit = listEmit;
        } else {
            const NLChunkKind kind = getChunkKind(column.getType());

            NLCollectFoldFunction fold = nullptr;
            NLCollectListEmitFunction listEmit = nullptr;
            NLExecutor::selectCollectEntityHandlers(kind, isDistinct, fold, listEmit);

            value._buffer = allocColumnForKind(kind);
            value._fold = fold;
            value._listEmit = listEmit;
        }

        state->addValueColumn(value);
    }

    // The reductions taken beside the list read the same groups, so their states
    // live on this state and are wired exactly as a grouped aggregation's are.
    NLGroupAggregates& aggregates = state->aggregates();

    for (size_t aggregateIndex = 0; aggregateIndex < kinds.size(); aggregateIndex++) {
        const storage::GroupAggregateKind aggregateKind = static_cast<storage::GroupAggregateKind>(kinds[aggregateIndex]);

        NLGroupAggregates::Aggregate aggregate;
        buildGroupAggregate(aggregateKind, columns[keyCount + valueCount + aggregateIndex], aggregate);

        aggregates.add(aggregate);
    }

    body->emplaceStmt(&NLExecutor::runCollectUpdate, data);
}

NLCollectState* NLTranslator::collectStateFor(mlir::Value handle) const {
    const auto stateIt = _collectStates.find(handle);
    if (stateIt == _collectStates.end()) {
        throw IRException("collect handle must be produced by an nl.collect_buffer");
    }

    return stateIt->second;
}

void NLTranslator::translateUnwindCollectLoop(const IteratorConfig& config,
                                       mlir::Block& loopBody,
                                       NLStmtContainer* body) {
    NLCollectState* state = config._collectState;
    if (!state) {
        throw IRException("nl.unwind_collect iterator must carry a collect accumulator");
    }

    throwIfAlreadyDrained(state);

    // Only a value collect bakes an unwind emit handler; an entity collect leaves it
    // unset, so its list can be returned but not yet unwound element by element.
    NLCollectState::ValueColumn& unwound = state->unwoundColumn();
    if (!unwound._unwindCollectEmit) {
        throw IRException("unwinding a collected entity list is not supported");
    }

    // For::verify binds one loop variable per iterator chunk; the unwind iterator's
    // chunks are the grouping keys then the element value, so the loop takes one
    // variable per grouping key plus one for the value.
    std::vector<NLGroupTable::KeyColumn>& keyColumns = state->groupTable().keyColumns();
    const size_t keyCount = keyColumns.size();
    if (loopBody.getNumArguments() != keyCount + 1) {
        throw IRException("nl.unwind_collect loop must bind one variable per grouping key plus the element");
    }

    NLUnwindCollectLoopData* loopData = _program->allocFunctionData<NLUnwindCollectLoopData>(state);
    loopData->getGroupIndices()->reserve(_program->getChunkSize());
    loopData->getPositions()->reserve(_program->getChunkSize());

    // The first keyCount loop variables are the grouping keys (filled by gathering
    // their key buffers, one row per emitted element); wire each into its key column's
    // output slot on the shared state.
    for (size_t keyIndex = 0; keyIndex < keyCount; keyIndex++) {
        const mlir::Value loopVariable = loopBody.getArgument(static_cast<unsigned>(keyIndex));

        Column* output = allocColumnForChunkType(loopVariable.getType());
        _valueSlots[loopVariable] = output;
        keyColumns[keyIndex]._output = output;
    }

    // The last loop variable is the unwound element value (a nullable value chunk).
    const mlir::Value valueVariable = loopBody.getArgument(static_cast<unsigned>(keyCount));
    Column* valueOutput = allocColumnForChunkType(valueVariable.getType());
    _valueSlots[valueVariable] = valueOutput;
    unwound._output = valueOutput;

    body->emplaceStmt(&NLExecutor::runUnwindCollectLoop, loopData);

    translateBlock(loopBody, loopData->getStmts());
}

void NLTranslator::translateCollectLoop(const IteratorConfig& config,
                                        mlir::Block& loopBody,
                                        NLStmtContainer* body) {
    NLCollectState* state = config._collectState;
    if (!state) {
        throw IRException("nl.collect iterator must carry a collect accumulator");
    }

    throwIfAlreadyDrained(state);

    // The collect iterator's chunks are the grouping keys, one list cell per collected
    // column, then one per aggregate reduced over the same groups.
    std::vector<NLGroupTable::KeyColumn>& keyColumns = state->groupTable().keyColumns();
    const size_t keyCount = keyColumns.size();
    std::vector<NLCollectState::ValueColumn>& valueColumns = state->valueColumns();
    const std::span<NLGroupAggregates::Aggregate> aggregates = state->aggregates().getAggregates();
    if (loopBody.getNumArguments() != keyCount + valueColumns.size() + aggregates.size()) {
        throw IRException("nl.collect loop must bind one variable per grouping key, one per list, and one per aggregate");
    }

    NLCollectLoopData* loopData = _program->allocFunctionData<NLCollectLoopData>(state, _program->allocStepBuffers());

    // The first keyCount loop variables are the grouping keys (filled by slicing their
    // key buffers, one row per group); wire each into its key column's output slot.
    for (size_t keyIndex = 0; keyIndex < keyCount; keyIndex++) {
        const mlir::Value loopVariable = loopBody.getArgument(static_cast<unsigned>(keyIndex));

        Column* output = allocColumnForChunkType(loopVariable.getType());
        _valueSlots[loopVariable] = output;
        keyColumns[keyIndex]._output = output;
    }

    // Then one per-group list cell per collected column (a ColumnVector<ListView>).
    for (size_t valueIndex = 0; valueIndex < valueColumns.size(); valueIndex++) {
        const mlir::Value listVariable = loopBody.getArgument(static_cast<unsigned>(keyCount + valueIndex));

        Column* listOutput = allocColumnForChunkType(listVariable.getType());
        _valueSlots[listVariable] = listOutput;
        valueColumns[valueIndex]._output = listOutput;
    }

    // The remaining variables take the reductions, one per aggregate, filled from the
    // same group window the lists are sliced over.
    for (size_t aggregateIndex = 0; aggregateIndex < aggregates.size(); aggregateIndex++) {
        const unsigned argumentIndex = static_cast<unsigned>(keyCount + valueColumns.size() + aggregateIndex);
        const mlir::Value loopVariable = loopBody.getArgument(argumentIndex);

        Column* output = allocColumnForChunkType(loopVariable.getType());
        _valueSlots[loopVariable] = output;
        aggregates[aggregateIndex]._output = output;
    }

    body->emplaceStmt(&NLExecutor::runCollectLoop, loopData);

    translateBlock(loopBody, loopData->getStmts());
}

const ColumnVector<CustomBool>* NLTranslator::getMaskColumn(mlir::Value chunkValue) const {
    if (!chunkValue) {
        return nullptr;
    }

    return static_cast<const ColumnVector<CustomBool>*>(getColumn(chunkValue));
}

NLKeyAppendFunction NLTranslator::selectMergeKeyAppend(mlir::Type chunkType,
                                                      const Column* column,
                                                      ValueType keyType) {
    if (isUntypedNullChunkType(chunkType)) {
        return NLExecutor::selectNullMergeKeyAppendFunction();
    }

    if (isConstantColumn(column)) {
        return NLExecutor::selectConstMergeKeyAppendFunction(valueTypeFromChunkType(chunkType), keyType);
    }

    const mlir::Type elementType = mlir::cast<nl::ChunkType>(chunkType).getElementType();

    if (const auto nullableType = mlir::dyn_cast<storage::NullableType>(elementType)) {
        return NLExecutor::selectOptMergeKeyAppendFunction(chunkKindFromNullableValue(nullableType.getValueType()), keyType);
    }

    return NLExecutor::selectPlainMergeKeyAppendFunction(chunkKindFromElementType(elementType), keyType);
}
