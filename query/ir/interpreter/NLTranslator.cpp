#include "NLTranslator.h"

#include <algorithm>
#include <optional>
#include <string_view>
#include <unordered_map>

#include <spdlog/fmt/bundled/format.h>

#include "NLOps.h"

#include "IRConstantColumn.h"
#include "IRRowAlignment.h"
#include "list/ListBuffer.h"
#include "mlir/IR/Block.h"
#include "mlir/IR/BuiltinAttributes.h"
#include "mlir/IR/BuiltinTypes.h"
#include "mlir/IR/Verifier.h"
#include "llvm/ADT/DenseMap.h"
#include "llvm/ADT/SmallVector.h"

#include "Procedure.h"
#include "ProcedureContext.h"
#include "ProcedureData.h"
#include "ProcedureManager.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnMask.h"
#include "columns/ColumnOptVector.h"
#include "columns/Functions.h"
#include "list/ListView.h"
#include "metadata/GraphMetadata.h"
#include "metadata/LabelSet.h"
#include "metadata/LabelSetHandle.h"
#include "metadata/PropertyNull.h"
#include "metadata/PropertyType.h"
#include "reader/GraphReader.h"
#include "views/GraphView.h"
#include "writers/MetadataBuilder.h"

#include "NLExecutor.h"
#include "NLSystemTranslator.h"

#include "LocalMemory.h"
#include "IRException.h"
#include "BioAssert.h"

using namespace db;

namespace nl = mlir::nl;
namespace storage = mlir::storage;

namespace {

// A chunk holding the single row a reduction collapsed the whole relation to, or a
// computation over such rows and constants: like a constant, it holds one value for
// every row of the step that reads it, whichever loop that step belongs to.
bool yieldsReducedRowChunk(mlir::Value column, llvm::DenseMap<mlir::Value, bool>& classified) {
    const auto classifiedIt = classified.find(column);
    if (classifiedIt != classified.end()) {
        return classifiedIt->second;
    }

    mlir::Operation* const definingOp = column.getDefiningOp();

    bool isReducedRow = false;
    if (definingOp) {
        if (mlir::isa<nl::CountResult, nl::AggregateResult>(definingOp)) {
            isReducedRow = true;
        } else if (definingOp->hasTrait<mlir::OpTrait::ConstantThroughOperands>()) {
            bool readsAReducedRow = false;
            bool everyOperandStandsForEveryRow = true;

            for (const mlir::Value operand : definingOp->getOperands()) {
                const bool operandIsReducedRow = yieldsReducedRowChunk(operand, classified);
                const bool operandStandsForEveryRow = operandIsReducedRow || yieldsConstantColumn(operand);

                readsAReducedRow = readsAReducedRow || operandIsReducedRow;
                everyOperandStandsForEveryRow = everyOperandStandsForEveryRow && operandStandsForEveryRow;
            }

            isReducedRow = readsAReducedRow && everyOperandStandsForEveryRow;
        }
    }

    classified[column] = isReducedRow;

    return isReducedRow;
}

bool yieldsReducedRowChunk(mlir::Value column) {
    llvm::DenseMap<mlir::Value, bool> classified;

    return yieldsReducedRowChunk(column, classified);
}

using NLUnaryFunctionSelector = NLUnaryFunctionKernel (*)(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);

const std::unordered_map<std::string_view, NLUnaryFunctionSelector> unaryFunctionSelectors = {
    {"nl.labels",     &NLExecutor::selectFunction<LabelsFunction>},
    {"nl.edge_type",  &NLExecutor::selectFunction<EdgeTypesFunction>},
    {"nl.to_integer", &NLExecutor::selectFunction<toIntegerFunction>},
    {"nl.to_float",   &NLExecutor::selectFunction<toFloatFunction>},
    {"nl.to_boolean", &NLExecutor::selectFunction<toBoolFunction>},
};

NLUnaryFunctionSelector lookupUnaryFunctionSelector(mlir::Operation& operation) {
    const llvm::StringRef name = operation.getName().getStringRef();
    const auto it = unaryFunctionSelectors.find(std::string_view(name.data(), name.size()));
    return it == unaryFunctionSelectors.end() ? nullptr : it->second;
}

using NLBinaryFunctionSelector = NLBinaryFn (*)(const Column* lhs, const Column* rhs, LocalMemory* memory, Column*& result);

const std::unordered_map<std::string_view, NLBinaryFunctionSelector> binaryFunctionSelectors = {
    {"nl.cosine_similarity",  &NLExecutor::selectBinary<OP_FUNC_COSINE_SIMILARITY>},
    {"nl.euclidean_distance", &NLExecutor::selectBinary<OP_FUNC_EUCLIDEAN_DISTANCE>},
};

NLBinaryFunctionSelector lookupBinaryFunctionSelector(mlir::Operation& operation) {
    const llvm::StringRef name = operation.getName().getStringRef();
    const auto it = binaryFunctionSelectors.find(std::string_view(name.data(), name.size()));
    return it == binaryFunctionSelectors.end() ? nullptr : it->second;
}

// Pool-allocate a plain (never-null) chunk column of the given element type, reserving a
// full chunk so execution stays allocation-free. Every such column - an ID chunk, or one a
// procedure yielded - is a ColumnVector, so the element type is all that varies.
template <typename T>
Column* allocPlainChunkColumn(LocalMemory* memory, size_t chunkSize) {
    ColumnVector<T>* column = memory->alloc<ColumnVector<T>>();
    column->reserve(chunkSize);
    return column;
}

// The edge type name carried by the nl.get_edge_type handle a by-type hop's
// edge_type operand names. The name lives on the handle op, not the hop, so it is
// resolved once above the loops; a hop reads it back through its operand here (the
// same way translatePropertyFetch reads a property name off nl.get_property_type).
llvm::StringRef edgeTypeName(mlir::Value handle) {
    nl::GetEdgeType handleOp = handle.getDefiningOp<nl::GetEdgeType>();
    if (!handleOp) {
        throw IRException("edge_type operand must come from nl.get_edge_type");
    }

    return handleOp.getName();
}

// The with-null fetch handler for a property's value type, on the node side
// when isNode is true and the edge side otherwise. Selecting it here keeps the
// value-type dispatch with the rest of translation; the handler bodies live in
// NLExecutor.
NLHandlerFunction selectPropertyFetchHandler(bool isNode, ValueType valueType) {
    // The handler is the runPropertyFetch instantiation for the node or edge ID
    // type and the property's value type; ValueTypeDispatcher maps the runtime
    // value type to that compile-time type, so the per-type fan-out lives in one
    // shared place rather than a switch repeated across the lowering.
    NLHandlerFunction handler = nullptr;
    const auto select = [&]<SupportedType T>() {
        handler = isNode ? &NLExecutor::runPropertyFetch<NodeID, T>
                         : &NLExecutor::runPropertyFetch<EdgeID, T>;
    };
    ValueTypeDispatcher(valueType).execute(select);

    return handler;
}

// Reverse of DBLowering's valueTypeToElementType: recover the storage value
// type baked into a nullable value chunk's element type. A cross product result
// keeps its input chunk's element type, so a crossed property column carries the
// same !storage.nullable<...> the fetch produced, and this maps it back to allocate
// the matching nullable value column and pick the broadcast.
ValueType valueTypeFromElementType(mlir::Type elementType) {
    if (mlir::isa<storage::StringType>(elementType)) {
        return ValueType::String;
    } else if (mlir::isa<storage::EmbeddingType>(elementType)) {
        return ValueType::Embedding;
    } else if (mlir::isa<mlir::Float64Type>(elementType)) {
        return ValueType::Double;
    } else if (const auto intType = mlir::dyn_cast<mlir::IntegerType>(elementType)) {
        const bool isBool = intType.getWidth() == 1;
        const bool isUnsigned = intType.isUnsigned();
        if (isBool) {
            return ValueType::Bool;
        } else if (isUnsigned) {
            return ValueType::UInt64;
        } else {
            return ValueType::Int64;
        }
    } else if (mlir::isa<mlir::NoneType>(elementType)) {
        // A null literal carries no type of its own, and a column has to have one to be
        // laid out: it is carried as a null integer, which every reader sees as the
        // absent value it is - the count that skips it, the key that groups on it
        return ValueType::Int64;
    }

    throw IRException("Unsupported nullable value chunk element type");
}

template <SupportedType T>
typename T::Primitive constantValueAs(mlir::Attribute value) {
    if constexpr (std::same_as<T, types::Int64>) {
        return mlir::cast<mlir::IntegerAttr>(value).getInt();
    } else if constexpr (std::same_as<T, types::UInt64>) {
        return mlir::cast<mlir::IntegerAttr>(value).getValue().getZExtValue();
    } else if constexpr (std::same_as<T, types::Double>) {
        return mlir::cast<mlir::FloatAttr>(value).getValueAsDouble();
    } else if constexpr (std::same_as<T, types::Bool>) {
        return CustomBool(mlir::cast<mlir::IntegerAttr>(value).getInt() != 0);
    } else if constexpr (std::same_as<T, types::String>) {
        return mlir::cast<mlir::StringAttr>(value).getValue();
    } else if constexpr (std::same_as<T, types::Embedding>) {
        const llvm::ArrayRef<float> floats = mlir::cast<mlir::DenseF32ArrayAttr>(value).asArrayRef();
        return std::span<const float> {floats.data(), floats.size()};
    } else {
        throw IRException("Unsupported constant value type");
    }
}

// Coerces a property-value scan's literal to the property's stored type. The analyzer
// admits an integer literal against an Int64, UInt64 or Double property and every other
// literal only against its own type, so those are the pairs bound here; a negative
// integer against a UInt64 property can equal no stored value and stays unbound.
void bindPropertyScanLiteral(NLScanByPropertyValueLoopData& loopData,
                             const PropertyType& propertyType,
                             mlir::TypedAttr literal) {
    const PropertyTypeID id = propertyType._id;
    const ValueType valueType = propertyType._valueType;

    const mlir::IntegerAttr integer = mlir::dyn_cast<mlir::IntegerAttr>(literal);
    const mlir::FloatAttr floating = mlir::dyn_cast<mlir::FloatAttr>(literal);
    const mlir::StringAttr string = mlir::dyn_cast<mlir::StringAttr>(literal);
    const bool isBoolLiteral = integer && integer.getType().isSignlessInteger(1);
    const bool isIntegerLiteral = integer && !isBoolLiteral;

    if (valueType == ValueType::Int64 && isIntegerLiteral) {
        loopData.bind(id, valueType, integer.getInt());
    } else if (valueType == ValueType::UInt64 && isIntegerLiteral && integer.getInt() >= 0) {
        loopData.bind(id, valueType, static_cast<uint64_t>(integer.getInt()));
    } else if (valueType == ValueType::Double && floating) {
        loopData.bind(id, valueType, floating.getValueAsDouble());
    } else if (valueType == ValueType::Double && isIntegerLiteral) {
        loopData.bind(id, valueType, static_cast<double>(integer.getInt()));
    } else if (valueType == ValueType::Bool && isBoolLiteral) {
        loopData.bind(id, valueType, CustomBool(integer.getInt() != 0));
    } else if (valueType == ValueType::String && string) {
        loopData.bind(id, valueType, std::string(string.getValue()));
    }
}

// The runtime reduction the interpreter dispatches on, from the MLIR one the op
// carries. The two enums are kept separate so the runtime (NLProgram / NLExecutor)
// stays free of the MLIR dialect headers; this is the one place they meet.
AggregateKind toRuntimeAggregateKind(storage::AggregateKind kind) {
    switch (kind) {
        case storage::AggregateKind::Sum:
            return AggregateKind::Sum;
        break;

        case storage::AggregateKind::Min:
            return AggregateKind::Min;
        break;

        case storage::AggregateKind::Max:
            return AggregateKind::Max;
        break;

        case storage::AggregateKind::Avg:
            return AggregateKind::Avg;
        break;
    }

    throw IRException("Unhandled aggregate kind");
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
    }

    throw IRException("Unhandled group aggregate kind");
}

// The value type wrapped by a nullable value chunk (!nl.chunk<!storage.nullable<T>>).
// Every chunk carrying scalar values is such a chunk - an aggregate's input and result,
// a property fetch, a homogeneous unwind - so a chunk that is not one (an ID chunk) is
// rejected.
ValueType nullableChunkValueType(mlir::Type chunkType) {
    const auto chunk = mlir::cast<nl::ChunkType>(chunkType);
    const auto nullableType = mlir::dyn_cast<storage::NullableType>(chunk.getElementType());
    if (!nullableType) {
        throw IRException("Expected a nullable value chunk");
    }

    return valueTypeFromElementType(nullableType.getValueType());
}

// Whether a chunk carries the null literal, which has no value type of its own: a
// !nl.chunk<!storage.nullable<none>>
bool isUntypedNullChunk(mlir::Type chunkType) {
    const auto chunk = mlir::dyn_cast<nl::ChunkType>(chunkType);
    if (!chunk) {
        return false;
    }

    const auto nullableType = mlir::dyn_cast<storage::NullableType>(chunk.getElementType());

    return nullableType && mlir::isa<mlir::NoneType>(nullableType.getValueType());
}

// The property value type a chunk writes, which is not quite the shape it holds: a
// column that owns its strings - a loaded CSV field - writes a String property like a
// borrowed one, so it is recognised here and not in valueTypeFromElementType, which
// answers what nullable value column a chunk needs.
ValueType valueTypeFromChunkType(mlir::Type chunkType) {
    const auto chunk = mlir::cast<nl::ChunkType>(chunkType);
    const mlir::Type elementType = chunk.getElementType();

    if (const auto nullableType = mlir::dyn_cast<storage::NullableType>(elementType)) {
        return valueTypeFromElementType(nullableType.getValueType());
    }

    if (mlir::isa<storage::OwnedStringType>(elementType)) {
        return ValueType::String;
    }

    return valueTypeFromElementType(elementType);
}

bool isConstantColumn(const Column* column) {
    return column->getContainerKind() == ContainerKind::code<ColumnConst>();
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
                           const ProcedureContext* procedureContext)
    : _program(program),
    _memory(memory),
    _view(view),
    _metadataBuilder(metadataBuilder),
    _procedureContext(procedureContext),
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

    translateBlock(bodyRegion.front(), _program->getStmts());
}

void NLTranslator::translateBlock(mlir::Block& block, NLStmtContainer* body) {
    for (mlir::Operation& operation : block) {
        if (nl::ScanNodes scanNodes = mlir::dyn_cast<nl::ScanNodes>(operation)) {
            _iteratorConfigs[scanNodes.getResult()] = IteratorConfig {IteratorKind::ScanNodes, {}, {}};
        } else if (nl::ScanNodesByLabel scanNodesByLabel = mlir::dyn_cast<nl::ScanNodesByLabel>(operation)) {
            IteratorConfig config {IteratorKind::ScanNodesByLabel, {}, {}};
            for (const mlir::Attribute label : scanNodesByLabel.getLabels()) {
                config._labels.emplace_back(mlir::cast<mlir::StringAttr>(label).getValue());
            }
            _iteratorConfigs[scanNodesByLabel.getResult()] = config;
        } else if (nl::ConstScanNodes constScanNodes = mlir::dyn_cast<nl::ConstScanNodes>(operation)) {
            IteratorConfig config {IteratorKind::ConstScanNodes, {}, {}};
            config._nodeIDs = constScanNodes.getNodeIDs();
            _iteratorConfigs[constScanNodes.getResult()] = config;
        } else if (nl::ScanNodesByPropertyValue scanByValue = mlir::dyn_cast<nl::ScanNodesByPropertyValue>(operation)) {
            IteratorConfig config {IteratorKind::ScanNodesByPropertyValue, {}, {}};
            config._property = scanByValue.getProperty();
            config._propertyValue = scanByValue.getValue();
            if (const std::optional<mlir::ArrayAttr> labels = scanByValue.getLabels()) {
                for (const mlir::Attribute label : *labels) {
                    config._labels.emplace_back(mlir::cast<mlir::StringAttr>(label).getValue());
                }
            }
            _iteratorConfigs[scanByValue.getResult()] = config;
        } else if (nl::ScanEdges scanEdges = mlir::dyn_cast<nl::ScanEdges>(operation)) {
            _iteratorConfigs[scanEdges.getResult()] = IteratorConfig {IteratorKind::ScanEdges, {}, {}};
        } else if (nl::ScanEdgesByType scanEdgesByType = mlir::dyn_cast<nl::ScanEdgesByType>(operation)) {
            IteratorConfig config {IteratorKind::ScanEdgesByType, {}, {}};
            config._edgeType = edgeTypeName(scanEdgesByType.getEdgeType());
            _iteratorConfigs[scanEdgesByType.getResult()] = config;
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
            config._edgeType = edgeTypeName(getOutEdgesByType.getEdgeType());
            _iteratorConfigs[getOutEdgesByType.getResult()] = config;
        } else if (nl::GetInEdgesByType getInEdgesByType = mlir::dyn_cast<nl::GetInEdgesByType>(operation)) {
            IteratorConfig config {IteratorKind::GetInEdgesByType, getInEdgesByType.getInputNodes(), {}};
            const mlir::OperandRange carriedColumns = getInEdgesByType.getColumnsToFilter();
            config._carriedColumns.assign(carriedColumns.begin(), carriedColumns.end());
            config._edgeType = edgeTypeName(getInEdgesByType.getEdgeType());
            _iteratorConfigs[getInEdgesByType.getResult()] = config;
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
        } else if (nl::UnwindConst unwindConst = mlir::dyn_cast<nl::UnwindConst>(operation)) {
            IteratorConfig config;
            config._kind = IteratorKind::UnwindConst;
            config._list = materializeListView(unwindConst.getElements());
            _iteratorConfigs[unwindConst.getResult()] = config;
        } else if (nl::LoadCSV loadCSV = mlir::dyn_cast<nl::LoadCSV>(operation)) {
            IteratorConfig config;
            config._kind = IteratorKind::LoadCSV;
            config._csvPath = loadCSV.getPath();
            config._csvFields = loadCSV.getFieldsAttr();
            config._csvHasHeaders = loadCSV.getWithHeaders();
            config._csvSkipOnError = loadCSV.getSkipOnError();
            _iteratorConfigs[loadCSV.getResult()] = config;
        } else if (nl::VectorSearch vectorSearch = mlir::dyn_cast<nl::VectorSearch>(operation)) {
            IteratorConfig config;
            config._kind = IteratorKind::VectorSearch;
            config._indexName = vectorSearch.getIndexName();
            config._neighbourCount = vectorSearch.getK();
            config._queryVector = vectorSearch.getQueryVector();
            _iteratorConfigs[vectorSearch.getResult()] = config;
        } else if (nl::Unwind unwind = mlir::dyn_cast<nl::Unwind>(operation)) {
            IteratorConfig config;
            config._kind = IteratorKind::Unwind;
            config._source = unwind.getSource();

            const mlir::OperandRange carriedColumns = unwind.getColumnsToFilter();
            config._carriedColumns.assign(carriedColumns.begin(), carriedColumns.end());

            _iteratorConfigs[unwind.getResult()] = config;
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
        } else if (mlir::isa<nl::GetPropertyType, nl::GetEdgeType>(operation)) {
            // The handle carries only a name; a fetch/hop resolves it on consumption
        } else if (nl::Constant constant = mlir::dyn_cast<nl::Constant>(operation)) {
            translateConstant(constant);
        } else if (nl::BroadcastConstant broadcast = mlir::dyn_cast<nl::BroadcastConstant>(operation)) {
            translateBroadcastConstant(broadcast, body);
        } else if (nl::Add add = mlir::dyn_cast<nl::Add>(operation)) {
            translateBinaryOp<OP_ADD>(add, body);
        } else if (nl::Concat concat = mlir::dyn_cast<nl::Concat>(operation)) {
            translateBinaryOp<OP_CONCAT>(concat, body);
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
        } else if (lookupUnaryFunctionSelector(operation)) {
            translateUnaryFunction(&operation, body);
        } else if (lookupBinaryFunctionSelector(operation)) {
            translateBinaryFunction(&operation, body);
        } else if (nl::Filter filter = mlir::dyn_cast<nl::Filter>(operation)) {
            translateFilter(filter, body);
        } else if (nl::GetNodeProperties getNodeProperties = mlir::dyn_cast<nl::GetNodeProperties>(operation)) {
            translatePropertyFetch(getNodeProperties.getInputNodes(),
                                   getNodeProperties.getPropertyType(),
                                   getNodeProperties.getValues(),
                                   /*isNode=*/true,
                                   body);
        } else if (nl::GetEdgeProperties getEdgeProperties = mlir::dyn_cast<nl::GetEdgeProperties>(operation)) {
            translatePropertyFetch(getEdgeProperties.getInputEdges(),
                                   getEdgeProperties.getPropertyType(),
                                   getEdgeProperties.getValues(),
                                   /*isNode=*/false,
                                   body);
        } else if (nl::GetNodeLabelSet getNodeLabelSet = mlir::dyn_cast<nl::GetNodeLabelSet>(operation)) {
            translateGetNodeLabelSet(getNodeLabelSet, body);
        } else if (nl::CheckLabelConstraint checkLabelConstraint = mlir::dyn_cast<nl::CheckLabelConstraint>(operation)) {
            translateCheckLabelConstraint(checkLabelConstraint, body);
        } else if (nl::CheckEdgeTypeConstraint checkEdgeTypeConstraint = mlir::dyn_cast<nl::CheckEdgeTypeConstraint>(operation)) {
            translateCheckEdgeTypeConstraint(checkEdgeTypeConstraint, body);
        } else if (nl::CrossProduct crossProduct = mlir::dyn_cast<nl::CrossProduct>(operation)) {
            translateCrossProduct(crossProduct, body);
        } else if (nl::Limit limit = mlir::dyn_cast<nl::Limit>(operation)) {
            translateLimit(limit, body);
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
        } else if (nl::SortCollect sortCollect = mlir::dyn_cast<nl::SortCollect>(operation)) {
            translateSortCollect(sortCollect, body);
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
        } else if (nl::CreateNode createNode = mlir::dyn_cast<nl::CreateNode>(operation)) {
            translateCreateNode(createNode, body);
        } else if (nl::CreateEdge createEdge = mlir::dyn_cast<nl::CreateEdge>(operation)) {
            translateCreateEdge(createEdge, body);
        } else if (nl::SetNodeProperty setNodeProperty = mlir::dyn_cast<nl::SetNodeProperty>(operation)) {
            translateSetNodeProperty(setNodeProperty, body);
        } else if (nl::SetEdgeProperty setEdgeProperty = mlir::dyn_cast<nl::SetEdgeProperty>(operation)) {
            translateSetEdgeProperty(setEdgeProperty, body);
        } else if (nl::DeleteNode deleteNode = mlir::dyn_cast<nl::DeleteNode>(operation)) {
            translateDeleteNode(deleteNode, body);
        } else if (nl::DeleteEdge deleteEdge = mlir::dyn_cast<nl::DeleteEdge>(operation)) {
            translateDeleteEdge(deleteEdge, body);
        } else if (nl::Procedure procedureOp = mlir::dyn_cast<nl::Procedure>(operation)) {
            translateProcedure(procedureOp, body);
        } else if (nl::Output output = mlir::dyn_cast<nl::Output>(operation)) {
            translateOutput(output, body);
        } else if (mlir::isa<nl::Yield, mlir::func::ReturnOp>(operation)) {
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
    } else if (config._kind == IteratorKind::ScanEdgesByType) {
        translateScanEdgesByTypeLoop(config, loopBody, limit, body);
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
    } else if (config._kind == IteratorKind::ProcedureInit) {
        translateProcedureInitLoop(config, loopBody, limit, body);
    } else {
        translateEdgeLoop(config, loopBody, limit, body);
    }
}

void NLTranslator::translateScanLoop(mlir::Block& loopBody, NLLimitState* limit, NLStmtContainer* body) {
    // For::verify guarantees one block argument per iterator chunk, and a
    // node scan iterator has exactly one chunk of node IDs
    ColumnNodeIDs* nodeIDs = static_cast<ColumnNodeIDs*>(allocColumn(loopBody.getArgument(0)));

    NLScanLoopData* loopData = _program->allocFunctionData<NLScanLoopData>(nodeIDs);
    loopData->setLimit(limit);

    body->emplaceStmt(&NLExecutor::runScanNodesLoop, loopData);

    translateBlock(loopBody, loopData->getStmts());
}

void NLTranslator::translateScanByLabelLoop(const IteratorConfig& config,
                                            mlir::Block& loopBody,
                                            NLLimitState* limit,
                                            NLStmtContainer* body) {
    // A label scan binds the same single node ID chunk as a plain scan.
    const mlir::Value nodeChunk = loopBody.getArgument(0);
    ColumnNodeIDs* nodeIDs = static_cast<ColumnNodeIDs*>(allocColumn(nodeChunk));

    // An unmatchable scan emits nothing rather than dropping the absent name and
    // matching a weaker set.
    LabelSet labelset;
    const bool matchable = resolveLabelSet(config._labels, labelset);

    NLScanByLabelLoopData* loopData = _program->allocFunctionData<NLScanByLabelLoopData>(nodeIDs, labelset, matchable);
    loopData->setLimit(limit);

    body->emplaceStmt(&NLExecutor::runScanNodesByLabelLoop, loopData);

    translateBlock(loopBody, loopData->getStmts());
}

void NLTranslator::translateConstScanLoop(const IteratorConfig& config,
                                          mlir::Block& loopBody,
                                          NLLimitState* limit,
                                          NLStmtContainer* body) {
    // A const scan binds the same single node ID chunk as a plain scan.
    const mlir::Value nodeChunk = loopBody.getArgument(0);
    ColumnNodeIDs* nodeIDs = static_cast<ColumnNodeIDs*>(allocColumn(nodeChunk));

    NLConstScanLoopData* loopData = _program->allocFunctionData<NLConstScanLoopData>(nodeIDs);
    loopData->setLimit(limit);

    const GraphReader reader = _view->read();

    loopData->reserveNodeIDs(config._nodeIDs.size());
    for (const int64_t nodeID : config._nodeIDs) {
        const NodeID candidate(static_cast<uint64_t>(nodeID));
        if (reader.graphHasNode(candidate)) {
            loopData->addNodeID(candidate);
        }
    }

    body->emplaceStmt(&NLExecutor::runConstScanNodesLoop, loopData);

    translateBlock(loopBody, loopData->getStmts());
}

void NLTranslator::translateScanByPropertyValueLoop(const IteratorConfig& config,
                                                    mlir::Block& loopBody,
                                                    NLLimitState* limit,
                                                    NLStmtContainer* body) {
    const mlir::Value nodeChunk = loopBody.getArgument(0);
    ColumnNodeIDs* nodeIDs = static_cast<ColumnNodeIDs*>(allocColumn(nodeChunk));

    NLScanByPropertyValueLoopData* loopData = _program->allocFunctionData<NLScanByPropertyValueLoopData>(nodeIDs);
    loopData->setLimit(limit);

    bool matchable = true;
    if (!config._labels.empty()) {
        LabelSet labelset;
        matchable = resolveLabelSet(config._labels, labelset);
        loopData->setLabelSet(labelset);
    }

    const llvm::StringRef property = config._property;
    const std::optional<PropertyType> propertyType = _view->metadata().propTypes().get(std::string_view(property.data(), property.size()));
    if (matchable && propertyType) {
        bindPropertyScanLiteral(*loopData, *propertyType, config._propertyValue);
    }

    body->emplaceStmt(&NLExecutor::runScanNodesByPropertyValueLoop, loopData);

    translateBlock(loopBody, loopData->getStmts());
}

bool NLTranslator::resolveLabelSet(llvm::ArrayRef<llvm::StringRef> labels, LabelSet& labelset) const {
    const LabelMap& labelMap = _view->metadata().labels();

    for (const llvm::StringRef label : labels) {
        const std::optional<LabelID> id = labelMap.get(label);
        if (!id) {
            return false;
        }

        labelset.set(*id);
    }

    return true;
}

void NLTranslator::translateUnwindConstLoop(const IteratorConfig& config,
                                            mlir::Block& loopBody,
                                            NLLimitState* limit,
                                            NLStmtContainer* body) {
    // An unwind_const binds a single value chunk: the loop emits one element per row.
    const mlir::Value valueChunk = loopBody.getArgument(0);
    const mlir::Type elementType = mlir::cast<nl::ChunkType>(valueChunk.getType()).getElementType();

    // The chunk element type is the homogeneity verdict: a list_element chunk is the
    // type-erased column of tagged scalars; a nullable value chunk is a homogeneous
    // column of that one value type, the shape every other value-chunk consumer reads.
    const bool heterogeneous = llvm::isa<storage::ListElementType>(elementType);

    Column* output {nullptr};
    ValueType valueType {ValueType::Invalid};

    if (heterogeneous) {
        output = allocListElementColumn();
    } else {
        valueType = nullableChunkValueType(valueChunk.getType());
        output = allocOptColumnForValueType(valueType);
    }

    _valueSlots[valueChunk] = output;

    NLUnwindConstLoopData* loopData = _program->allocFunctionData<NLUnwindConstLoopData>(output,
                                                                                        config._list,
                                                                                        heterogeneous,
                                                                                        valueType);
    loopData->setLimit(limit);

    body->emplaceStmt(&NLExecutor::runUnwindConstLoop, loopData);

    translateBlock(loopBody, loopData->getStmts());
}

void NLTranslator::translateLoadCSVLoop(const IteratorConfig& config,
                                        mlir::Block& loopBody,
                                        NLLimitState* limit,
                                        NLStmtContainer* body) {
    // A CSV load binds one owning string chunk per field it produces. The parser fills a
    // row of field columns, so the chunks are gathered into one - the chunk a downstream
    // op reads is the very column the parser wrote.
    ColumnStringTable* const row = _memory->alloc<ColumnStringTable>();

    for (const mlir::Value fieldChunk : loopBody.getArguments()) {
        Column* const field = allocColumn(fieldChunk);
        row->addFieldColumn(static_cast<ColumnStringTable::StringColumn*>(field));
    }

    const std::string_view path {config._csvPath.data(), config._csvPath.size()};

    NLLoadCSVLoopData* loopData = _program->allocFunctionData<NLLoadCSVLoopData>(row,
                                                                                path,
                                                                                config._csvHasHeaders,
                                                                                config._csvSkipOnError);
    loopData->setLimit(limit);

    for (const mlir::Attribute field : config._csvFields) {
        if (const auto header = mlir::dyn_cast<mlir::StringAttr>(field)) {
            const llvm::StringRef name = header.getValue();
            loopData->addField({._header = std::string_view {name.data(), name.size()},
                                ._byHeader = true});
        } else {
            const auto index = mlir::cast<mlir::IntegerAttr>(field);
            loopData->addField({._index = index.getValue().getZExtValue()});
        }
    }

    body->emplaceStmt(&NLExecutor::runLoadCSVLoop, loopData);

    translateBlock(loopBody, loopData->getStmts());
}

void NLTranslator::translateVectorSearchLoop(const IteratorConfig& config,
                                             mlir::Block& loopBody,
                                             NLLimitState* limit,
                                             NLStmtContainer* body) {
    // A vector search binds two chunks: the neighbour nodes and the distances they scored.
    const mlir::Value idChunk = loopBody.getArgument(0);
    const mlir::Value scoreChunk = loopBody.getArgument(1);

    ColumnNodeIDs* const ids = static_cast<ColumnNodeIDs*>(allocColumn(idChunk));
    Column* const scores = allocColumn(scoreChunk);

    const std::string_view indexName {config._indexName.data(), config._indexName.size()};
    const std::span<const float> queryVector {config._queryVector.data(), config._queryVector.size()};

    NLVectorSearchLoopData* loopData = _program->allocFunctionData<NLVectorSearchLoopData>(ids,
                                                                                          scores,
                                                                                          indexName,
                                                                                          config._neighbourCount,
                                                                                          queryVector);
    loopData->setLimit(limit);

    body->emplaceStmt(&NLExecutor::runVectorSearchLoop, loopData);

    translateBlock(loopBody, loopData->getStmts());
}

NLUnwindElementEmitFunction NLTranslator::selectListUnwindEmit(mlir::Type chunkType) {
    const mlir::Type elementType = mlir::cast<nl::ChunkType>(chunkType).getElementType();

    if (const auto nullableType = mlir::dyn_cast<storage::NullableType>(elementType)) {
        return NLExecutor::selectListUnwindValueEmit(valueTypeFromElementType(nullableType.getValueType()));
    } else if (mlir::isa<storage::NodeIDType>(elementType)) {
        return NLExecutor::selectListUnwindNodeEmit();
    } else if (mlir::isa<storage::EdgeIDType>(elementType)) {
        return NLExecutor::selectListUnwindEdgeEmit();
    } else if (mlir::isa<storage::ListType>(elementType)) {
        return NLExecutor::selectListUnwindListEmit();
    }

    return NLExecutor::selectListUnwindElementEmit();
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

    // A column whose cells hold more than the element - a list, or a tagged scalar that
    // may itself be a list - drains through its own emit. A scalar column already holds
    // one element per cell, so its element column is the source's own rows gathered by
    // the step: a carried column like the rest, and no drain here.
    NLUnwindElementCountFunction elementCount = nullptr;
    NLUnwindElementEmitFunction elementEmit = nullptr;

    if (llvm::isa<storage::ListType>(sourceElement)) {
        elementCount = NLExecutor::selectListUnwindElementCount();
        elementEmit = selectListUnwindEmit(elementValue.getType());
    } else if (llvm::isa<storage::ListElementType>(sourceElement)) {
        elementCount = NLExecutor::selectTaggedUnwindElementCount();
        elementEmit = NLExecutor::selectTaggedUnwindElementEmit();
    } else if (const auto nullableType = mlir::dyn_cast<storage::NullableType>(sourceElement)) {
        const ValueType valueType = valueTypeFromElementType(nullableType.getValueType());
        elementCount = NLExecutor::selectOptUnwindElementCount(valueType);
    } else {
        elementCount = NLExecutor::selectValueUnwindElementCount();
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

    if (!elementEmit) {
        const NLCarriedColumn elementColumn(source,
                                            allocColumn(elementValue),
                                            selectGatherForChunkType(sourceChunk));
        loopData->addCarriedColumn(elementColumn);
    }

    const size_t carriedCount = config._carriedColumns.size();
    for (size_t carriedIndex = 0; carriedIndex < carriedCount; carriedIndex++) {
        const mlir::Value carriedValue = config._carriedColumns[carriedIndex];
        const mlir::Value loopVariable = loopBody.getArgument(static_cast<unsigned>(1 + carriedIndex));

        const NLCarriedColumn carriedColumn(getColumn(carriedValue),
                                            allocColumn(loopVariable),
                                            selectGatherForChunkType(loopVariable.getType()));
        loopData->addCarriedColumn(carriedColumn);
    }

    body->emplaceStmt(&NLExecutor::runUnwindLoop, loopData);

    translateBlock(loopBody, loopData->getStmts());
}

// The bytes the values of a literal list's elements occupy, tag bytes excluded - what
// ListBuffer::reserveList sizes the region from. Each element stores the same value object
// the write below hands it, so the two walks must recognise the same attribute kinds in the
// same order.
size_t NLTranslator::listValueBytes(mlir::ArrayAttr elements) {
    size_t valueBytes = 0;

    for (const mlir::Attribute element : elements) {
        // BoolAttr is an i1 IntegerAttr, so it must be tested before IntegerAttr; an
        // i64 literal falls through to the integer case.
        if (mlir::isa<mlir::BoolAttr>(element)) {
            valueBytes += sizeof(types::Bool::Primitive);
        } else if (mlir::isa<mlir::IntegerAttr>(element)) {
            valueBytes += sizeof(types::Int64::Primitive);
        } else if (mlir::isa<mlir::FloatAttr>(element)) {
            valueBytes += sizeof(types::Double::Primitive);
        } else if (mlir::isa<mlir::StringAttr>(element)) {
            valueBytes += sizeof(types::String::Primitive);
        } else if (mlir::isa<mlir::DenseF32ArrayAttr>(element)) {
            valueBytes += sizeof(types::Embedding::Primitive);
        } else if (mlir::isa<mlir::UnitAttr>(element)) {
            valueBytes += sizeof(PropertyNull);
        } else if (mlir::isa<mlir::ArrayAttr>(element)) {
            // A nested list is one element of this one, storing the child's view
            valueBytes += sizeof(ListView);
        } else {
            throw IRException("Unsupported literal attribute in a constant list");
        }
    }

    return valueBytes;
}

ListView NLTranslator::materializeListView(mlir::ArrayAttr elements) {
    // The region is reserved and committed up front, so the elements are written straight
    // into their final place - no staging container between the attributes and the buffer.
    // A later reservation lands after this region rather than inside it, which is what lets
    // a nested list be materialized part-way through filling its parent.
    ListWriteCursor cursor = _memory->listBuffer().reserveList(elements.size(),
                                                              listValueBytes(elements));

    for (const mlir::Attribute element : elements) {
        if (const auto boolAttr = mlir::dyn_cast<mlir::BoolAttr>(element)) {
            cursor.writeValue(ListBufferTypeTag::Bool, types::Bool::Primitive(boolAttr.getValue()));
        } else if (const auto intAttr = mlir::dyn_cast<mlir::IntegerAttr>(element)) {
            cursor.writeValue(ListBufferTypeTag::Int,
                              static_cast<types::Int64::Primitive>(intAttr.getInt()));
        } else if (const auto floatAttr = mlir::dyn_cast<mlir::FloatAttr>(element)) {
            cursor.writeValue(ListBufferTypeTag::Double,
                              static_cast<types::Double::Primitive>(floatAttr.getValueAsDouble()));
        } else if (const auto stringAttr = mlir::dyn_cast<mlir::StringAttr>(element)) {
            // The payload stays in the attribute, which outlives the query, so the stored
            // view points at it rather than at a copy
            const llvm::StringRef value = stringAttr.getValue();
            cursor.writeValue(ListBufferTypeTag::String,
                              types::String::Primitive(value.data(), value.size()));
        } else if (const auto embeddingAttr = mlir::dyn_cast<mlir::DenseF32ArrayAttr>(element)) {
            // The floats stay in the attribute, as a string element's bytes do, so the
            // stored span points at them rather than at a copy
            const llvm::ArrayRef<float> floats = embeddingAttr.asArrayRef();
            cursor.writeValue(ListBufferTypeTag::Embedding,
                              types::Embedding::Primitive(floats.data(), floats.size()));
        } else if (mlir::isa<mlir::UnitAttr>(element)) {
            cursor.writeValue(ListBufferTypeTag::Null, PropertyNull {});
        } else if (const auto nestedAttr = mlir::dyn_cast<mlir::ArrayAttr>(element)) {
            cursor.writeValue(ListBufferTypeTag::ListView, materializeListView(nestedAttr));
        } else {
            throw IRException("Unsupported literal attribute in a constant list");
        }
    }

    return cursor.getView();
}

void NLTranslator::translateScanEdgesLoop(mlir::Block& loopBody, NLLimitState* limit, NLStmtContainer* body) {
    // For::verify guarantees one block argument per iterator chunk, and an edge
    // scan iterator has exactly four chunks in the order getEdgeIteratorType
    // establishes: sources, edge IDs, edge type IDs, targets.
    ColumnNodeIDs* sources = static_cast<ColumnNodeIDs*>(allocColumn(loopBody.getArgument(0)));
    ColumnEdgeIDs* edgeIDs = static_cast<ColumnEdgeIDs*>(allocColumn(loopBody.getArgument(1)));
    ColumnEdgeTypes* edgeTypes = static_cast<ColumnEdgeTypes*>(allocColumn(loopBody.getArgument(2)));
    ColumnNodeIDs* targets = static_cast<ColumnNodeIDs*>(allocColumn(loopBody.getArgument(3)));

    NLScanEdgesLoopData* loopData = _program->allocFunctionData<NLScanEdgesLoopData>(sources,
                                                                                     edgeIDs,
                                                                                     edgeTypes,
                                                                                     targets);
    loopData->setLimit(limit);

    body->addStmt(NLFunctionDescriptor {&NLExecutor::runScanEdgesLoop, loopData});

    translateBlock(loopBody, loopData->getStmts());
}

void NLTranslator::translateScanEdgesByTypeLoop(const IteratorConfig& config,
                                                mlir::Block& loopBody,
                                                NLLimitState* limit,
                                                NLStmtContainer* body) {
    ColumnNodeIDs* sources = static_cast<ColumnNodeIDs*>(allocColumn(loopBody.getArgument(0)));
    ColumnEdgeIDs* edgeIDs = static_cast<ColumnEdgeIDs*>(allocColumn(loopBody.getArgument(1)));
    ColumnEdgeTypes* edgeTypes = static_cast<ColumnEdgeTypes*>(allocColumn(loopBody.getArgument(2)));
    ColumnNodeIDs* targets = static_cast<ColumnNodeIDs*>(allocColumn(loopBody.getArgument(3)));

    // Resolve the edge type name against the schema, exactly as translateEdgeLoop
    // does for a by-type hop. A name absent from the schema matches no edge, so the
    // loop is marked unmatchable and emits nothing rather than scanning for a bogus
    // type.
    const std::optional<EdgeTypeID> edgeTypeID = _view->metadata().edgeTypes().get(config._edgeType);
    const bool matchable = edgeTypeID.has_value();
    const EdgeTypeID resolvedType = matchable ? *edgeTypeID : EdgeTypeID();

    NLScanEdgesByTypeLoopData* loopData =
        _program->allocFunctionData<NLScanEdgesByTypeLoopData>(sources,
                                                               edgeIDs,
                                                               edgeTypes,
                                                               targets,
                                                               resolvedType,
                                                               matchable);
    loopData->setLimit(limit);

    body->addStmt(NLFunctionDescriptor {&NLExecutor::runScanEdgesByTypeLoop, loopData});

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
    if (!edgeIDs && _view->tombstones().hasEdges()) {
        edgeIDs = _memory->alloc<ColumnEdgeIDs>();
        edgeIDs->reserve(_program->getChunkSize());
    }

    const ColumnNodeIDs* inputNodeIDs = static_cast<const ColumnNodeIDs*>(getColumn(config._inputNodes));

    const bool byType = config._kind == IteratorKind::GetOutEdgesByType
                        || config._kind == IteratorKind::GetInEdgesByType;

    // A by-type hop carries the resolved edge type in an NLEdgeByTypeLoopData; a
    // plain hop uses the base NLEdgeLoopData. The shared driver below (reserve,
    // carried columns, body) works through the base pointer either way.
    NLEdgeLoopData* loopData = nullptr;
    if (byType) {
        // Resolve the edge type name against the schema, exactly as
        // translateScanByLabelLoop resolves its labels. A name absent from the
        // schema matches no edge, so the loop is marked unmatchable and emits
        // nothing rather than filtering against a bogus type.
        const std::optional<EdgeTypeID> edgeTypeID = _view->metadata().edgeTypes().get(config._edgeType);
        const bool matchable = edgeTypeID.has_value();
        const EdgeTypeID resolvedType = matchable ? *edgeTypeID : EdgeTypeID();

        loopData = _program->allocFunctionData<NLEdgeByTypeLoopData>(inputNodeIDs,
                                                                     sources,
                                                                     edgeIDs,
                                                                     edgeTypes,
                                                                     targets,
                                                                     resolvedType,
                                                                     matchable);
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

    // Allocate carried columns in the carried set
    const size_t carriedCount = config._carriedColumns.size();
    for (size_t carriedIndex = 0; carriedIndex < carriedCount; carriedIndex++) {
        const mlir::Value carriedValue = config._carriedColumns[carriedIndex];

        if (!rowAlignedWith(carriedValue, config._inputNodes)) {
            throw IRException("Carried column is not row-aligned with the input chunk");
        }

        Column* carriedOutput = allocColumn(loopBody.getArgument(static_cast<unsigned>(4 + carriedIndex)));

        const NLCarriedColumn carriedColumn(getColumn(carriedValue),
                                            carriedOutput,
                                            selectGatherForChunkType(carriedValue.getType()));
        loopData->addCarriedColumn(carriedColumn);
    }

    NLHandlerFunction handler = nullptr;
    if (config._kind == IteratorKind::GetOutEdges) {
        handler = &NLExecutor::runGetOutEdgesLoop;
    } else if (config._kind == IteratorKind::GetInEdges) {
        handler = &NLExecutor::runGetInEdgesLoop;
    } else if (config._kind == IteratorKind::GetEdges) {
        handler = &NLExecutor::runGetEdgesLoop;
    } else if (config._kind == IteratorKind::GetOutEdgesByType) {
        handler = &NLExecutor::runGetOutEdgesByTypeLoop;
    } else {
        handler = &NLExecutor::runGetInEdgesByTypeLoop;
    }
    body->emplaceStmt(handler, loopData);

    translateBlock(loopBody, loopData->getStmts());
}

void NLTranslator::translatePropertyFetch(mlir::Value inputValue,
                                          mlir::Value propertyTypeValue,
                                          mlir::Value resultValue,
                                          bool isNode,
                                          NLStmtContainer* body) {
    // The property name lives on the nl.get_property_type that produced the handle
    nl::GetPropertyType handleOp = propertyTypeValue.getDefiningOp<nl::GetPropertyType>();
    if (!handleOp) {
        throw IRException("property_type operand must come from nl.get_property_type");
    }

    const llvm::StringRef name = handleOp.getName();

    // Resolve the name against the schema once, here, so execution works from a
    // PropertyTypeID and value type and never sees the name again
    const std::optional<PropertyType> propertyType = _view->metadata().propTypes().get(std::string_view(name.data(), name.size()));
    if (!propertyType) {
        throw IRException("Unknown property '" + name.str() + "'");
    }

    const ValueType valueType = propertyType->_valueType;

    const Column* input = getColumn(inputValue);
    Column* output = allocOptColumnForValueType(valueType);
    _valueSlots[resultValue] = output;

    NLPropertyFetchData* fetchData = _program->allocFunctionData<NLPropertyFetchData>(input,
                                                                                      output,
                                                                                      propertyType->_id);

    const NLHandlerFunction handler = selectPropertyFetchHandler(isNode, valueType);
    body->emplaceStmt(handler, fetchData);
}

void NLTranslator::translateGetNodeLabelSet(nl::GetNodeLabelSet op, NLStmtContainer* body) {
    const mlir::TypedValue<::mlir::nl::ChunkType> nodes = op.getInputNodes();
    const Column* nodeCol = getColumn(nodes);
    const ColumnNodeIDs* input = static_cast<const ColumnNodeIDs*>(nodeCol);

    ColumnLabelSetIDs* output = _memory->alloc<ColumnLabelSetIDs>();
    output->reserve(_program->getChunkSize());
    _valueSlots[op.getLabelSetIds()] = output;

    NLGetNodeLabelSetData* data = _program->allocFunctionData<NLGetNodeLabelSetData>(input, output);
    body->emplaceStmt(&NLExecutor::runGetNodeLabelSet, data);
}

void NLTranslator::translateCheckLabelConstraint(nl::CheckLabelConstraint op, NLStmtContainer* body) {
    const mlir::TypedValue<::mlir::nl::ChunkType> lbls = op.getLabelsetIds();
    const Column* lblsCol = getColumn(lbls);
    const ColumnLabelSetIDs* input = static_cast<const ColumnLabelSetIDs*>(lblsCol);

    ColumnMask* output = _memory->alloc<ColumnMask>();
    output->reserve(_program->getChunkSize());
    _valueSlots[op.getResult()] = output;

    NLCheckLabelConstraintData* data = _program->allocFunctionData<NLCheckLabelConstraintData>(input, output);
    for (const int64_t rawID : op.getMatchingIds()) {
        data->addMatchingID(LabelSetID(static_cast<uint32_t>(rawID)));
    }

    body->emplaceStmt(&NLExecutor::runCheckLabelConstraint, data);
}

void NLTranslator::translateCheckEdgeTypeConstraint(nl::CheckEdgeTypeConstraint op, NLStmtContainer* body) {
    const mlir::TypedValue<::mlir::nl::ChunkType> etypes = op.getEdgeTypeIds();
    const Column* etypesCol = getColumn(etypes);
    const ColumnEdgeTypes* input = static_cast<const ColumnEdgeTypes*>(etypesCol);

    ColumnMask* output = _memory->alloc<ColumnMask>();
    output->reserve(_program->getChunkSize());
    _valueSlots[op.getResult()] = output;

    NLCheckEdgeTypeConstraintData* data = _program->allocFunctionData<NLCheckEdgeTypeConstraintData>(input, output);
    for (const int64_t rawID : op.getMatchingIds()) {
        data->addMatchingID(EdgeTypeID(static_cast<uint64_t>(rawID)));
    }

    body->emplaceStmt(&NLExecutor::runCheckEdgeTypeConstraint, data);
}

void NLTranslator::translateCreateNode(nl::CreateNode createNode, NLStmtContainer* body) {
    if (!_metadataBuilder) {
        throw IRException("nl.create_node requires a MetadataBuilder (write transaction)");
    }

    LabelSet labelset;
    for (const mlir::Attribute attr : createNode.getLabels()) {
        const llvm::StringRef labelName = mlir::cast<mlir::StringAttr>(attr).getValue();
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
        const mlir::Value propValue = propValues[propIndex];
        const ValueType valueType = valueTypeFromChunkType(propValue.getType());

        const PropertyType propType = _metadataBuilder->getOrCreatePropertyType(propName, valueType);
        const Column* propColumn = getColumn(propValue);

        data->addProperty({._propertyTypeID=propType._id, ._values=propColumn});
    }

    if (const mlir::Value cardinality = createNode.getCardinality()) {
        data->setCardinality(getColumn(cardinality));
    }

    body->emplaceStmt(&NLExecutor::runCreateNode, data);
}

void NLTranslator::translateCreateEdge(nl::CreateEdge createEdge, NLStmtContainer* body) {
    if (!_metadataBuilder) {
        throw IRException("nl.create_edge requires a MetadataBuilder (write transaction)");
    }

    const llvm::StringRef edgeTypeName = createEdge.getEdgeType();
    const EdgeTypeID edgeTypeID = _metadataBuilder->getOrCreateEdgeType(edgeTypeName);

    const mlir::Value srcValue = createEdge.getSrcIds();
    const mlir::Value tgtValue = createEdge.getTgtIds();
    const bool srcIsPending = _pendingNodeValues.contains(srcValue);
    const bool tgtIsPending = _pendingNodeValues.contains(tgtValue);

    const ColumnNodeIDs* srcColumn = static_cast<const ColumnNodeIDs*>(getColumn(srcValue));
    const ColumnNodeIDs* tgtColumn = static_cast<const ColumnNodeIDs*>(getColumn(tgtValue));

    ColumnEdgeIDs* result = _memory->alloc<ColumnEdgeIDs>();
    _valueSlots[createEdge.getResult()] = result;

    NLCreateEdgeData* data = _program->allocFunctionData<NLCreateEdgeData>(
        edgeTypeID,
        srcColumn,
        srcIsPending,
        tgtColumn,
        tgtIsPending,
        result);

    const mlir::OperandRange propValues = createEdge.getPropValues();
    const mlir::ArrayAttr propNames = createEdge.getPropNames();

    for (size_t propIndex = 0; propIndex < propNames.size(); propIndex++) {
        const llvm::StringRef propName = mlir::cast<mlir::StringAttr>(propNames[propIndex]).getValue();
        const mlir::Value propValue = propValues[propIndex];
        const ValueType valueType = valueTypeFromChunkType(propValue.getType());

        const PropertyType propType = _metadataBuilder->getOrCreatePropertyType(propName, valueType);
        const Column* propColumn = getColumn(propValue);

        data->addProperty({._propertyTypeID=propType._id, ._values=propColumn});
    }

    body->emplaceStmt(&NLExecutor::runCreateEdge, data);
}

void NLTranslator::translateSetNodeProperty(nl::SetNodeProperty setNodeProperty, NLStmtContainer* body) {
    if (!_metadataBuilder) {
        throw IRException("nl.set_node_property requires a MetadataBuilder (write transaction)");
    }

    const llvm::StringRef propName = setNodeProperty.getProperty();
    const mlir::Value inputValue = setNodeProperty.getInputNodes();
    const mlir::Value propValue = setNodeProperty.getValue();

    const ValueType valueType = valueTypeFromChunkType(propValue.getType());
    const PropertyType propType = _metadataBuilder->getOrCreatePropertyType(propName, valueType);

    const ColumnNodeIDs* inputColumn = static_cast<const ColumnNodeIDs*>(getColumn(inputValue));
    const Column* valueColumn = getColumn(propValue);

    NLSetNodePropertyData* data = _program->allocFunctionData<NLSetNodePropertyData>(
        propType._id,
        inputColumn,
        valueColumn);

    body->emplaceStmt(&NLExecutor::runSetNodeProperty, data);
}

void NLTranslator::translateSetEdgeProperty(nl::SetEdgeProperty setEdgeProperty, NLStmtContainer* body) {
    if (!_metadataBuilder) {
        throw IRException("nl.set_edge_property requires a MetadataBuilder (write transaction)");
    }

    const llvm::StringRef propName = setEdgeProperty.getProperty();
    const mlir::Value inputValue = setEdgeProperty.getInputEdges();
    const mlir::Value propValue = setEdgeProperty.getValue();

    const ValueType valueType = valueTypeFromChunkType(propValue.getType());
    const PropertyType propType = _metadataBuilder->getOrCreatePropertyType(propName, valueType);

    const ColumnEdgeIDs* inputColumn = static_cast<const ColumnEdgeIDs*>(getColumn(inputValue));
    const Column* valueColumn = getColumn(propValue);

    NLSetEdgePropertyData* data = _program->allocFunctionData<NLSetEdgePropertyData>(
        propType._id,
        inputColumn,
        valueColumn);

    body->emplaceStmt(&NLExecutor::runSetEdgeProperty, data);
}

void NLTranslator::translateDeleteNode(nl::DeleteNode deleteNode, NLStmtContainer* body) {
    const mlir::Value inputValue = deleteNode.getInputNodes();
    const ColumnNodeIDs* inputColumn = static_cast<const ColumnNodeIDs*>(getColumn(inputValue));

    NLDeleteNodeData* data = _program->allocFunctionData<NLDeleteNodeData>(
        inputColumn,
        deleteNode.getDetach());

    body->emplaceStmt(&NLExecutor::runDeleteNode, data);
}

void NLTranslator::translateDeleteEdge(nl::DeleteEdge deleteEdge, NLStmtContainer* body) {
    const mlir::Value inputValue = deleteEdge.getInputEdges();
    const ColumnEdgeIDs* inputColumn = static_cast<const ColumnEdgeIDs*>(getColumn(inputValue));

    NLDeleteEdgeData* data = _program->allocFunctionData<NLDeleteEdgeData>(inputColumn);

    body->emplaceStmt(&NLExecutor::runDeleteEdge, data);
}

void NLTranslator::translateConstant(nl::Constant constant) {
    const mlir::TypedValue<mlir::nl::ChunkType> res = constant.getResult();
    const auto chunkType = mlir::cast<nl::ChunkType>(res.getType());
    const mlir::Type elementType = chunkType.getElementType();

    // A list literal is carried as the array of its elements. It is written into the
    // query-scoped list buffer once, here, and the column holds a view over that run: one
    // list for every row, as a scalar constant holds one value. The views outlive
    // translation, so the chunk needs no per-step fill.
    if (llvm::isa<storage::ListType>(elementType)) {
        const auto elements = mlir::cast<mlir::ArrayAttr>(constant.getValue());

        ColumnConst<ListView>* lists = _memory->alloc<ColumnConst<ListView>>();
        lists->set(materializeListView(elements));

        _valueSlots[res] = lists;
        return;
    }

    const mlir::Attribute value = constant.getValue();

    if (const auto nullableType = mlir::dyn_cast<storage::NullableType>(elementType)) {
        if (mlir::isa<mlir::NoneType>(nullableType.getValueType())) {
            _valueSlots[res] = _memory->alloc<ColumnConst<PropertyNull>>();
            return;
        }
    }

    const ValueType valueType = valueTypeFromElementType(elementType);

    Column* column = nullptr;
    const auto materialize = [&]<SupportedType T>() {
        auto* typed = _memory->alloc<ColumnConst<typename T::Primitive>>();
        typed->set(constantValueAs<T>(value));
        column = typed;
    };
    ValueTypeDispatcher(valueType).execute(materialize);
    bioassert(column, "Failed to allocate column.");

    _valueSlots[res] = column;
}

void NLTranslator::translateBroadcastConstant(nl::BroadcastConstant broadcast, NLStmtContainer* body) {
    const Column* value = getColumn(broadcast.getValue());

    // Absent when no relation drives the projection, which is then the single row the
    // constant is, so there is no chunk to read a row count from
    const mlir::Value cardinalityChunk = broadcast.getCardinality();
    const Column* cardinality = cardinalityChunk ? getColumn(cardinalityChunk) : nullptr;

    const mlir::Value result = broadcast.getResult();
    const mlir::Type resultType = result.getType();

    Column* output = allocColumnForChunkType(resultType);
    _valueSlots[result] = output;

    // The untyped null constant is a ColumnConst<PropertyNull>, which holds no value to
    // repeat: its rows are absent values rather than copies of one. A list rides a list
    // chunk rather than a nullable value one, so its fill repeats the one view the constant
    // holds instead of dispatching on a value type.
    const bool isUntypedNull = isUntypedNullChunk(broadcast.getValue().getType());
    const bool isList = llvm::isa<storage::ListType>(mlir::cast<nl::ChunkType>(resultType).getElementType());

    NLBroadcastConstantFunction fill = nullptr;
    if (isUntypedNull) {
        fill = NLExecutor::selectNullConstantBroadcast();
    } else if (isList) {
        fill = NLExecutor::selectConstantListBroadcast();
    } else {
        fill = NLExecutor::selectConstantBroadcast(nullableChunkValueType(resultType));
    }

    NLBroadcastConstantData* data = _program->allocFunctionData<NLBroadcastConstantData>(value, cardinality, output, fill);
    body->emplaceStmt(&NLExecutor::runBroadcastConstant, data);
}

template <ColumnOperator Op, typename OpType>
void NLTranslator::translateBinaryOp(OpType op, NLStmtContainer* body) {
    const Column* lhs = getColumn(op.getLhs());
    const Column* rhs = getColumn(op.getRhs());

    Column* result = nullptr;
    const NLBinaryFn fn = NLExecutor::selectBinary<Op>(lhs, rhs, _memory, result);
    bioassert(result, "Failed to translate binary operator result.");

    _valueSlots[op.getResult()] = result;

    NLBinaryData* data = _program->allocFunctionData<NLBinaryData>(lhs, rhs, result, fn, _memory);
    body->emplaceStmt(&NLExecutor::runBinary, data);
}

void NLTranslator::translateNot(nl::Not notOp, NLStmtContainer* body) {
    const Column* operand = getColumn(notOp.getOperand());

    Column* result = nullptr;
    const NLUnaryFn fn = NLExecutor::selectNot(operand, _memory, result);
    bioassert(result, "Failed to allocate NOT result column.");

    _valueSlots[notOp.getResult()] = result;

    NLUnaryData* data = _program->allocFunctionData<NLUnaryData>(operand, result, fn);
    body->emplaceStmt(&NLExecutor::runUnary, data);
}

void NLTranslator::translateToNullable(nl::ToNullable toNullable, NLStmtContainer* body) {
    const Column* operand = getColumn(toNullable.getOperand());

    const auto resultChunk = mlir::cast<nl::ChunkType>(toNullable.getResult().getType());
    const auto nullableType = mlir::cast<storage::NullableType>(resultChunk.getElementType());
    const ValueType valueType = valueTypeFromElementType(nullableType.getValueType());

    Column* result = nullptr;
    const NLUnaryFn fn = NLExecutor::selectToNullable(valueType, operand, _memory, result);
    bioassert(result, "Failed to allocate the nullable column of nl.to_nullable.");

    _valueSlots[toNullable.getResult()] = result;

    NLUnaryData* data = _program->allocFunctionData<NLUnaryData>(operand, result, fn);
    body->emplaceStmt(&NLExecutor::runUnary, data);
}

void NLTranslator::translateUnaryFunction(mlir::Operation* op, NLStmtContainer* body) {
    const NLUnaryFunctionSelector select = lookupUnaryFunctionSelector(*op);
    bioassert(select, "translateUnaryFunction called on a non-function op");

    const mlir::Value inputValue = op->getOperand(0);
    const Column* input = getColumn(inputValue);

    const auto inputChunk = mlir::cast<nl::ChunkType>(inputValue.getType());
    const bool inputNullable = mlir::isa<storage::NullableType>(inputChunk.getElementType());

    Column* result = nullptr;
    const NLUnaryFunctionKernel kernel = select(input, inputNullable, _memory, result);
    bioassert(result, "Failed to allocate unary function result column.");

    _valueSlots[op->getResult(0)] = result;

    NLUnaryFunctionData* data = _program->allocFunctionData<NLUnaryFunctionData>(input, result, kernel);
    body->emplaceStmt(&NLExecutor::runUnaryFunction, data);
}

void NLTranslator::translateBinaryFunction(mlir::Operation* op, NLStmtContainer* body) {
    const NLBinaryFunctionSelector select = lookupBinaryFunctionSelector(*op);
    bioassert(select, "translateBinaryFunction called on a non-function op");

    const Column* lhs = getColumn(op->getOperand(0));
    const Column* rhs = getColumn(op->getOperand(1));

    Column* result = nullptr;
    const NLBinaryFn fn = select(lhs, rhs, _memory, result);
    bioassert(result, "Failed to allocate binary function result column.");

    _valueSlots[op->getResult(0)] = result;

    NLBinaryData* data = _program->allocFunctionData<NLBinaryData>(lhs, rhs, result, fn, _memory);
    body->emplaceStmt(&NLExecutor::runBinary, data);
}

void NLTranslator::translateFilter(nl::Filter filter, NLStmtContainer* body) {
    const Column* mask = getColumn(filter.getMask());

    const auto maskChunk = mlir::cast<nl::ChunkType>(filter.getMask().getType());
    const bool maskNullable = mlir::isa<storage::NullableType>(maskChunk.getElementType());
    const NLMaskSurvivorFunction filterFunction =
        NLExecutor::selectMaskSurvivorFunction(maskNullable);

    NLFilterData* data = _program->allocFunctionData<NLFilterData>(mask, filterFunction);

    // Reserve to avoid execution time allocations
    data->getIndices()->reserve(_program->getChunkSize());

    // New column for each result output, same types
    const mlir::OperandRange columns = filter.getColumns();
    const mlir::ResultRange results = filter.getResults();
    for (size_t columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
        const mlir::Value colVal = columns[columnIndex];
        const mlir::Type colType = colVal.getType();

        const Column* input = getColumn(colVal);
        Column* output = allocColumnForChunkType(colType);
        _valueSlots[results[columnIndex]] = output;

        const NLGatherFunction gatherType = selectGatherForChunkType(colType);

        const NLFilterData::FilterColumn column {input, output, gatherType};

        data->addColumn(column);
    }

    body->emplaceStmt(&NLExecutor::runFilter, data);
}

void NLTranslator::translateOutput(nl::Output output, NLStmtContainer* body) {
    // The optional limit handle is a separate operand, so read just the columns;
    // including it in this list would treat the handle as a chunk.
    const mlir::OperandRange columns = output.getColumns();
    if (columns.empty()) {
        throw IRException("nl.output requires at least one column");
    }

    // Output is either the per-step sink inside an nl.for body, or - for an
    // aggregate like COUNT that collapses to one row - a one-shot emit at function
    // scope. Either way its per-row columns must be bound in the output's own block,
    // which the per-column check below enforces: a loop variable of this loop, or a
    // chunk materialized in this block (a property fetch, or nl.count_result at
    // function scope). Two chunks cross that scope: a constant, and the single row a
    // reduction collapsed to. Any other chunk from an outer or sibling loop fails the
    // check.
    mlir::Block* outputBlock = output->getBlock();

    // A constant broadcasts to any step, since reading it ignores the row. A reduced
    // row is one row and nothing here spreads it over more, so it crosses only into a
    // step that is that single row.
    const bool singleRowStep = stepKeepsASingleRow(outputBlock);

    NLOutputData* outputData = _program->allocFunctionData<NLOutputData>();
    outputData->setLimit(limitStateFor(output.getLimit()));
    outputData->setSkip(skipStateFor(output.getSkip()));

    if (const mlir::Value cardinality = output.getCardinality()) {
        outputData->setCardinality(getColumn(cardinality));
    }

    for (const mlir::Value column : columns) {
        const auto columnArgument = mlir::dyn_cast<mlir::BlockArgument>(column);
        const bool isInnermostLoopVariable = columnArgument && columnArgument.getOwner() == outputBlock;

        // A property fetch result is not a loop variable but an op result
        // produced in this same loop body; it is equally available to output.
        mlir::Operation* definingOp = column.getDefiningOp();
        const bool isProducedInThisBlock = definingOp && definingOp->getBlock() == outputBlock;

        const bool isConstant = yieldsConstantColumn(column);
        const bool isReducedRow = singleRowStep && yieldsReducedRowChunk(column);

        if (!isInnermostLoopVariable && !isProducedInThisBlock && !isConstant && !isReducedRow) {
            throw IRException("nl.output columns must be a loop variable of the enclosing "
                              "nl.for, produced in this block, a constant, or a reduced row");
        }

        outputData->addOutputColumn(getColumn(column), !isConstant && !isReducedRow);
    }

    // The names label the result, not one emission of it, so they go on the program
    // rather than the per-step output data the limit and skip handles sit on.
    if (const mlir::ArrayAttr columnNames = output.getColumnNamesAttr()) {
        llvm::SmallVector<std::string_view> names;
        for (const mlir::Attribute name : columnNames) {
            const llvm::StringRef nameText = mlir::cast<mlir::StringAttr>(name).getValue();
            names.emplace_back(nameText.data(), nameText.size());
        }

        _program->setColumnNames(names);
    }

    body->emplaceStmt(&NLExecutor::runOutput, outputData);
}

bool NLTranslator::stepKeepsASingleRow(mlir::Block* block) const {
    // A cross product pairs what the loop bound with every row of another factor, so the
    // block holds that product rather than the one row the loop stands for.
    const bool crossesAnotherFactor = !block->getOps<nl::CrossProduct>().empty();
    if (crossesAnotherFactor) {
        return false;
    }

    nl::For forLoop = mlir::dyn_cast<nl::For>(block->getParentOp());
    if (!forLoop) {
        return true;
    }

    const auto configIt = _iteratorConfigs.find(forLoop.getIterator());
    if (configIt == _iteratorConfigs.end()) {
        return false;
    }

    const IteratorConfig& config = configIt->second;
    const bool drainsACollect = config._kind == IteratorKind::Collect;

    return drainsACollect && config._collectState->keyColumns().empty();
}

void NLTranslator::translateLimit(nl::Limit limit, NLStmtContainer* body) {
    // Allocate the runtime counter and map the handle to it, so the loops, the
    // update and the output that name the handle all find the same counter.
    NLLimitState* state = _program->allocLimitState();
    _limitStates[limit.getState()] = state;

    // The reset runs each time the block holding this nl.limit runs: once at
    // function scope for a top-level LIMIT, per enclosing step for a nested one.
    const size_t count = limit.getCount();
    NLLimitInitData* initData = _program->allocFunctionData<NLLimitInitData>(state, count);

    body->emplaceStmt(&NLExecutor::runLimitInit, initData);
}

void NLTranslator::translateLimitUpdate(nl::LimitUpdate update, NLStmtContainer* body) {
    // The handle is a required operand, so limitStateFor returns its counter or
    // throws if it was not produced by an nl.limit.
    NLLimitState* state = limitStateFor(update.getState());

    const Column* representative = getColumn(update.getRows());
    NLLimitUpdateData* updateData = _program->allocFunctionData<NLLimitUpdateData>(state, representative);

    body->emplaceStmt(&NLExecutor::runLimitUpdate, updateData);
}

void NLTranslator::translateLimitTruncate(nl::LimitTruncate truncate, NLStmtContainer* body) {
    // The handle is a required operand, so limitStateFor returns its counter or
    // throws if it was not produced by an nl.limit.
    NLLimitState* state = limitStateFor(truncate.getState());

    NLLimitTruncateData* data = _program->allocFunctionData<NLLimitTruncateData>(state);

    // One fresh output column per input, walked in step with the results the
    // downstream consumers are mapped to.
    const mlir::OperandRange columns = truncate.getColumns();
    const mlir::ResultRange results = truncate.getResults();
    for (size_t columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
        addTruncateColumn(columns[columnIndex], results[columnIndex], data);
    }

    body->emplaceStmt(&NLExecutor::runLimitTruncate, data);
}

void NLTranslator::addTruncateColumn(mlir::Value inputValue,
                                     mlir::Value resultValue,
                                     NLLimitTruncateData* data) {
    const Column* input = getColumn(inputValue);

    const auto chunkType = mlir::cast<nl::ChunkType>(inputValue.getType());
    const mlir::Type elementType = chunkType.getElementType();

    // The output keeps the input's element type; only the row count is cut. The
    // copy is a block-repeat with factor 1 (each input row once, stopping at
    // emitThisStep), so it reuses the block-repeat broadcast families the cross
    // product uses - by value type for a nullable value chunk, by chunk kind for
    // an ID chunk, on uint64 for a count chunk.
    Column* output = nullptr;
    NLBroadcastFunction copyPrefix = nullptr;

    if (isConstantColumn(input)) {
        output = _memory->allocSame(input);
        copyPrefix = NLExecutor::selectConstBlockRepeatFunction();
    } else if (const auto nullableType = mlir::dyn_cast<storage::NullableType>(elementType)) {
        const ValueType valueType = valueTypeFromElementType(nullableType.getValueType());
        output = allocOptColumnForValueType(valueType);
        copyPrefix = NLExecutor::selectOptBlockRepeatFunction(valueType);
    } else if (isPlainValueElementType(elementType)) {
        const ValueType valueType = valueTypeFromElementType(elementType);
        output = allocPlainColumn(valueType);
        copyPrefix = NLExecutor::selectPlainBlockRepeatFunction(valueType);
    } else if (llvm::isa<storage::ListType>(elementType)) {
        output = allocListColumn();
        copyPrefix = NLExecutor::selectListBlockRepeatFunction();
    } else if (mlir::isa<storage::ListElementType>(elementType)) {
        output = allocListElementColumn();
        copyPrefix = NLExecutor::selectListElementBlockRepeatFunction();
    } else {
        const NLChunkKind kind = getChunkKind(chunkType);
        output = allocColumnForKind(kind);
        copyPrefix = NLExecutor::selectBlockRepeatFunction(kind);
    }

    _valueSlots[resultValue] = output;

    const NLCrossColumn truncateColumn(input, output, copyPrefix);
    data->addColumn(truncateColumn);
}

NLLimitState* NLTranslator::limitStateFor(mlir::Value handle) const {
    if (!handle) {
        return nullptr;
    }

    const auto stateIt = _limitStates.find(handle);
    if (stateIt == _limitStates.end()) {
        throw IRException("limit handle must be produced by an nl.limit");
    }

    return stateIt->second;
}

void NLTranslator::translateSkip(nl::Skip skip, NLStmtContainer* body) {
    // Allocate the runtime counter and map the handle to it, so the update and the
    // truncate that name the handle both find the same counter.
    NLSkipState* state = _program->allocSkipState();
    _skipStates[skip.getState()] = state;

    // The reset runs each time the block holding this nl.skip runs: once at
    // function scope for a top-level SKIP.
    const size_t count = skip.getCount();
    NLSkipInitData* initData = _program->allocFunctionData<NLSkipInitData>(state, count);

    body->emplaceStmt(&NLExecutor::runSkipInit, initData);
}

void NLTranslator::translateSkipUpdate(nl::SkipUpdate update, NLStmtContainer* body) {
    // The handle is a required operand, so skipStateFor returns its counter or
    // throws if it was not produced by an nl.skip.
    NLSkipState* state = skipStateFor(update.getState());

    const Column* representative = getColumn(update.getRows());
    NLSkipUpdateData* updateData = _program->allocFunctionData<NLSkipUpdateData>(state, representative);

    body->emplaceStmt(&NLExecutor::runSkipUpdate, updateData);
}

void NLTranslator::translateSkipTruncate(nl::SkipTruncate truncate, NLStmtContainer* body) {
    // The handle is a required operand, so skipStateFor returns its counter or
    // throws if it was not produced by an nl.skip.
    NLSkipState* state = skipStateFor(truncate.getState());

    NLSkipTruncateData* data = _program->allocFunctionData<NLSkipTruncateData>(state);

    // One fresh output column per input, walked in step with the results the
    // downstream consumers are mapped to.
    const mlir::OperandRange columns = truncate.getColumns();
    const mlir::ResultRange results = truncate.getResults();
    for (size_t columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
        addSkipColumn(columns[columnIndex], results[columnIndex], data);
    }

    body->emplaceStmt(&NLExecutor::runSkipTruncate, data);
}

void NLTranslator::addSkipColumn(mlir::Value inputValue,
                                 mlir::Value resultValue,
                                 NLSkipTruncateData* data) {
    const Column* input = getColumn(inputValue);

    const auto chunkType = mlir::cast<nl::ChunkType>(inputValue.getType());
    const mlir::Type elementType = chunkType.getElementType();

    // The output keeps the input's element type; only the dropped prefix is
    // removed. The copy is a range copy of the surviving suffix to the front of a
    // fresh chunk, so it uses the copy families - by value type for a nullable
    // value chunk, by chunk kind for an ID chunk, on uint64 for a count chunk.
    Column* output = nullptr;
    NLCopyFunction copySuffix = nullptr;

    if (isConstantColumn(input)) {
        output = _memory->allocSame(input);
        copySuffix = NLExecutor::selectConstCopyFunction();
    } else if (const auto nullableType = mlir::dyn_cast<storage::NullableType>(elementType)) {
        const ValueType valueType = valueTypeFromElementType(nullableType.getValueType());
        output = allocOptColumnForValueType(valueType);
        copySuffix = NLExecutor::selectOptCopyFunction(valueType);
    } else if (isPlainValueElementType(elementType)) {
        const ValueType valueType = valueTypeFromElementType(elementType);
        output = allocPlainColumn(valueType);
        copySuffix = NLExecutor::selectPlainCopyFunction(valueType);
    } else if (llvm::isa<storage::ListType>(elementType)) {
        output = allocListColumn();
        copySuffix = NLExecutor::selectListCopyFunction();
    } else if (mlir::isa<storage::ListElementType>(elementType)) {
        output = allocListElementColumn();
        copySuffix = NLExecutor::selectListElementCopyFunction();
    } else {
        const NLChunkKind kind = getChunkKind(chunkType);
        output = allocColumnForKind(kind);
        copySuffix = NLExecutor::selectCopyFunction(kind);
    }

    _valueSlots[resultValue] = output;

    const NLSkipColumn skipColumn(input, output, copySuffix);
    data->addColumn(skipColumn);
}

NLSkipState* NLTranslator::skipStateFor(mlir::Value handle) const {
    if (!handle) {
        return nullptr;
    }

    const auto stateIt = _skipStates.find(handle);
    if (stateIt == _skipStates.end()) {
        throw IRException("skip handle must be produced by an nl.skip");
    }

    return stateIt->second;
}

void NLTranslator::translateSortBuffer(nl::SortBuffer buffer, NLStmtContainer* body) {
    // Allocate the runtime accumulator and map the handle to it, so the collect
    // and the emit loop that name the handle share the same buffers. The buffer
    // columns themselves are allocated by the collect, which knows their types.
    NLSortState* state = _program->allocSortState();
    _sortStates[buffer.getState()] = state;

    // A fused ORDER BY ... LIMIT carries the bound as the top-K count, so the
    // accumulator keeps only the best k rows. Absent, it keeps every row.
    if (const std::optional<uint64_t> topK = buffer.getTopK()) {
        state->setTopK(*topK);
    }

    // The reset empties the buffers each time the block holding this nl.sort_buffer
    // runs: once at function scope for a top-level ORDER BY.
    NLSortResetData* resetData = _program->allocFunctionData<NLSortResetData>(state);
    body->emplaceStmt(&NLExecutor::runSortReset, resetData);
}

void NLTranslator::translateSortCollect(nl::SortCollect collect, NLStmtContainer* body) {
    const mlir::Value collectState = collect.getState();
    NLSortState* state = sortStateFor(collectState);

    // The sort spec lives on the nl.sort_buffer that produced the handle.
    nl::SortBuffer buffer = collectState.getDefiningOp<nl::SortBuffer>();
    if (!buffer) {
        throw IRException("sort_collect state must come from nl.sort_buffer");
    }

    // The buffers are allocated once, by the single collect feeding a buffer.
    // Generated IR has exactly one collect per accumulator; a second would append
    // to the same buffers and double the rows, so it is rejected here.
    if (!state->buffers().empty()) {
        throw IRException("an nl.sort_buffer must be fed by a single nl.sort_collect");
    }

    NLSortCollectData* data = _program->allocFunctionData<NLSortCollectData>(state);

    // One growing buffer per collected column, row-aligned. The buffer keeps the
    // column's element type; the append copies a chunk's rows onto its tail. A
    // bounded accumulator also gets a compaction scratch column and the gather
    // that fills it, so the trim can drop all but the best k rows.
    const bool bounded = state->isBounded();
    const mlir::OperandRange columns = collect.getColumns();
    for (const mlir::Value column : columns) {
        Column* bufferColumn = allocColumnForChunkType(column.getType());

        Column* tempColumn = bounded ? allocColumnForChunkType(column.getType()) : nullptr;
        const NLGatherFunction gather = bounded ? selectGatherForChunkType(column.getType()) : nullptr;
        state->addColumnBuffer(bufferColumn, tempColumn, gather);

        const NLSortCollectData::Append append {getColumn(column),
                                                bufferColumn,
                                                selectAppendForChunkType(column.getType())};
        data->addAppend(append);
    }

    // Build the comparators from the spec, most significant key first. Each key
    // indexes a collected column, so the index must be in range, and the column's
    // element type must have an order (an embedding key is rejected by the
    // selector). The buffer column is the one the comparator reads at run time.
    const llvm::ArrayRef<int64_t> keyColumns = buffer.getKeyColumns();
    const llvm::ArrayRef<bool> keyAscending = buffer.getKeyAscending();
    for (size_t keyIndex = 0; keyIndex < keyColumns.size(); keyIndex++) {
        const int64_t keyColumn = keyColumns[keyIndex];
        const bool inRange = keyColumn >= 0 && static_cast<size_t>(keyColumn) < columns.size();
        if (!inRange) {
            throw IRException("sort key column index is out of range");
        }

        const mlir::Type keyType = columns[static_cast<size_t>(keyColumn)].getType();
        const NLSortState::Key key {state->buffer(static_cast<size_t>(keyColumn)),
                                    keyAscending[keyIndex],
                                    selectCompareForChunkType(keyType)};
        state->addKey(key);
    }

    body->emplaceStmt(&NLExecutor::runSortCollect, data);
}

void NLTranslator::translateSortLoop(const IteratorConfig& config,
                                     mlir::Block& loopBody,
                                     NLLimitState* limit,
                                     NLStmtContainer* body) {
    NLSortState* state = config._sortState;
    if (!state) {
        throw IRException("nl.sort iterator must carry a sort accumulator");
    }

    // For::verify binds one loop variable per iterator chunk, and the sort
    // iterator's chunk types are the collected column types, so the loop must
    // take exactly one variable per buffer.
    const size_t bufferCount = state->buffers().size();
    if (loopBody.getNumArguments() != bufferCount) {
        throw IRException("nl.sort loop must bind one variable per collected column");
    }

    NLSortLoopData* loopData = _program->allocFunctionData<NLSortLoopData>(state);
    loopData->setLimit(limit);

    // Reserve the permutation-slice scratch so the per-step gather stays
    // allocation-free, the same as the edge loop's indices column.
    loopData->getIndices()->reserve(_program->getChunkSize());

    // Each loop variable is filled from its buffer by gathering the rows the
    // current emit step covers, in permutation order. The buffer is the read
    // side, the loop variable the write side - the NLCarriedColumn (input,
    // output, gather) shape the edge loops already use.
    for (size_t columnIndex = 0; columnIndex < bufferCount; columnIndex++) {
        const mlir::Value loopVariable = loopBody.getArgument(static_cast<unsigned>(columnIndex));

        Column* output = allocColumnForChunkType(loopVariable.getType());
        _valueSlots[loopVariable] = output;

        const NLCarriedColumn column(state->buffer(columnIndex),
                                     output,
                                     selectGatherForChunkType(loopVariable.getType()));
        loopData->addColumn(column);
    }

    body->emplaceStmt(&NLExecutor::runSortLoop, loopData);

    translateBlock(loopBody, loopData->getStmts());
}

NLSortState* NLTranslator::sortStateFor(mlir::Value handle) const {
    const auto stateIt = _sortStates.find(handle);
    if (stateIt == _sortStates.end()) {
        throw IRException("sort handle must be produced by an nl.sort_buffer");
    }

    return stateIt->second;
}

void NLTranslator::translateDistinctState(nl::Distinct distinct, NLStmtContainer* body) {
    // Allocate the runtime seen-set and map the handle to it, so the filter that
    // names the handle finds the same set.
    NLDistinctState* state = _program->allocDistinctState();
    _distinctStates[distinct.getState()] = state;

    // The reset empties the set each time the block holding this nl.distinct runs:
    // once at function scope for a top-level / mid-query DISTINCT.
    NLDistinctResetData* resetData = _program->allocFunctionData<NLDistinctResetData>(state);
    body->emplaceStmt(&NLExecutor::runDistinctReset, resetData);
}

void NLTranslator::translateDistinctFilter(nl::DistinctFilter filter, NLStmtContainer* body) {
    // The handle is a required operand, so distinctStateFor returns its set or
    // throws if it was not produced by an nl.distinct.
    NLDistinctState* state = distinctStateFor(filter.getState());

    NLDistinctFilterData* data = _program->allocFunctionData<NLDistinctFilterData>(state);

    // Reserve the surviving-indices scratch so the per-step gather stays
    // allocation-free, the same as the edge and sort loops' indices column.
    data->getIndices()->reserve(_program->getChunkSize());

    // One fresh output column per input, walked in step with the results the
    // downstream consumers are mapped to.
    const mlir::OperandRange columns = filter.getColumns();
    const mlir::ResultRange results = filter.getResults();
    for (size_t columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
        addDistinctColumn(columns[columnIndex], results[columnIndex], data);
    }

    body->emplaceStmt(&NLExecutor::runDistinctFilter, data);
}

void NLTranslator::addDistinctColumn(mlir::Value inputValue,
                                     mlir::Value resultValue,
                                     NLDistinctFilterData* data) {
    const Column* input = getColumn(inputValue);

    // The output keeps the input's element type; only duplicate rows are removed.
    // The key-append serializes one of its rows into the shared row key, and the
    // gather copies the survivors into the fresh output - the same gather family
    // the edge and sort loops use, selected by chunk type.
    Column* output = allocColumnForChunkType(inputValue.getType());
    _valueSlots[resultValue] = output;

    const NLDistinctFilterData::FilterColumn column {input,
                                                     output,
                                                     selectKeyAppendForChunkType(inputValue.getType()),
                                                     selectGatherForChunkType(inputValue.getType())};
    data->addColumn(column);
}

NLDistinctState* NLTranslator::distinctStateFor(mlir::Value handle) const {
    const auto stateIt = _distinctStates.find(handle);
    if (stateIt == _distinctStates.end()) {
        throw IRException("distinct handle must be produced by an nl.distinct");
    }

    return stateIt->second;
}

void NLTranslator::translateCountState(nl::Count count, NLStmtContainer* body) {
    // Allocate the runtime tally and map the handle to it, so the update and the
    // emit loop that name the handle share the same counter.
    NLCountState* state = _program->allocCountState();
    _countStates[count.getState()] = state;

    // The reset zeroes the tally each time the block holding this nl.count runs:
    // once at function scope for a top-level / mid-query COUNT.
    NLCountResetData* resetData = _program->allocFunctionData<NLCountResetData>(state);
    body->emplaceStmt(&NLExecutor::runCountReset, resetData);
}

void NLTranslator::translateCountUpdate(nl::CountUpdate update, NLStmtContainer* body) {
    // The handle is a required operand, so countStateFor returns its tally or
    // throws if it was not produced by an nl.count.
    NLCountState* state = countStateFor(update.getState());

    // Measure the chunk's non-null rows the way its type demands: all rows for an
    // ID chunk, the present values for a nullable value chunk. count(*) reads no value,
    // so it charges every row whatever the chunk holds.
    const mlir::Value rows = update.getRows();
    const Column* input = getColumn(rows);
    const NLCountFunction count = update.getAllRows() ? &NLExecutor::countAllRows
                                                      : selectCountForChunkType(rows.getType());

    NLCountUpdateData* data = _program->allocFunctionData<NLCountUpdateData>(state, input, count);
    body->emplaceStmt(&NLExecutor::runCountUpdate, data);
}

void NLTranslator::translateCountResult(nl::CountResult result, NLStmtContainer* body) {
    // The handle is a required operand, so countStateFor returns its tally or
    // throws if it was not produced by an nl.count.
    NLCountState* state = countStateFor(result.getState());

    // The result is the unsigned i64 count chunk (!nl.chunk<ui64>) runCountResult
    // fills with the single tally row.
    ColumnVector<uint64_t>* output = allocCountColumn();
    _valueSlots[result.getResult()] = output;

    NLCountResultData* data = _program->allocFunctionData<NLCountResultData>(state, output);
    body->emplaceStmt(&NLExecutor::runCountResult, data);
}

NLCountState* NLTranslator::countStateFor(mlir::Value handle) const {
    const auto stateIt = _countStates.find(handle);
    if (stateIt == _countStates.end()) {
        throw IRException("count handle must be produced by an nl.count");
    }

    return stateIt->second;
}

void NLTranslator::translateAggregateState(nl::Aggregate aggregate, NLStmtContainer* body) {
    // Allocate the runtime accumulator and map the handle to it, so the update and
    // the emit that name the handle share the same accumulator.
    NLAggregateState* state = _program->allocAggregateState();
    _aggregateStates[aggregate.getState()] = state;

    // The state handle's element type is the accumulator's value type (f64 for an
    // avg, the input type otherwise), baked during lowering. Allocate the single-row
    // nullable value column the reduction folds into.
    const auto stateType = mlir::cast<nl::AggregateStateType>(aggregate.getState().getType());
    const ValueType accumulatorType = valueTypeFromElementType(stateType.getElementType());
    Column* accumulator = allocSingleRowOptColumnForValueType(accumulatorType);
    state->setAccumulator(accumulator);

    // The reset re-initializes the accumulator each time the block holding this
    // nl.aggregate runs (once at function scope for a top-level aggregate): a present
    // zero for sum/avg, null for min/max.
    const AggregateKind kind = toRuntimeAggregateKind(aggregate.getKind());
    const NLAggregateResetFunction reset = NLExecutor::selectAggregateReset(kind, accumulatorType);

    NLAggregateResetData* resetData = _program->allocFunctionData<NLAggregateResetData>(state, reset);
    body->emplaceStmt(&NLExecutor::runAggregateReset, resetData);
}

void NLTranslator::translateAggregateUpdate(nl::AggregateUpdate update, NLStmtContainer* body) {
    // The handle is a required operand, so aggregateStateFor returns its accumulator
    // or throws if it was not produced by an nl.aggregate.
    NLAggregateState* state = aggregateStateFor(update.getState());

    // Fold the chunk's non-null values the way its kind and chunk type demand. The
    // input value type may differ from the accumulator's (an avg of i64 folds into
    // an f64 accumulator), so the handler is selected from the input here.
    const mlir::Value rows = update.getRows();
    const Column* input = getColumn(rows);
    const AggregateKind kind = toRuntimeAggregateKind(update.getKind());
    const NLAggregateUpdateFunction fold = selectAggregateUpdateForChunkType(kind, rows.getType());

    NLAggregateUpdateData* data = _program->allocFunctionData<NLAggregateUpdateData>(state, input, fold);
    body->emplaceStmt(&NLExecutor::runAggregateUpdate, data);
}

void NLTranslator::translateAggregateResult(nl::AggregateResult result, NLStmtContainer* body) {
    // The handle is a required operand, so aggregateStateFor returns its accumulator
    // or throws if it was not produced by an nl.aggregate.
    NLAggregateState* state = aggregateStateFor(result.getState());

    // The result is a single-row nullable value chunk runAggregateResult fills with
    // the reduced value. Allocate its ColumnOptVector on the result value type and
    // map the op result to it.
    const mlir::Value resultChunk = result.getResult();
    const ValueType resultType = nullableChunkValueType(resultChunk.getType());
    Column* output = allocOptColumnForValueType(resultType);
    _valueSlots[resultChunk] = output;

    const AggregateKind kind = toRuntimeAggregateKind(result.getKind());
    const NLAggregateResultFunction emit = NLExecutor::selectAggregateResult(kind, resultType);

    NLAggregateResultData* data = _program->allocFunctionData<NLAggregateResultData>(state, output, emit);
    body->emplaceStmt(&NLExecutor::runAggregateResult, data);
}

NLAggregateState* NLTranslator::aggregateStateFor(mlir::Value handle) const {
    const auto stateIt = _aggregateStates.find(handle);
    if (stateIt == _aggregateStates.end()) {
        throw IRException("aggregate handle must be produced by an nl.aggregate");
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
                                       NLGroupAggregateState::Aggregate& aggregate) {
    const GroupAggregateKind kind = toRuntimeGroupAggregateKind(mlirKind);
    const mlir::Type chunkType = column.getType();

    aggregate._input = getColumn(column);

    // A switch (not an if/else) over every kind so a new one is a compile error
    // here rather than silently taking the value-reduction path.
    switch (kind) {
        case GroupAggregateKind::Count: {
            // count tallies rows, so it keeps no reduced value (a null
            // accumulator, only the per-group tally). count(*) over an ID chunk
            // charges every row; count(x) over a nullable value chunk charges
            // only the present values.
            aggregate._accumulator = nullptr;
            aggregate._grow = NLExecutor::selectGroupAggregateGrow(kind, ValueType::Int64);
            aggregate._emit = NLExecutor::selectGroupAggregateEmit(kind, ValueType::Int64);

            const auto chunk = mlir::cast<nl::ChunkType>(chunkType);
            const mlir::Type countElementType = chunk.getElementType();
            const auto nullable = mlir::dyn_cast<storage::NullableType>(countElementType);
            if (nullable) {
                const ValueType valueType = valueTypeFromElementType(nullable.getValueType());
                aggregate._fold = NLExecutor::selectGroupAggregateFold(kind, valueType);
            } else if (mlir::isa<storage::ListElementType>(countElementType)) {
                aggregate._fold = NLExecutor::selectGroupCountListElementFold();
            } else if (llvm::isa<storage::ListType>(countElementType)) {
                // No row of a list chunk is null, so a group's tally is its whole row
                // count, as it is for count(*).
                aggregate._fold = NLExecutor::selectGroupCountAllFold();
            } else {
                // A non-nullable chunk holds no null to skip - an ID chunk of
                // count(*), or a column a CALL yielded - so every row is charged.
                aggregate._fold = NLExecutor::selectGroupCountAllFold();
            }
        }
        break;

        case GroupAggregateKind::CountRows: {
            // count(*) charges every row of the group and reads no value, so the
            // chunk it is anchored on needs no type dispatch at all
            aggregate._accumulator = nullptr;
            aggregate._grow = NLExecutor::selectGroupAggregateGrow(kind, ValueType::Int64);
            aggregate._emit = NLExecutor::selectGroupAggregateEmit(kind, ValueType::Int64);
            aggregate._fold = NLExecutor::selectGroupCountAllFold();
        }
        break;

        case GroupAggregateKind::CountDistinct: {
            // count(DISTINCT x) keeps the same per-group tally as count, charged
            // once per distinct value instead of once per row, so it shares count's
            // grow and emit and differs only in the fold. count(DISTINCT n) over an
            // ID chunk keys on the ID; count(DISTINCT x) over a nullable value chunk
            // keys on the value and skips the nulls.
            aggregate._accumulator = nullptr;
            aggregate._grow = NLExecutor::selectGroupAggregateGrow(kind, ValueType::Int64);
            aggregate._emit = NLExecutor::selectGroupAggregateEmit(kind, ValueType::Int64);

            const auto chunk = mlir::cast<nl::ChunkType>(chunkType);
            const auto nullable = mlir::dyn_cast<storage::NullableType>(chunk.getElementType());
            if (nullable) {
                const ValueType valueType = valueTypeFromElementType(nullable.getValueType());
                aggregate._fold = NLExecutor::selectGroupAggregateFold(kind, valueType);
            } else if (mlir::isa<storage::ListElementType>(chunk.getElementType())) {
                aggregate._fold = NLExecutor::selectGroupCountDistinctListElementFold();
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
        case GroupAggregateKind::AvgDistinct: {
            // sum/min/max/avg reduce the values themselves, so the input must be a
            // nullable value chunk; avg accumulates as f64, the rest in the input's
            // own type. nullableChunkValueType rejects an ID chunk here. The distinct
            // kinds keep the shape of the kind they mirror and differ only in the
            // fold, which charges each of a group's values once.
            //
            // A type-erased input carries a type per cell, so it reduces by tag into the
            // f64 its mixed numeric tags land on rather than into the input's own type.
            const mlir::Type reducedElement = mlir::cast<nl::ChunkType>(chunkType).getElementType();
            const bool reducesTaggedCells = mlir::isa<storage::ListElementType>(reducedElement);

            const bool accumulatesAsDouble = reducesTaggedCells
                                          || (kind == GroupAggregateKind::Avg)
                                          || (kind == GroupAggregateKind::AvgDistinct);

            const ValueType inputType = reducesTaggedCells ? ValueType::Double
                                                           : nullableChunkValueType(chunkType);
            const ValueType accumulatorType = accumulatesAsDouble ? ValueType::Double : inputType;

            aggregate._accumulator = allocOptColumnForValueType(accumulatorType);
            aggregate._grow = NLExecutor::selectGroupAggregateGrow(kind, accumulatorType);
            aggregate._fold = reducesTaggedCells
                                  ? NLExecutor::selectTaggedGroupAggregateFold(kind)
                                  : NLExecutor::selectGroupAggregateFold(kind, inputType);
            aggregate._emit = NLExecutor::selectGroupAggregateEmit(kind, accumulatorType);
        }
        break;
    }
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

    // The key buffers and accumulators are allocated once, by the single update
    // feeding an accumulator. Generated IR has exactly one update per accumulator; a
    // second would fold the same rows twice, so it is rejected here.
    if (!state->keyColumns().empty() || !state->aggregates().empty()) {
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

    // One growing key buffer per grouping key, holding its distinct value per group.
    // The buffer keeps the key column's element type; the key-append serializes a row
    // into the group key (the DISTINCT serializer), the gather-append grows the
    // buffer with each new group's key values, and the range copy slices it at emit.
    for (size_t keyIndex = 0; keyIndex < keyCount; keyIndex++) {
        const mlir::Value column = columns[keyIndex];

        NLGroupAggregateState::KeyColumn key;
        key._input = getColumn(column);
        key._buffer = allocColumnForChunkType(column.getType());
        key._keyAppend = selectKeyAppendForChunkType(column.getType());
        key._gatherAppend = selectGroupKeyGatherForChunkType(column.getType());
        key._emitCopy = selectCopyForChunkType(column.getType());

        state->addKeyColumn(key);
    }

    // One per-group accumulator per aggregate, with the grow/fold/emit handlers baked
    // from the kind and the input value type.
    for (size_t aggregateIndex = 0; aggregateIndex < kinds.size(); aggregateIndex++) {
        const auto kind = static_cast<storage::GroupAggregateKind>(kinds[aggregateIndex]);

        NLGroupAggregateState::Aggregate aggregate;
        buildGroupAggregate(kind, columns[keyCount + aggregateIndex], aggregate);

        state->addAggregate(aggregate);
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
    const size_t keyCount = state->keyColumns().size();
    const size_t aggregateCount = state->aggregates().size();
    if (loopBody.getNumArguments() != keyCount + aggregateCount) {
        throw IRException("nl.group_aggregate loop must bind one variable per output column");
    }

    NLGroupAggregateLoopData* loopData = _program->allocFunctionData<NLGroupAggregateLoopData>(state);
    loopData->setLimit(limit);

    // Each loop variable is the emit output of one column: the first keyCount are the
    // grouping keys (filled by slicing their key buffers), the rest the aggregate
    // results (filled from the per-group state). Wire each into its column's slot on
    // the shared state, so runGroupAggregateLoop fills the loop variables directly.
    for (size_t keyIndex = 0; keyIndex < keyCount; keyIndex++) {
        const mlir::Value loopVariable = loopBody.getArgument(static_cast<unsigned>(keyIndex));

        Column* output = allocColumnForResultChunkType(loopVariable.getType());
        _valueSlots[loopVariable] = output;
        state->keyColumns()[keyIndex]._output = output;
    }

    for (size_t aggregateIndex = 0; aggregateIndex < aggregateCount; aggregateIndex++) {
        const mlir::Value loopVariable = loopBody.getArgument(static_cast<unsigned>(keyCount + aggregateIndex));

        Column* output = allocColumnForResultChunkType(loopVariable.getType());
        _valueSlots[loopVariable] = output;
        state->aggregates()[aggregateIndex]._output = output;
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
    if (!state->keyColumns().empty() || !state->valueColumns().empty()) {
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

    // One growing key buffer per grouping key, holding its distinct value per group -
    // the same setup as translateGroupAggregateUpdate's keys.
    for (size_t keyIndex = 0; keyIndex < keyCount; keyIndex++) {
        const mlir::Value column = columns[keyIndex];

        NLCollectState::KeyColumn key;
        key._input = getColumn(column);
        key._buffer = allocColumnForChunkType(column.getType());
        key._keyAppend = selectKeyAppendForChunkType(column.getType());
        key._gatherAppend = selectGroupKeyGatherForChunkType(column.getType());
        key._emitCopy = selectCopyForChunkType(column.getType());
        key._gather = selectGatherForChunkType(column.getType());

        state->addKeyColumn(key);
    }

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

        if (mlir::isa<storage::NullableType>(element)) {
            const ValueType valueType = nullableChunkValueType(column.getType());

            value._buffer = allocValueColumnForValueType(valueType);
            value._fold = isDistinct ? NLExecutor::selectCollectDistinctFold(valueType)
                                     : NLExecutor::selectCollectFold(valueType);
            value._unwindCollectEmit = NLExecutor::selectUnwindCollectValueEmit(valueType);
            value._listEmit = NLExecutor::selectCollectListEmit(valueType);
        } else if (mlir::isa<storage::ListType>(element)) {
            NLCollectFoldFunction fold = nullptr;
            NLCollectListEmitFunction listEmit = nullptr;
            NLExecutor::selectCollectListHandlers(isDistinct, fold, listEmit);

            value._buffer = allocListColumn();
            value._fold = fold;
            value._listEmit = listEmit;
        } else if (mlir::isa<storage::ListElementType>(element)) {
            NLCollectFoldFunction fold = nullptr;
            NLCollectListEmitFunction listEmit = nullptr;
            NLExecutor::selectCollectTaggedHandlers(isDistinct, fold, listEmit);

            value._buffer = allocListElementColumn();
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

    // The reductions taken beside the list read the same groups, so their accumulators
    // live on this state and are wired exactly as a grouped aggregation's are.
    for (size_t aggregateIndex = 0; aggregateIndex < kinds.size(); aggregateIndex++) {
        const storage::GroupAggregateKind aggregateKind = static_cast<storage::GroupAggregateKind>(kinds[aggregateIndex]);

        NLGroupAggregateState::Aggregate aggregate;
        buildGroupAggregate(aggregateKind, columns[keyCount + valueCount + aggregateIndex], aggregate);

        state->addAggregate(aggregate);
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
    const size_t keyCount = state->keyColumns().size();
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

        Column* output = allocColumnForResultChunkType(loopVariable.getType());
        _valueSlots[loopVariable] = output;
        state->keyColumns()[keyIndex]._output = output;
    }

    // The last loop variable is the unwound element value (a nullable value chunk).
    const mlir::Value valueVariable = loopBody.getArgument(static_cast<unsigned>(keyCount));
    Column* valueOutput = allocColumnForResultChunkType(valueVariable.getType());
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
    const size_t keyCount = state->keyColumns().size();
    std::vector<NLCollectState::ValueColumn>& valueColumns = state->valueColumns();
    std::vector<NLGroupAggregateState::Aggregate>& aggregates = state->aggregates();
    if (loopBody.getNumArguments() != keyCount + valueColumns.size() + aggregates.size()) {
        throw IRException("nl.collect loop must bind one variable per grouping key, one per list, and one per aggregate");
    }

    NLCollectLoopData* loopData = _program->allocFunctionData<NLCollectLoopData>(state);

    // The first keyCount loop variables are the grouping keys (filled by slicing their
    // key buffers, one row per group); wire each into its key column's output slot.
    for (size_t keyIndex = 0; keyIndex < keyCount; keyIndex++) {
        const mlir::Value loopVariable = loopBody.getArgument(static_cast<unsigned>(keyIndex));

        Column* output = allocColumnForResultChunkType(loopVariable.getType());
        _valueSlots[loopVariable] = output;
        state->keyColumns()[keyIndex]._output = output;
    }

    // Then one per-group list cell per collected column (a ColumnVector<ListView>).
    for (size_t valueIndex = 0; valueIndex < valueColumns.size(); valueIndex++) {
        const mlir::Value listVariable = loopBody.getArgument(static_cast<unsigned>(keyCount + valueIndex));

        Column* listOutput = allocColumnForResultChunkType(listVariable.getType());
        _valueSlots[listVariable] = listOutput;
        valueColumns[valueIndex]._output = listOutput;
    }

    // The remaining variables take the reductions, one per aggregate, filled from the
    // same group window the lists are sliced over.
    for (size_t aggregateIndex = 0; aggregateIndex < aggregates.size(); aggregateIndex++) {
        const unsigned argumentIndex = static_cast<unsigned>(keyCount + valueColumns.size() + aggregateIndex);
        const mlir::Value loopVariable = loopBody.getArgument(argumentIndex);

        Column* output = allocColumnForResultChunkType(loopVariable.getType());
        _valueSlots[loopVariable] = output;
        aggregates[aggregateIndex]._output = output;
    }

    body->emplaceStmt(&NLExecutor::runCollectLoop, loopData);

    translateBlock(loopBody, loopData->getStmts());
}

// A plain (non-nullable) value column of the collected type's primitive: collect
// drops nulls, so only present values land here. It grows as values are appended
// across steps, so no initial reserve is needed.
Column* NLTranslator::allocValueColumnForValueType(ValueType valueType) {
    switch (valueType) {
        case ValueType::Int64:
            return _memory->alloc<ColumnVector<types::Int64::Primitive>>();
        break;

        case ValueType::UInt64:
            return _memory->alloc<ColumnVector<types::UInt64::Primitive>>();
        break;

        case ValueType::Double:
            return _memory->alloc<ColumnVector<types::Double::Primitive>>();
        break;

        case ValueType::Bool:
            return _memory->alloc<ColumnVector<types::Bool::Primitive>>();
        break;

        case ValueType::String:
            return _memory->alloc<ColumnVector<types::String::Primitive>>();
        break;

        default:
            throw IRException("collect does not support this value type");
        break;
    }

    return nullptr;
}

void NLTranslator::translateProcedure(nl::Procedure procedureOp, NLStmtContainer* body) {
    const llvm::StringRef name = procedureOp.getName();

    if (!_procedureContext) {
        throw IRException(fmt::format("nl.procedure of '{}' requires a procedure context, but the "
                                      "translator was created without one",
                                      name.str()));
    }

    const ProcedureManager* procedures = _procedureContext->getProcedures();
    if (!procedures) {
        throw IRException("The procedure context carries no procedure registry to resolve "
                          "nl.procedure against");
    }

    // A procedure fills at most a chunk's worth of rows per call, reading that budget
    // off the context: a zero budget leaves a drive loop asking an exhausted-looking
    // procedure for rows forever, so reject the unset context here rather than hang.
    if (_procedureContext->getChunkSize() == 0) {
        throw IRException("The procedure context carries no chunk size, so a procedure would be "
                          "asked for chunks of no rows");
    }

    const Procedure* procedure = procedures->getProcedure(std::string_view(name.data(), name.size()));
    if (!procedure) {
        throw IRException(fmt::format("Procedure '{}' does not exist", name.str()));
    }

    // A procedure keeps its own state - iterators, tallies - in the data its alloc
    // callback produces, and every callback runs through the execute entry point, so
    // a procedure missing either cannot be called at all.
    const Procedure::AllocCallback alloc = procedure->getAllocCallback();
    if (!alloc || !procedure->getExecCallback()) {
        throw IRException(fmt::format("Procedure '{}' has no alloc or execute callback", name.str()));
    }

    // The slots the procedure reads its arguments from and writes its return values
    // into. Every declared value gets a slot, so a return value this call does not
    // yield stays null and the procedure skips it.
    ProcedureData* procedureData = alloc();
    procedureData->resizeInputColumns(procedure->argumentTypes().size());
    procedureData->resizeReturnColumns(procedure->returnValues().size());

    // The runtime call owns that data from here on - its dealloc callback releases it
    // - and is mapped to the handle, so every op that names the handle drives the same
    // procedure.
    NLProcedureState* state = _program->allocProcedureState(procedure, procedureData, _procedureContext);
    _procedureStates[procedureOp.getState()] = state;

    // Resolve each yielded name to the procedure's own return value index once here,
    // so the ops that bind columns work off indices rather than names.
    for (const mlir::Attribute yield : procedureOp.getYields()) {
        const llvm::StringRef yieldName = mlir::cast<mlir::StringAttr>(yield).getValue();

        state->addYieldIndex(procedure->getReturnValueIndex(std::string_view(yieldName.data(), yieldName.size())));
    }
}

void NLTranslator::bindProcedureInputs(NLProcedureState* state, mlir::ValueRange inputs) {
    // Operand i is argument i, so each argument chunk lands in the slot the procedure
    // reads that argument from. The chunks are the enclosing loop's variables, refilled
    // in place each step, so binding them once here holds for every step. A call that
    // stopped short of the trailing optional arguments leaves their slots at the null
    // the alloc sized them to, which is how the procedure reads an omitted one.
    const Procedure* procedure = state->getProcedure();
    const size_t argumentCount = procedure->argumentTypes().size();
    const size_t requiredCount = procedure->getRequiredArgumentCount();
    const bool tooFewArguments = inputs.size() < requiredCount;
    const bool tooManyArguments = inputs.size() > argumentCount;
    if (tooFewArguments || tooManyArguments) {
        throw IRException(fmt::format("A procedure call passes {} arguments, but the procedure "
                                      "declares {}, {} of them required",
                                      inputs.size(),
                                      argumentCount,
                                      requiredCount));
    }

    ProcedureData* procedureData = state->getData();
    for (size_t inputIndex = 0; inputIndex < inputs.size(); inputIndex++) {
        procedureData->setInputColumn(inputIndex, getColumn(inputs[inputIndex]));
    }
}

void NLTranslator::addProcedureCarriedColumns(const IteratorConfig& config,
                                              mlir::Block& loopBody,
                                              size_t yieldCount,
                                              NLProcedureLoopData* loopData) {
    const llvm::SmallVector<mlir::Value, 4>& carriedColumns = config._carriedColumns;
    const Procedure* procedure = loopData->getState()->getProcedure();

    // A call binding no return value has no row count for a carried row to be replicated
    // against, so nothing can ride through it - it is driven for what it does, not for
    // rows. The db verifier settles this, so reaching it here means hand-written nl IR.
    if (!carriedColumns.empty() && yieldCount == 0) {
        throw IRException(fmt::format("nl.procedure_init carries columns past '{}', which binds no "
                                      "return value, so they could not be aligned with anything",
                                      procedure->getFullName()));
    }

    // Only a procedure that declares it reports the input row behind each row it emits
    // can be carried past: that report is what the carried columns are rebuilt from.
    // Lowering settles this at plan time, so reaching it here means hand-written nl IR.
    if (!carriedColumns.empty() && !procedure->hasIndices()) {
        throw IRException(fmt::format("nl.procedure_init carries columns past '{}', but the "
                                      "procedure does not report the input row of the rows it emits",
                                      procedure->getFullName()));
    }

    for (size_t carriedIndex = 0; carriedIndex < carriedColumns.size(); carriedIndex++) {
        const mlir::Value carried = carriedColumns[carriedIndex];

        // A carried chunk comes back as a loop variable after the yields, and is
        // rebuilt into it each step - the call may repeat or drop its rows, so it
        // cannot be passed through in place.
        const auto loopVariableIndex = static_cast<unsigned>(yieldCount + carriedIndex);
        const mlir::Value loopVariable = loopBody.getArgument(loopVariableIndex);

        Column* output = allocColumnForChunkType(loopVariable.getType());
        _valueSlots[loopVariable] = output;

        const NLCarriedColumn column(getColumn(carried),
                                     output,
                                     selectGatherForChunkType(loopVariable.getType()));
        loopData->addCarriedColumn(column);
    }

    if (carriedColumns.empty()) {
        return;
    }

    // The procedure reports the input row behind each row it emits only when something
    // is carried past the call; hand it the map the loop gathers those columns through,
    // reserving a chunk so the reporting stays allocation-free.
    ColumnIndices* indices = loopData->getIndices();
    indices->reserve(_program->getChunkSize());

    ProcedureData* data = loopData->getState()->getData();
    IndexedProcedureData* indexedData = dynamic_cast<IndexedProcedureData*>(data);

    bioassert(indexedData,
              "Procedure '{}' is expected to have indices but has no indices data",
              procedure->getFullName());

    indexedData->setIndices(indices);
}

void NLTranslator::translateProcedureInitLoop(const IteratorConfig& config,
                                              mlir::Block& loopBody,
                                              NLLimitState* limit,
                                              NLStmtContainer* body) {
    NLProcedureState* state = config._procedureState;
    if (!state) {
        throw IRException("nl.procedure_init iterator must carry a procedure call");
    }

    bindProcedureInputs(state, config._procedureInputs);

    // For::verify binds one loop variable per iterator chunk, and a drive iterator's
    // chunks are the yielded return values followed by the carried ones. The yields are
    // the columns the procedure fills, so those loop variables are its result columns -
    // each step rewrites them in place, with no gather and no copy.
    const size_t yieldCount = state->yieldIndices().size();
    const size_t carriedCount = config._carriedColumns.size();
    if (loopBody.getNumArguments() != yieldCount + carriedCount) {
        throw IRException(fmt::format("An nl.procedure_init loop binds {} variables, but its call "
                                      "yields {} return values and carries {} columns",
                                      loopBody.getNumArguments(),
                                      yieldCount,
                                      carriedCount));
    }

    bindProcedureResults(state, loopBody.getArguments().take_front(yieldCount));

    NLProcedureLoopData* loopData = _program->allocFunctionData<NLProcedureLoopData>(state);
    loopData->setLimit(limit);

    // The carried columns come back as the trailing loop variables, rebuilt each step
    // from the input rows the procedure reports.
    addProcedureCarriedColumns(config, loopBody, yieldCount, loopData);

    body->emplaceStmt(&NLExecutor::runProcedureInitLoop, loopData);

    translateBlock(loopBody, loopData->getStmts());
}

NLProcedureState* NLTranslator::procedureStateFor(mlir::Value handle) const {
    const auto stateIt = _procedureStates.find(handle);
    if (stateIt == _procedureStates.end()) {
        throw IRException("procedure handle must be produced by an nl.procedure");
    }

    return stateIt->second;
}

void NLTranslator::bindProcedureResults(NLProcedureState* state, mlir::ValueRange chunks) {
    // One op binds the call's result columns - the drive loop - so a second one would
    // allocate a rival set of columns the procedure no longer writes into.
    if (!state->resultColumns().empty()) {
        throw IRException("A procedure call binds its result chunks once, but a second operation "
                          "names the same handle");
    }

    const std::vector<size_t>& yieldIndices = state->yieldIndices();
    if (chunks.size() != yieldIndices.size()) {
        throw IRException(fmt::format("A procedure call produces {} chunks, but its nl.procedure "
                                      "yields {} return values",
                                      chunks.size(),
                                      yieldIndices.size()));
    }

    const Procedure* procedure = state->getProcedure();
    ProcedureData* procedureData = state->getData();

    for (size_t yieldIndex = 0; yieldIndex < yieldIndices.size(); yieldIndex++) {
        const size_t returnIndex = yieldIndices[yieldIndex];
        Column* column = allocColumnForProcedureType(procedure->getReturnValueType(returnIndex));

        // The procedure writes through the slot its own declaration order names; the
        // engine reads the same column through the chunk value, and the ordered list
        // on the call sizes each step's rows.
        procedureData->setReturnColumn(returnIndex, column);
        state->addResultColumn(column);
        _valueSlots[chunks[yieldIndex]] = column;
    }
}

Column* NLTranslator::allocColumnForProcedureType(ProcedureType procedureType) {
    const size_t chunkSize = _program->getChunkSize();

    // The column type each declared return type is written through, matching the
    // pipeline engine's allocReturnValues: the procedure static_casts its return
    // column to exactly this type, so the two must not drift.
    switch (procedureType) {
        case ProcedureType::NODE:
            return allocPlainChunkColumn<NodeID>(_memory, chunkSize);
        break;

        case ProcedureType::EDGE:
            return allocPlainChunkColumn<EdgeID>(_memory, chunkSize);
        break;

        case ProcedureType::LABEL_ID:
            return allocPlainChunkColumn<LabelID>(_memory, chunkSize);
        break;

        case ProcedureType::EDGE_TYPE_ID:
            return allocPlainChunkColumn<EdgeTypeID>(_memory, chunkSize);
        break;

        case ProcedureType::PROPERTY_TYPE_ID:
            return allocPlainChunkColumn<PropertyTypeID>(_memory, chunkSize);
        break;

        case ProcedureType::VALUE_TYPE:
            return allocPlainChunkColumn<ValueType>(_memory, chunkSize);
        break;

        case ProcedureType::UINT_64:
            return allocPlainChunkColumn<types::UInt64::Primitive>(_memory, chunkSize);
        break;

        case ProcedureType::INT64:
            return allocPlainChunkColumn<types::Int64::Primitive>(_memory, chunkSize);
        break;

        case ProcedureType::DOUBLE:
            return allocPlainChunkColumn<types::Double::Primitive>(_memory, chunkSize);
        break;

        case ProcedureType::BOOL:
            return allocPlainChunkColumn<types::Bool::Primitive>(_memory, chunkSize);
        break;

        case ProcedureType::STRING_VIEW:
            return allocPlainChunkColumn<types::String::Primitive>(_memory, chunkSize);
        break;

        case ProcedureType::STRING:
            return allocPlainChunkColumn<std::string>(_memory, chunkSize);
        break;

        case ProcedureType::LIST:
            return allocPlainChunkColumn<ListView>(_memory, chunkSize);
        break;

        case ProcedureType::INVALID:
        case ProcedureType::_SIZE:
            throw IRException("Invalid procedure return type");
        break;
    }

    throw IRException("Unhandled procedure return type");
}

// An ID chunk allocates an ID column on its kind; a !storage.nullable<...> chunk
// allocates a ColumnOptVector on its value type; a ui64 count chunk a
// ColumnVector<uint64_t>. Mirrors addCrossColumn's split.
Column* NLTranslator::allocColumnForChunkType(mlir::Type chunkType) {
    const auto chunk = mlir::cast<nl::ChunkType>(chunkType);
    const mlir::Type elementType = chunk.getElementType();

    if (const auto nullableType = mlir::dyn_cast<storage::NullableType>(elementType)) {
        const ValueType valueType = valueTypeFromElementType(nullableType.getValueType());
        return allocOptColumnForValueType(valueType);
    } else if (mlir::isa<storage::ListElementType>(elementType)) {
        return allocListElementColumn();
    }

    if (isPlainValueElementType(elementType)) {
        return allocPlainColumn(valueTypeFromElementType(elementType));
    }

    if (llvm::isa<storage::ListType>(elementType)) {
        return allocListColumn();
    }

    return allocColumnForKind(chunkKindFromElementType(elementType));
}

NLAppendFunction NLTranslator::selectAppendForChunkType(mlir::Type chunkType) {
    const auto chunk = mlir::cast<nl::ChunkType>(chunkType);
    const mlir::Type elementType = chunk.getElementType();

    if (const auto nullableType = mlir::dyn_cast<storage::NullableType>(elementType)) {
        const ValueType valueType = valueTypeFromElementType(nullableType.getValueType());
        return NLExecutor::selectOptAppendFunction(valueType);
    } else if (mlir::isa<storage::ListElementType>(elementType)) {
        return NLExecutor::selectListElementAppendFunction();
    }

    if (isPlainValueElementType(elementType)) {
        return NLExecutor::selectPlainAppendFunction(valueTypeFromElementType(elementType));
    }

    return NLExecutor::selectAppendFunction(chunkKindFromElementType(elementType));
}

NLGatherFunction NLTranslator::selectGatherForChunkType(mlir::Type chunkType) {
    const auto chunk = mlir::cast<nl::ChunkType>(chunkType);
    const mlir::Type elementType = chunk.getElementType();

    if (const auto nullableType = mlir::dyn_cast<storage::NullableType>(elementType)) {
        const ValueType valueType = valueTypeFromElementType(nullableType.getValueType());
        return NLExecutor::selectOptGatherFunction(valueType);
    } else if (mlir::isa<storage::ListElementType>(elementType)) {
        return NLExecutor::selectListElementGatherFunction();
    }

    if (isPlainValueElementType(elementType)) {
        return NLExecutor::selectPlainGatherFunction(valueTypeFromElementType(elementType));
    }

    return NLExecutor::selectGatherFunction(chunkKindFromElementType(elementType));
}

NLCompareFunction NLTranslator::selectCompareForChunkType(mlir::Type chunkType) {
    const auto chunk = mlir::cast<nl::ChunkType>(chunkType);
    const mlir::Type elementType = chunk.getElementType();

    if (const auto nullableType = mlir::dyn_cast<storage::NullableType>(elementType)) {
        const ValueType valueType = valueTypeFromElementType(nullableType.getValueType());
        return NLExecutor::selectOptCompareFunction(valueType);
    } else if (mlir::isa<storage::ListElementType>(elementType)) {
        return NLExecutor::selectListElementCompareFunction();
    }

    if (isPlainValueElementType(elementType)) {
        return NLExecutor::selectPlainCompareFunction(valueTypeFromElementType(elementType));
    }

    if (llvm::isa<storage::ListType>(elementType)) {
        return NLExecutor::selectListCompareFunction();
    }

    return NLExecutor::selectCompareFunction(chunkKindFromElementType(elementType));
}

NLKeyAppendFunction NLTranslator::selectKeyAppendForChunkType(mlir::Type chunkType) {
    const auto chunk = mlir::cast<nl::ChunkType>(chunkType);
    const mlir::Type elementType = chunk.getElementType();

    if (const auto nullableType = mlir::dyn_cast<storage::NullableType>(elementType)) {
        const ValueType valueType = valueTypeFromElementType(nullableType.getValueType());
        return NLExecutor::selectOptKeyAppendFunction(valueType);
    } else if (mlir::isa<storage::ListElementType>(elementType)) {
        return NLExecutor::selectListElementKeyAppendFunction();
    }

    if (isPlainValueElementType(elementType)) {
        return NLExecutor::selectPlainKeyAppendFunction(valueTypeFromElementType(elementType));
    }

    return NLExecutor::selectKeyAppendFunction(chunkKindFromElementType(elementType));
}

// The value-reduction sibling of selectCountForChunkType: a type-erased column of tagged
// cells is folded cell by cell, every other one through the value type it shares.
NLAggregateUpdateFunction NLTranslator::selectAggregateUpdateForChunkType(AggregateKind kind,
                                                                         mlir::Type chunkType) {
    const auto chunk = mlir::cast<nl::ChunkType>(chunkType);
    if (mlir::isa<storage::ListElementType>(chunk.getElementType())) {
        return NLExecutor::selectTaggedAggregateUpdate(kind);
    }

    return NLExecutor::selectAggregateUpdate(kind, nullableChunkValueType(chunkType));
}

NLCountFunction NLTranslator::selectCountForChunkType(mlir::Type chunkType) {
    const auto chunk = mlir::cast<nl::ChunkType>(chunkType);
    const mlir::Type elementType = chunk.getElementType();

    if (const auto nullableType = mlir::dyn_cast<storage::NullableType>(elementType)) {
        const ValueType valueType = valueTypeFromElementType(nullableType.getValueType());
        return NLExecutor::selectOptCountFunction(valueType);
    } else if (mlir::isa<storage::ListElementType>(elementType)) {
        return NLExecutor::selectListElementCountFunction();
    }

    return &NLExecutor::countAllRows;
}

NLGroupKeyGatherFunction NLTranslator::selectGroupKeyGatherForChunkType(mlir::Type chunkType) {
    const auto chunk = mlir::cast<nl::ChunkType>(chunkType);
    const mlir::Type elementType = chunk.getElementType();

    if (const auto nullableType = mlir::dyn_cast<storage::NullableType>(elementType)) {
        const ValueType valueType = valueTypeFromElementType(nullableType.getValueType());
        return NLExecutor::selectOptGroupKeyGather(valueType);
    } else if (mlir::isa<storage::ListElementType>(elementType)) {
        return NLExecutor::selectListElementGroupKeyGatherFunction();
    }

    if (isPlainValueElementType(elementType)) {
        return NLExecutor::selectPlainGroupKeyGather(valueTypeFromElementType(elementType));
    }

    return NLExecutor::selectGroupKeyGather(chunkKindFromElementType(elementType));
}

NLCopyFunction NLTranslator::selectCopyForChunkType(mlir::Type chunkType) {
    const auto chunk = mlir::cast<nl::ChunkType>(chunkType);
    const mlir::Type elementType = chunk.getElementType();

    if (const auto nullableType = mlir::dyn_cast<storage::NullableType>(elementType)) {
        const ValueType valueType = valueTypeFromElementType(nullableType.getValueType());
        return NLExecutor::selectOptCopyFunction(valueType);
    } else if (mlir::isa<storage::ListElementType>(elementType)) {
        return NLExecutor::selectListElementCopyFunction();
    }

    if (isPlainValueElementType(elementType)) {
        return NLExecutor::selectPlainCopyFunction(valueTypeFromElementType(elementType));
    }

    return NLExecutor::selectCopyFunction(chunkKindFromElementType(elementType));
}

// An ID chunk (a grouping key) allocates an ID column on its kind; a
// !storage.nullable<...> chunk (a key or a sum/min/max/avg result) a
// ColumnOptVector on its value type; a ui64 chunk (a count result) a
// ColumnVector<uint64_t> directly, as nl.count_result's output is allocated.
Column* NLTranslator::allocColumnForResultChunkType(mlir::Type chunkType) {
    const auto chunk = mlir::cast<nl::ChunkType>(chunkType);
    const mlir::Type elementType = chunk.getElementType();

    if (const auto nullableType = mlir::dyn_cast<storage::NullableType>(elementType)) {
        const ValueType valueType = valueTypeFromElementType(nullableType.getValueType());
        return allocOptColumnForValueType(valueType);
    } else if (mlir::isa<storage::ListElementType>(elementType)) {
        return allocListElementColumn();
    }

    if (isPlainValueElementType(elementType)) {
        return allocPlainColumn(valueTypeFromElementType(elementType));
    }

    // A list chunk (the nl.collect drain's per-group cell) is a column of ListViews,
    // each spanning that group's run in the accumulator's list buffer.
    if (llvm::isa<storage::ListType>(elementType)) {
        return allocListColumn();
    }

    return allocColumnForKind(chunkKindFromElementType(elementType));
}

void NLTranslator::translateCrossProduct(nl::CrossProduct cross, NLStmtContainer* body) {
    const mlir::OperandRange outerColumns = cross.getOuterColumns();
    const mlir::OperandRange innerColumns = cross.getInnerColumns();

    // At run time runCrossProduct takes N from the first outer column and M from
    // the first inner column, so a side with no column cannot be sized. Reject
    // that here: DBLowering never emits it, but the nl IR may come from elsewhere.
    if (outerColumns.empty() || innerColumns.empty()) {
        throw IRException("nl.cross_product needs at least one column on each side");
    }

    NLCrossProductData* data = _program->allocFunctionData<NLCrossProductData>();

    // The optional limit handle is a separate operand group, so it never appears
    // among the columns; null leaves the product unbounded.
    data->setLimit(limitStateFor(cross.getLimit()));

    // The results are the outer columns followed by the inner, the order
    // inferReturnTypes lays them out, so walk the result list in step.
    const mlir::ResultRange results = cross.getResults();
    size_t resultIndex = 0;

    for (const mlir::Value column : outerColumns) {
        addCrossColumn(column, results[resultIndex], /*isOuter=*/true, data);
        resultIndex++;
    }

    for (const mlir::Value column : innerColumns) {
        addCrossColumn(column, results[resultIndex], /*isOuter=*/false, data);
        resultIndex++;
    }

    body->emplaceStmt(&NLExecutor::runCrossProduct, data);
}

void NLTranslator::addCrossColumn(mlir::Value inputValue,
                                  mlir::Value resultValue,
                                  bool isOuter,
                                  NLCrossProductData* data) {
    const Column* input = getColumn(inputValue);

    const auto chunkType = mlir::cast<nl::ChunkType>(inputValue.getType());
    const mlir::Type elementType = chunkType.getElementType();

    // The output keeps the input's element type, only the row count changes. A
    // nullable value chunk allocates a ColumnOptVector and broadcasts on its
    // value type; a list_element chunk allocates a column of tagged scalars, which
    // carry their own type; an ID chunk allocates on its chunk kind.
    Column* output = nullptr;
    NLBroadcastFunction broadcast = nullptr;

    // A constant column holds one value standing for every row, so repeating it block-wise
    // and tiling it give the same column: the one broadcast serves both sides.
    if (isConstantColumn(input)) {
        output = _memory->allocSame(input);
        broadcast = NLExecutor::selectConstBlockRepeatFunction();
    } else if (const auto nullableType = mlir::dyn_cast<storage::NullableType>(elementType)) {
        const ValueType valueType = valueTypeFromElementType(nullableType.getValueType());
        output = allocOptColumnForValueType(valueType);
        broadcast = isOuter ? NLExecutor::selectOptBlockRepeatFunction(valueType)
                            : NLExecutor::selectOptTileFunction(valueType);
    } else if (mlir::isa<storage::ListElementType>(elementType)) {
        output = allocListElementColumn();
        broadcast = isOuter ? NLExecutor::selectListElementBlockRepeatFunction()
                            : NLExecutor::selectListElementTileFunction();
    } else if (isPlainValueElementType(elementType)) {
        const ValueType valueType = valueTypeFromElementType(elementType);
        output = allocPlainColumn(valueType);
        broadcast = isOuter ? NLExecutor::selectPlainBlockRepeatFunction(valueType)
                            : NLExecutor::selectPlainTileFunction(valueType);
    } else {
        const NLChunkKind kind = getChunkKind(chunkType);
        output = allocColumnForKind(kind);
        broadcast = isOuter ? NLExecutor::selectBlockRepeatFunction(kind)
                            : NLExecutor::selectTileFunction(kind);
    }

    _valueSlots[resultValue] = output;

    const NLCrossColumn crossColumn(input, output, broadcast);
    if (isOuter) {
        data->addOuterColumn(crossColumn);
    } else {
        data->addInnerColumn(crossColumn);
    }
}

Column* NLTranslator::allocColumn(mlir::Value chunkValue) {
    Column* column = allocColumnForChunkType(chunkValue.getType());
    _valueSlots[chunkValue] = column;
    return column;
}

Column* NLTranslator::allocColumnIfUsed(mlir::Value chunkValue) {
    if (chunkValue.use_empty()) {
        return nullptr;
    }

    return allocColumn(chunkValue);
}

// Pool-allocate a chunk column of the right concrete type from the external
// arena, reserving a full chunk so execution stays allocation-free
Column* NLTranslator::allocColumnForKind(NLChunkKind kind) {
    const size_t chunkSize = _program->getChunkSize();

    Column* column = nullptr;
    dispatchChunkKind(kind, [&]<typename ElementType>() {
        column = allocPlainChunkColumn<ElementType>(_memory, chunkSize);
    });

    return column;
}

// Pool-allocate a nullable value column (ColumnOptVector) of the right primitive
// for a property's value type, reserving a full chunk so execution stays
// allocation-free. The value type was baked into the chunk during lowering and
// re-resolved from the schema here.
Column* NLTranslator::allocOptColumnForValueType(ValueType valueType) {
    // Per-step loop columns reserve a full chunk so execution stays
    // allocation-free.
    return allocOptColumn(valueType, _program->getChunkSize());
}

Column* NLTranslator::allocSingleRowOptColumnForValueType(ValueType valueType) {
    // The aggregate accumulator only ever holds one row (reset assign(1, ...),
    // update rewrites .front()), so reserve a single element rather than a chunk
    // it would never fill.
    return allocOptColumn(valueType, 1);
}

// A plain value element type: a 64-bit number carried without a nullable wrapper. A
// tally is one (a ui64 that is never null) and so is an expression over it (a signed
// i64, or an f64 once a double takes part). A width-1 integer is a mask, not one of
// these, and an ID or list element is its own family.
bool NLTranslator::isPlainValueElementType(mlir::Type elementType) {
    if (mlir::isa<mlir::Float64Type>(elementType)) {
        return true;
    }

    const auto intType = mlir::dyn_cast<mlir::IntegerType>(elementType);

    return intType && intType.getWidth() == 64;
}

Column* NLTranslator::allocPlainColumn(ValueType valueType) {
    const size_t chunkSize = _program->getChunkSize();

    switch (valueType) {
        case ValueType::Int64: {
            ColumnVector<types::Int64::Primitive>* column = _memory->alloc<ColumnVector<types::Int64::Primitive>>();
            column->reserve(chunkSize);
            return column;
        }
        break;

        case ValueType::UInt64:
            return allocCountColumn();
        break;

        case ValueType::Double: {
            ColumnVector<types::Double::Primitive>* column = _memory->alloc<ColumnVector<types::Double::Primitive>>();
            column->reserve(chunkSize);
            return column;
        }
        break;

        default:
            throw IRException("a plain value column must be numeric");
        break;
    }
}

ColumnVector<uint64_t>* NLTranslator::allocCountColumn() {
    ColumnVector<uint64_t>* column = _memory->alloc<ColumnVector<uint64_t>>();
    column->reserve(_program->getChunkSize());

    return column;
}

Column* NLTranslator::allocListColumn() {
    ColumnVector<ListView>* column = _memory->alloc<ColumnVector<ListView>>();
    column->reserve(_program->getChunkSize());

    return column;
}

Column* NLTranslator::allocListElementColumn() {
    ColumnVector<ListElementView>* column = _memory->alloc<ColumnVector<ListElementView>>();
    column->reserve(_program->getChunkSize());

    return column;
}

Column* NLTranslator::allocOptColumn(ValueType valueType, size_t reserveSize) {
    // Allocate a ColumnOptVector for the value type's primitive, reserving
    // reserveSize optionals up front; ValueTypeDispatcher maps the runtime value
    // type to that compile-time primitive.
    Column* column = nullptr;
    const auto allocate = [&]<SupportedType T>() {
        ColumnOptVector<typename T::Primitive>* typed = _memory->alloc<ColumnOptVector<typename T::Primitive>>();
        typed->reserve(reserveSize);
        column = typed;
    };
    ValueTypeDispatcher(valueType).execute(allocate);

    return column;
}

Column* NLTranslator::getColumn(mlir::Value chunkValue) const {
    // Get allocated column for a given MLIR Value
    const auto slotIt = _valueSlots.find(chunkValue);
    if (slotIt == _valueSlots.end()) {
        throw IRException("Chunk value must be a loop variable of an enclosing nl.for");
    }

    return slotIt->second;
}

NLChunkKind NLTranslator::getChunkKind(mlir::Type chunkType) {
    const auto chunk = mlir::dyn_cast<nl::ChunkType>(chunkType);
    if (!chunk) {
        throw IRException("Expected an !nl.chunk type");
    }

    return chunkKindFromElementType(chunk.getElementType());
}

NLChunkKind NLTranslator::chunkKindFromElementType(mlir::Type elementType) {
    // The ID chunks a scan or a hop binds, then the element types a CALL's yielded
    // columns carry - so a yielded column is crossed and carried like any other. The
    // integer widths are read the way a nullable value chunk's are: unsigned for a ui64,
    // one bit for a bool.
    if (mlir::isa<storage::NodeIDType>(elementType)) {
        return NLChunkKind::NodeID;
    } else if (mlir::isa<storage::EdgeIDType>(elementType)) {
        return NLChunkKind::EdgeID;
    } else if (mlir::isa<storage::EdgeTypeIDType>(elementType)) {
        return NLChunkKind::EdgeTypeID;
    } else if (mlir::isa<storage::LabelIDType>(elementType)) {
        return NLChunkKind::LabelID;
    } else if (mlir::isa<storage::PropertyTypeIDType>(elementType)) {
        return NLChunkKind::PropertyTypeID;
    } else if (mlir::isa<storage::ValueTypeType>(elementType)) {
        return NLChunkKind::ValueTypeCode;
    } else if (mlir::isa<storage::StringType>(elementType)) {
        return NLChunkKind::String;
    } else if (mlir::isa<storage::OwnedStringType>(elementType)) {
        return NLChunkKind::OwnedString;
    } else if (mlir::isa<storage::ListType>(elementType)) {
        return NLChunkKind::List;
    } else if (mlir::isa<storage::BoolType>(elementType)) {
        return NLChunkKind::Bool;
    } else if (mlir::isa<mlir::Float64Type>(elementType)) {
        return NLChunkKind::Double;
    } else if (const auto intType = mlir::dyn_cast<mlir::IntegerType>(elementType)) {
        if (intType.getWidth() == 1) {
            return NLChunkKind::Bool;
        } else if (intType.isUnsigned()) {
            return NLChunkKind::UInt64;
        }

        return NLChunkKind::Int64;
    }

    throw IRException("Unsupported chunk element type");
}
