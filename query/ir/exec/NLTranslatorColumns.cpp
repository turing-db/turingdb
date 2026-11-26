#include "NLTranslator.h"

#include <string>
#include <string_view>
#include <utility>

#include <spdlog/fmt/bundled/format.h>

#include "mlir/IR/BuiltinTypes.h"
#include "llvm/ADT/DenseMap.h"
#include "llvm/Support/raw_ostream.h"

#include "columns/ColumnConst.h"
#include "columns/ColumnOptVector.h"
#include "list/ListView.h"
#include "metadata/PropertyNull.h"
#include "metadata/PropertyType.h"

#include "NLRuntimeProperty.h"
#include "NLTranslatorChunkTypes.h"

#include "LocalMemory.h"
#include "IRException.h"
#include "BioAssert.h"

using namespace db;

namespace nl = mlir::nl;
namespace storage = mlir::storage;

namespace {

// Pool-allocate a plain (never-null) chunk column of the given element type, reserving a
// full chunk so execution stays allocation-free. Every such column - an ID chunk, or one a
// procedure yielded - is a ColumnVector, so the element type is all that varies.
template <typename T>
Column* allocPlainChunkColumn(LocalMemory* memory, size_t chunkSize) {
    ColumnVector<T>* column = memory->alloc<ColumnVector<T>>();
    column->reserve(chunkSize);
    return column;
}

template <typename T>
Column* allocOptChunkColumn(LocalMemory* memory, size_t chunkSize) {
    ColumnOptVector<T>* column = memory->alloc<ColumnOptVector<T>>();
    column->reserve(chunkSize);
    return column;
}

template <typename T>
Column* allocProcedureChunkColumn(LocalMemory* memory, size_t chunkSize, bool nullable) {
    if (nullable) {
        return allocOptChunkColumn<T>(memory, chunkSize);
    }

    return allocPlainChunkColumn<T>(memory, chunkSize);
}

template <template <typename> typename Container, size_t... Indices>
bool isNullHoldingColumn(ColumnKind::Code kind, std::index_sequence<Indices...>) {
    return ((Container<InternalKind::Types::TypeAt<Indices>>::IS_NULLABLE
             && kind == Container<InternalKind::Types::TypeAt<Indices>>::staticKind())
            || ...);
}

bool columnHoldsNulls(const Column* column) {
    const ColumnKind::Code kind = column->getKind();
    const std::make_index_sequence<InternalKind::Count> internalKinds;

    return column->getContainerKind() == ContainerKind::code<ColumnOptVector>()
        || isNullHoldingColumn<ColumnConst>(kind, internalKinds)
        || kind == ColumnVector<PropertyNull>::staticKind()
        || kind == ColumnConst<PropertyNull>::staticKind();
}

}

Column* NLTranslator::allocColumnForProcedureType(const NamedProcedureType& returnValue) {
    const size_t chunkSize = _program->getChunkSize();
    const bool nullable = returnValue._nullable;

    // The column type each declared return type is written through, matching the
    // pipeline engine's allocReturnValues: the procedure static_casts its return
    // column to exactly this type, so the two must not drift.
    switch (returnValue._type) {
        case ProcedureType::NODE:
            return allocPlainChunkColumn<NodeID>(_memory, chunkSize);
        break;

        case ProcedureType::EDGE:
            return allocPlainChunkColumn<EdgeID>(_memory, chunkSize);
        break;

        case ProcedureType::LABEL_ID:
            return allocProcedureChunkColumn<LabelID>(_memory, chunkSize, nullable);
        break;

        case ProcedureType::EDGE_TYPE_ID:
            return allocProcedureChunkColumn<EdgeTypeID>(_memory, chunkSize, nullable);
        break;

        case ProcedureType::PROPERTY_TYPE_ID:
            return allocProcedureChunkColumn<PropertyTypeID>(_memory, chunkSize, nullable);
        break;

        case ProcedureType::VALUE_TYPE:
            return allocProcedureChunkColumn<ValueType>(_memory, chunkSize, nullable);
        break;

        case ProcedureType::UINT_64:
            return allocProcedureChunkColumn<types::UInt64::Primitive>(_memory, chunkSize, nullable);
        break;

        case ProcedureType::INT64:
            return allocProcedureChunkColumn<types::Int64::Primitive>(_memory, chunkSize, nullable);
        break;

        case ProcedureType::DOUBLE:
            return allocProcedureChunkColumn<types::Double::Primitive>(_memory, chunkSize, nullable);
        break;

        case ProcedureType::BOOL:
            return allocProcedureChunkColumn<types::Bool::Primitive>(_memory, chunkSize, nullable);
        break;

        case ProcedureType::STRING_VIEW:
            return allocProcedureChunkColumn<types::String::Primitive>(_memory, chunkSize, nullable);
        break;

        case ProcedureType::LIST:
            return allocProcedureChunkColumn<ListView>(_memory, chunkSize, nullable);
        break;

        case ProcedureType::MAP:
            throw IRException("Unsupported procedure return type: MAP");
        break;

        case ProcedureType::INVALID:
        case ProcedureType::_SIZE:
            throw IRException("Invalid procedure return type");
        break;
    }

    throw IRException("Unhandled procedure return type");
}

// A !storage.nullable<...> chunk allocates a ColumnOptVector on the kind it wraps, and
// any other chunk a column of its own kind. Mirrors addCrossColumn's split.
Column* NLTranslator::allocColumnForChunkType(mlir::Type chunkType) {
    const auto chunk = mlir::cast<nl::ChunkType>(chunkType);
    const mlir::Type elementType = chunk.getElementType();

    if (isNullableList(elementType)) {
        return allocOptListColumn();
    }

    if (const auto nullableType = mlir::dyn_cast<storage::NullableType>(elementType)) {
        return allocOptColumnForKind(chunkKindFromNullableValue(nullableType.getValueType()));
    } else if (mlir::isa<storage::AnyType>(elementType)) {
        return allocCellColumn();
    }

    return allocColumnForKind(chunkKindFromElementType(elementType));
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

void NLTranslator::bindKernelResult(mlir::Value resultValue, Column* result) {
    bioassert(result, "The kernel allocated no result column");

    const mlir::Type resultType = resultValue.getType();
    const bool declaredNullable = isNullableChunkType(resultType);

    const mlir::Type resultElement = mlir::cast<nl::ChunkType>(resultType).getElementType();
    const bool declaredBooleans = mlir::isa<storage::BoolType>(resultElement) || resultElement.isInteger(1);
    const bool writesBooleans = holdsTruthColumn(NLTruthColumnKind::Booleans, result);

    if (declaredNullable != columnHoldsNulls(result) || declaredBooleans != writesBooleans) {
        std::string declaredType;
        llvm::raw_string_ostream stream(declaredType);
        resultType.print(stream);

        const llvm::StringRef opName = resultValue.getDefiningOp()->getName().getStringRef();
        throw IRException(fmt::format("{} declares a {} result, but its kernel writes a {}",
                                      std::string_view(opName.data(), opName.size()),
                                      declaredType,
                                      result->getTypeName()));
    }

    _valueSlots[resultValue] = result;
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

Column* NLTranslator::allocOptColumnForKind(NLChunkKind kind) {
    const size_t chunkSize = _program->getChunkSize();

    Column* column = nullptr;
    dispatchOptChunkKind(kind, [&]<typename Element>() { column = allocOptChunkColumn<Element>(_memory, chunkSize); });

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

bool NLTranslator::isEntityIDElement(mlir::Type elementType) {
    return mlir::isa<storage::NodeIDType>(elementType) || mlir::isa<storage::EdgeIDType>(elementType);
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

Column* NLTranslator::allocOptListColumn() {
    ColumnOptVector<ListView>* column = _memory->alloc<ColumnOptVector<ListView>>();
    column->reserve(_program->getChunkSize());

    return column;
}

Column* NLTranslator::allocCellColumn() {
    ColumnVector<CellView>* column = _memory->alloc<ColumnVector<CellView>>();
    column->reserve(_program->getChunkSize());

    return column;
}

Column* NLTranslator::allocConstCellColumn() {
    return _memory->alloc<ColumnConst<CellView>>();
}

Column* NLTranslator::allocOptColumn(ValueType valueType, size_t reserveSize) {
    return allocOptValueColumn(_memory, valueType, reserveSize);
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
    } else if (mlir::isa<storage::LabelSetIDType>(elementType)) {
        return NLChunkKind::LabelSetID;
    } else if (mlir::isa<storage::LabelIDType>(elementType)) {
        return NLChunkKind::LabelID;
    } else if (mlir::isa<storage::PropertyTypeIDType>(elementType)) {
        return NLChunkKind::PropertyTypeID;
    } else if (mlir::isa<storage::ValueTypeType>(elementType)) {
        return NLChunkKind::ValueTypeCode;
    } else if (mlir::isa<storage::StringType>(elementType)) {
        return NLChunkKind::String;
    } else if (mlir::isa<storage::EmbeddingType>(elementType)) {
        return NLChunkKind::Embedding;
    } else if (mlir::isa<storage::ListType>(elementType)) {
        return NLChunkKind::List;
    } else if (mlir::isa<storage::MapType>(elementType)) {
        return NLChunkKind::Map;
    } else if (mlir::isa<storage::PathType>(elementType)) {
        return NLChunkKind::Path;
    } else if (mlir::isa<storage::DateTimeType>(elementType)) {
        return NLChunkKind::DateTime;
    } else if (mlir::isa<storage::DurationType>(elementType)) {
        return NLChunkKind::Duration;
    } else if (mlir::isa<storage::PointType>(elementType)) {
        return NLChunkKind::Point;
    } else if (mlir::isa<storage::PathRefType>(elementType)) {
        return NLChunkKind::PathRef;
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

NLChunkKind NLTranslator::getNullableChunkKind(mlir::Type chunkType) {
    const auto chunk = mlir::cast<nl::ChunkType>(chunkType);
    const auto nullableType = mlir::dyn_cast<storage::NullableType>(chunk.getElementType());
    if (!nullableType) {
        throw IRException("Expected a nullable chunk");
    }

    return chunkKindFromNullableValue(nullableType.getValueType());
}

NLChunkKind NLTranslator::chunkKindFromNullableValue(mlir::Type valueType) {
    // The null literal names no type of its own, so its column is laid out the way
    // valueTypeFromElementType lays it out: as a null integer
    if (mlir::isa<mlir::NoneType>(valueType)) {
        return NLChunkKind::Int64;
    }

    return chunkKindFromElementType(valueType);
}
