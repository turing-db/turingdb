#include "NLTranslator.h"

#include <span>
#include <string_view>

#include "NLOps.h"

#include "IRConstantColumn.h"
#include "NullLiteralType.h"
#include "list/ListBuffer.h"
#include "map/MapBuffer.h"
#include "map/MapWriteCursor.h"
#include "mlir/IR/BuiltinAttributes.h"
#include "mlir/IR/BuiltinTypes.h"
#include "llvm/ADT/DenseMap.h"

#include "columns/ColumnConst.h"
#include "list/ListView.h"
#include "metadata/PropertyNull.h"
#include "metadata/PropertyType.h"

#include "NLRuntimeProperty.h"
#include "NLExecutor.h"
#include "NLTranslatorChunkTypes.h"

#include "LocalMemory.h"
#include "IRException.h"
#include "BioAssert.h"

using namespace db;

namespace nl = mlir::nl;
namespace storage = mlir::storage;

namespace {

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
        } else if (mlir::isa<mlir::DictionaryAttr>(element)) {
            valueBytes += sizeof(MapView);
        } else {
            throw IRException("Unsupported literal attribute in a constant list");
        }
    }

    return valueBytes;
}

// The bytes the values of a literal map's entries occupy, key and tag bytes excluded - what
// MapBuffer::reserveMap sizes the region from. Each entry stores the same value object the
// write below hands it, so the two walks must recognise the same attribute kinds in the same
// order.
size_t NLTranslator::mapValueBytes(mlir::DictionaryAttr entries) {
    size_t valueBytes = 0;

    for (const mlir::NamedAttribute entry : entries) {
        const mlir::Attribute value = entry.getValue();

        // BoolAttr is an i1 IntegerAttr, so it must be tested before IntegerAttr; an
        // i64 literal falls through to the integer case.
        if (mlir::isa<mlir::BoolAttr>(value)) {
            valueBytes += sizeof(types::Bool::Primitive);
        } else if (mlir::isa<mlir::IntegerAttr>(value)) {
            valueBytes += sizeof(types::Int64::Primitive);
        } else if (mlir::isa<mlir::FloatAttr>(value)) {
            valueBytes += sizeof(types::Double::Primitive);
        } else if (mlir::isa<mlir::StringAttr>(value)) {
            valueBytes += sizeof(types::String::Primitive);
        } else if (mlir::isa<mlir::DenseF32ArrayAttr>(value)) {
            valueBytes += sizeof(types::Embedding::Primitive);
        } else if (mlir::isa<mlir::ArrayAttr>(value)) {
            valueBytes += sizeof(ListView);
        } else if (mlir::isa<mlir::DictionaryAttr>(value)) {
            // A nested map is one entry of this one, storing the child's view
            valueBytes += sizeof(MapView);
        } else if (mlir::isa<mlir::UnitAttr>(value)) {
            valueBytes += sizeof(PropertyNull);
        } else {
            throw IRException("Unsupported literal attribute in a constant map");
        }
    }

    return valueBytes;
}

std::string_view NLTranslator::ownedCharacters(llvm::StringRef text) {
    return _memory->stringBuffer().insert(text);
}

types::Embedding::Primitive NLTranslator::ownedFloats(llvm::ArrayRef<float> embedding) {
    return _memory->embeddingBuffer().insert(std::span<const float> {embedding.data(), embedding.size()});
}

MapView NLTranslator::materializeMapView(mlir::DictionaryAttr entries) {
    // The region is reserved and committed up front, so the entries are written straight
    // into their final place - no staging container between the attributes and the buffer.
    // A later reservation lands after this region rather than inside it, which is what lets
    // a nested map be materialized part-way through filling its parent.
    MapWriteCursor cursor = _memory->mapBuffer().reserveMap(entries.size(),
                                                            mapValueBytes(entries));

    for (const mlir::NamedAttribute entry : entries) {
        const mlir::StringAttr name = entry.getName();
        const mlir::Attribute value = entry.getValue();

        cursor.writeKey(ownedCharacters(name.getValue()));

        if (const auto boolAttr = mlir::dyn_cast<mlir::BoolAttr>(value)) {
            cursor.writeValue(CellTag::Bool, types::Bool::Primitive(boolAttr.getValue()));
        } else if (const auto intAttr = mlir::dyn_cast<mlir::IntegerAttr>(value)) {
            cursor.writeValue(CellTag::Int,
                              static_cast<types::Int64::Primitive>(intAttr.getInt()));
        } else if (const auto floatAttr = mlir::dyn_cast<mlir::FloatAttr>(value)) {
            cursor.writeValue(CellTag::Double,
                              static_cast<types::Double::Primitive>(floatAttr.getValueAsDouble()));
        } else if (const auto stringAttr = mlir::dyn_cast<mlir::StringAttr>(value)) {
            cursor.writeValue(CellTag::String, ownedCharacters(stringAttr.getValue()));
        } else if (const auto embeddingAttr = mlir::dyn_cast<mlir::DenseF32ArrayAttr>(value)) {
            cursor.writeValue(CellTag::Embedding, ownedFloats(embeddingAttr.asArrayRef()));
        } else if (const auto nestedList = mlir::dyn_cast<mlir::ArrayAttr>(value)) {
            cursor.writeValue(CellTag::ListView, materializeListView(nestedList));
        } else if (const auto nestedMap = mlir::dyn_cast<mlir::DictionaryAttr>(value)) {
            cursor.writeValue(CellTag::MapView, materializeMapView(nestedMap));
        } else if (mlir::isa<mlir::UnitAttr>(value)) {
            cursor.writeValue(CellTag::Null, PropertyNull {});
        } else {
            throw IRException("Unsupported literal attribute in a constant map");
        }
    }

    return cursor.getView();
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
            cursor.writeValue(CellTag::Bool, types::Bool::Primitive(boolAttr.getValue()));
        } else if (const auto intAttr = mlir::dyn_cast<mlir::IntegerAttr>(element)) {
            cursor.writeValue(CellTag::Int,
                              static_cast<types::Int64::Primitive>(intAttr.getInt()));
        } else if (const auto floatAttr = mlir::dyn_cast<mlir::FloatAttr>(element)) {
            cursor.writeValue(CellTag::Double,
                              static_cast<types::Double::Primitive>(floatAttr.getValueAsDouble()));
        } else if (const auto stringAttr = mlir::dyn_cast<mlir::StringAttr>(element)) {
            cursor.writeValue(CellTag::String, ownedCharacters(stringAttr.getValue()));
        } else if (const auto embeddingAttr = mlir::dyn_cast<mlir::DenseF32ArrayAttr>(element)) {
            cursor.writeValue(CellTag::Embedding, ownedFloats(embeddingAttr.asArrayRef()));
        } else if (mlir::isa<mlir::UnitAttr>(element)) {
            cursor.writeValue(CellTag::Null, PropertyNull {});
        } else if (const auto nestedAttr = mlir::dyn_cast<mlir::ArrayAttr>(element)) {
            cursor.writeValue(CellTag::ListView, materializeListView(nestedAttr));
        } else if (const auto nestedMap = mlir::dyn_cast<mlir::DictionaryAttr>(element)) {
            cursor.writeValue(CellTag::MapView, materializeMapView(nestedMap));
        } else {
            throw IRException("Unsupported literal attribute in a constant list");
        }
    }

    return cursor.getView();
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

    if (llvm::isa<storage::MapType>(elementType)) {
        const auto entries = mlir::cast<mlir::DictionaryAttr>(constant.getValue());

        ColumnConst<MapView>* map = _memory->alloc<ColumnConst<MapView>>();
        map->set(materializeMapView(entries));

        _valueSlots[res] = map;
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

        if constexpr (std::same_as<T, types::String>) {
            typed->set(ownedCharacters(mlir::cast<mlir::StringAttr>(value).getValue()));
        } else if constexpr (std::same_as<T, types::Embedding>) {
            typed->set(ownedFloats(mlir::cast<mlir::DenseF32ArrayAttr>(value).asArrayRef()));
        } else {
            typed->set(constantValueAs<T>(value));
        }

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
    // repeat: its rows are absent values rather than copies of one. A list or a map rides
    // its own container chunk rather than a nullable value one, so its fill repeats the one
    // view the constant holds instead of dispatching on a value type.
    const mlir::Type resultElement = mlir::cast<nl::ChunkType>(resultType).getElementType();
    const bool isUntypedNull = isUntypedNullChunkType(broadcast.getValue().getType());
    const bool isList = llvm::isa<storage::ListType>(resultElement);
    const bool isMap = llvm::isa<storage::MapType>(resultElement);

    NLBroadcastConstantFunction fill = nullptr;
    if (isUntypedNull) {
        fill = NLExecutor::selectNullConstantBroadcast();
    } else if (isReadAsRows(resultElement)) {
        fill = NLExecutor::selectConstantPlainBroadcast(chunkKindFromElementType(resultElement), value);
    } else if (isList) {
        fill = NLExecutor::selectConstantListBroadcast();
    } else if (isMap) {
        fill = NLExecutor::selectConstantMapBroadcast();
    } else if (mlir::isa<storage::AnyType>(resultElement)) {
        fill = NLExecutor::selectConstantCellBroadcast();
    } else {
        fill = NLExecutor::selectConstantBroadcast(getNullableChunkKind(resultType), value);
    }

    NLBroadcastConstantData* data = _program->allocFunctionData<NLBroadcastConstantData>(value, cardinality, output, fill);
    body->emplaceStmt(&NLExecutor::runBroadcastConstant, data);
}

void NLTranslator::translateRowConstant(nl::RowConstant rowConstant, NLStmtContainer* body) {
    const mlir::Value rowValue = rowConstant.getRow();

    if (isUntypedNullChunkType(rowValue.getType())) {
        _valueSlots[rowConstant.getResult()] = _memory->alloc<ColumnConst<PropertyNull>>();
        return;
    }

    const Column* row = getColumn(rowValue);

    Column* result = nullptr;
    const NLUnaryFn read = selectRowConstantForChunkType(rowValue.getType(), row, result);
    _valueSlots[rowConstant.getResult()] = result;

    NLUnaryData* data = _program->allocFunctionData<NLUnaryData>(row, result, read);
    body->emplaceStmt(&NLExecutor::runUnary, data);
}

void NLTranslator::translateNot(nl::Not notOp, NLStmtContainer* body) {
    const mlir::Value operandValue = notOp.getOperand();
    const Column* operand = getColumn(operandValue);
    const NLTruthColumnKind truthKind = truthColumnKindOf(operandValue.getType(), operand);

    Column* result = nullptr;
    const NLUnaryFn fn = isConstantColumn(operand) ? NLExecutor::selectConstantNot(truthKind, _memory, result)
                                                   : NLExecutor::selectNot(truthKind, _memory, result);
    bindKernelResult(notOp.getResult(), result);

    NLUnaryData* data = _program->allocFunctionData<NLUnaryData>(operand, result, fn);
    body->emplaceStmt(&NLExecutor::runUnary, data);
}

void NLTranslator::translateToNullable(nl::ToNullable toNullable, NLStmtContainer* body) {
    const mlir::Value operandValue = toNullable.getOperand();
    const Column* operand = getColumn(operandValue);

    const auto resultChunk = mlir::cast<nl::ChunkType>(toNullable.getResult().getType());

    // The null literal's chunk names no value type of its own, so there is no value to read
    // through: the result is one absent value per row, in the type it does name - what a
    // UNION branch spelling a column's value null hands the sink. An entity or a tagged
    // cell column holds its own null, in the invalid ID or in the tag.
    if (isUntypedNullChunkType(operandValue.getType())) {
        translateNullLiteral(toNullable.getResult(), operand, body);
        return;
    }

    const auto nullableType = mlir::cast<storage::NullableType>(resultChunk.getElementType());
    const mlir::Type valueElement = nullableType.getValueType();

    // An entity column carries its null in the ID rather than in an optional, so it is
    // read row by row for validity instead of copied straight across
    const auto operandChunk = mlir::cast<nl::ChunkType>(operandValue.getType());
    const mlir::Type operandElement = operandChunk.getElementType();
    const bool readsAnEntity = mlir::isa<storage::NodeIDType, storage::EdgeIDType>(operandElement);

    Column* result = nullptr;
    NLUnaryFn fn = nullptr;
    if (readsAnEntity) {
        fn = NLExecutor::selectEntityToNullable(chunkKindFromElementType(operandElement), _memory, result);
    } else {
        fn = NLExecutor::selectToNullable(chunkKindFromNullableValue(valueElement), operand, _memory, result);
    }
    bindKernelResult(toNullable.getResult(), result);

    NLUnaryData* data = _program->allocFunctionData<NLUnaryData>(operand, result, fn);
    body->emplaceStmt(&NLExecutor::runUnary, data);
}

void NLTranslator::translateNullLiteral(mlir::Value result, const Column* nullLiteral, NLStmtContainer* body) {
    const mlir::Type resultChunk = result.getType();

    Column* const nulls = allocColumnForChunkType(resultChunk);
    _valueSlots[result] = nulls;

    NLFillNullData* fillData = _program->allocFunctionData<NLFillNullData>(nullLiteral,
                                                                           nulls,
                                                                           selectFillNullForChunkType(resultChunk));
    body->emplaceStmt(&NLExecutor::runFillNull, fillData);
}

void NLTranslator::translateCheckedCast(nl::CheckedCast checkedCast, NLStmtContainer* body) {
    const mlir::Value operandValue = checkedCast.getOperand();
    const Column* operand = getColumn(operandValue);

    const nl::ChunkType resultChunk = mlir::cast<nl::ChunkType>(checkedCast.getResult().getType());

    if (isUntypedNullChunkType(operandValue.getType())) {
        translateNullLiteral(checkedCast.getResult(), operand, body);
        return;
    }

    Column* result = nullptr;
    const NLUnaryFn fn = NLExecutor::selectCheckedCast(chunkKindFromElementType(resultChunk.getElementType()),
                                                       operand,
                                                       _memory,
                                                       result);
    bindKernelResult(checkedCast.getResult(), result);

    NLUnaryData* data = _program->allocFunctionData<NLUnaryData>(operand, result, fn);
    body->emplaceStmt(&NLExecutor::runUnary, data);
}

void NLTranslator::translateFillNulls(nl::FillNulls fillNulls, NLStmtContainer* body) {
    const mlir::Value resultValue = fillNulls.getResult();
    const mlir::Type resultChunkType = resultValue.getType();

    Column* const result = allocColumnForChunkType(resultChunkType);
    _valueSlots[resultValue] = result;

    const Column* const value = getColumn(fillNulls.getValue());
    const Column* const nullRows = getColumn(fillNulls.getNullRows());
    const Column* const fill = getColumn(fillNulls.getFill());

    const ColumnKind::Code resultKind = result->getKind();
    bioassert(value->getKind() == resultKind && fill->getKind() == resultKind,
              "nl.fill_nulls reads columns of another kind than the column it writes.");
    bioassert(nullRows->getKind() == ColumnVector<CustomBool>::staticKind(), "nl.fill_nulls reads its null rows off a boolean column.");

    NLFillNullsData* data = _program->allocFunctionData<NLFillNullsData>(value,
                                                                         static_cast<const ColumnVector<CustomBool>*>(nullRows),
                                                                         fill,
                                                                         result,
                                                                         selectFillNullRowsForChunkType(resultChunkType));
    body->emplaceStmt(&NLExecutor::runFillNulls, data);
}

void NLTranslator::translateIsNotTrue(nl::IsNotTrue isNotTrue, NLStmtContainer* body) {
    const mlir::Value operandValue = isNotTrue.getOperand();
    const Column* operand = getColumn(operandValue);

    Column* result = nullptr;
    const NLUnaryFn fn = NLExecutor::selectIsNotTrue(truthColumnKindOf(operandValue.getType(), operand), _memory, result);

    bindKernelResult(isNotTrue.getResult(), result);

    NLUnaryData* data = _program->allocFunctionData<NLUnaryData>(operand, result, fn);
    body->emplaceStmt(&NLExecutor::runUnary, data);
}

void NLTranslator::translateToAny(nl::ToAny toAny, NLStmtContainer* body) {
    const mlir::Value operandValue = toAny.getOperand();
    const mlir::Value resultValue = toAny.getResult();
    Column* const operand = getColumn(operandValue);

    // A list's elements are tagged cells already, so its column is the result as it stands
    if (!isAnyChunk(resultValue.getType())) {
        _valueSlots[resultValue] = operand;
        return;
    }

    const bool readsANull = isUntypedNullChunkType(operandValue.getType());
    if (readsANull && yieldsConstantColumn(resultValue)) {
        ColumnConst<CellView>* const nullCell = _memory->alloc<ColumnConst<CellView>>();
        nullCell->set(CellView::nullElement());
        _valueSlots[resultValue] = nullCell;
        return;
    } else if (readsANull) {
        translateNullLiteral(resultValue, operand, body);
        return;
    }

    Column* const result = allocColumnForChunkType(resultValue.getType());
    _valueSlots[resultValue] = result;

    NLToAnyData* data = _program->allocFunctionData<NLToAnyData>(operand,
                                                                 result,
                                                                 selectListItemRead(operandValue.getType()),
                                                                 _program->allocStepBuffers());
    body->emplaceStmt(&NLExecutor::runToAny, data);
}

NLCaseResetFn NLTranslator::selectCaseResetForChunkType(mlir::Type chunkType) {
    const auto chunk = mlir::cast<nl::ChunkType>(chunkType);
    const mlir::Type elementType = chunk.getElementType();

    if (isAnyChunk(chunkType)) {
        return NLExecutor::selectTaggedCellReset();
    } else if (holdsItsOwnNull(elementType)) {
        return NLExecutor::selectOwnNullCaseReset(chunkKindFromElementType(elementType));
    }

    return NLExecutor::selectCaseReset(getNullableChunkKind(chunkType));
}

NLCaseFinishFn NLTranslator::selectCaseFinishForChunkType(mlir::Type chunkType) {
    const auto chunk = mlir::cast<nl::ChunkType>(chunkType);

    if (isAnyChunk(chunkType) || holdsItsOwnNull(chunk.getElementType())) {
        return nullptr;
    }

    return NLExecutor::selectCaseFinish(getNullableChunkKind(chunkType));
}

NLFillNullRowsFn NLTranslator::selectFillNullRowsForChunkType(mlir::Type chunkType) {
    const auto chunk = mlir::cast<nl::ChunkType>(chunkType);
    const mlir::Type elementType = chunk.getElementType();

    if (mlir::isa<storage::AnyType>(elementType)) {
        return NLExecutor::selectTaggedCellFillNullRows();
    } else if (holdsItsOwnNull(elementType)) {
        return NLExecutor::selectOwnNullFillNullRows(chunkKindFromElementType(elementType));
    }

    return NLExecutor::selectFillNullRows(getNullableChunkKind(chunkType));
}

NLCellWrite NLTranslator::selectCellWriteForChunkType(mlir::Type resultChunkType,
                                                      const Column* value,
                                                      mlir::Type valueChunkType) {
    if (!isAnyChunk(resultChunkType)) {
        return NLCellWrite {._write = selectCaseWriteForChunkType(resultChunkType, value, valueChunkType)};
    } else if (isUntypedNullChunkType(valueChunkType)) {
        return NLCellWrite {._write = NLExecutor::selectNullCellWrite()};
    }

    return NLCellWrite {._taggedRead = selectListItemRead(valueChunkType)};
}

NLCaseWriteFn NLTranslator::selectCaseWriteForChunkType(mlir::Type resultChunkType,
                                                        const Column* value,
                                                        mlir::Type valueChunkType) {
    const auto resultChunk = mlir::cast<nl::ChunkType>(resultChunkType);
    const mlir::Type resultElement = resultChunk.getElementType();

    if (holdsItsOwnNull(resultElement)) {
        return NLExecutor::selectOwnNullCaseWrite(chunkKindFromElementType(resultElement),
                                                  isUntypedNullChunkType(valueChunkType));
    }

    return NLExecutor::selectCaseWrite(getNullableChunkKind(resultChunkType),
                                       value,
                                       isNullableChunkType(valueChunkType),
                                       isUntypedNullChunkType(valueChunkType));
}

NLFillNullFunction NLTranslator::selectFillNullForChunkType(mlir::Type chunkType) {
    const auto chunk = mlir::cast<nl::ChunkType>(chunkType);
    const mlir::Type elementType = chunk.getElementType();

    if (isNullableList(elementType)) {
        return NLExecutor::selectOptListFillNull();
    }

    if (const auto nullableType = mlir::dyn_cast<storage::NullableType>(elementType)) {
        return NLExecutor::selectOptFillNullFunction(chunkKindFromNullableValue(nullableType.getValueType()));
    } else if (mlir::isa<storage::AnyType>(elementType)) {
        return NLExecutor::selectCellFillNull();
    }

    return NLExecutor::selectFillNullFunction(chunkKindFromElementType(elementType));
}

NLUnaryFn NLTranslator::selectRowConstantForChunkType(mlir::Type chunkType, const Column* row, Column*& result) {
    const mlir::Type elementType = mlir::cast<nl::ChunkType>(chunkType).getElementType();
    const auto nullableType = mlir::dyn_cast<storage::NullableType>(elementType);

    if (nullableType) {
        return NLExecutor::selectOptRowConstant(chunkKindFromNullableValue(nullableType.getValueType()), row, _memory, result);
    } else if (mlir::isa<storage::AnyType>(elementType)) {
        return NLExecutor::selectCellRowConstant(row, _memory, result);
    }

    return NLExecutor::selectRowConstant(chunkKindFromElementType(elementType), row, _memory, result);
}
