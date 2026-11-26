#include "NLTranslator.h"

#include <optional>

#include "NLOps.h"

#include "mlir/IR/BuiltinAttributes.h"
#include "llvm/ADT/DenseMap.h"

#include "metadata/PropertyType.h"

#include "NLExecutor.h"
#include "NLTranslatorChunkTypes.h"

#include "IRException.h"

using namespace db;

namespace nl = mlir::nl;
namespace storage = mlir::storage;

NLListItemReadFunction NLTranslator::selectListItemRead(mlir::Type chunkType) {
    const mlir::Type elementType = mlir::cast<nl::ChunkType>(chunkType).getElementType();

    if (mlir::isa<storage::NodeIDType>(elementType)) {
        return NLExecutor::selectNodeListItemRead();
    } else if (mlir::isa<storage::EdgeIDType>(elementType)) {
        return NLExecutor::selectEdgeListItemRead();
    } else if (mlir::isa<storage::PathType>(elementType)) {
        return NLExecutor::selectPathListItemRead();
    } else if (mlir::isa<storage::ListType>(elementType)) {
        return NLExecutor::selectNestedListItemRead();
    } else if (mlir::isa<storage::MapType>(elementType)) {
        return NLExecutor::selectNestedMapListItemRead();
    } else if (mlir::isa<storage::AnyType>(elementType)) {
        return NLExecutor::selectTaggedListItemRead();
    } else if (isNullableList(elementType)) {
        return NLExecutor::selectOptNestedListItemRead();
    }

    const auto nullableType = mlir::dyn_cast<storage::NullableType>(elementType);
    if (!nullableType) {
        throw IRException("nl.make_list requires a nullable value chunk for a scalar element column");
    }

    const mlir::Type valueElement = nullableType.getValueType();
    return NLExecutor::selectValueListItemRead(chunkKindFromNullableValue(valueElement));
}

void NLTranslator::translateMakeList(nl::MakeList makeList, NLStmtContainer* body) {
    const mlir::Value resultValue = makeList.getResult();

    Column* const result = allocColumnForChunkType(resultValue.getType());
    _valueSlots[resultValue] = result;

    NLMakeListData* data = _program->allocFunctionData<NLMakeListData>(result, _program->allocStepBuffers());

    for (const mlir::Value elementChunk : makeList.getElements()) {
        data->addElement(NLMakeListData::Element {
            ._column = getColumn(elementChunk),
            ._read = selectListItemRead(elementChunk.getType()),
        });
    }

    body->emplaceStmt(&NLExecutor::runMakeList, data);
}

NLMapValueReadFunction NLTranslator::selectMapValueRead(mlir::Type chunkType) {
    const mlir::Type elementType = mlir::cast<nl::ChunkType>(chunkType).getElementType();

    if (mlir::isa<storage::NodeIDType>(elementType)) {
        return NLExecutor::selectNodeMapValueRead();
    } else if (mlir::isa<storage::EdgeIDType>(elementType)) {
        return NLExecutor::selectEdgeMapValueRead();
    } else if (mlir::isa<storage::PathType>(elementType)) {
        return NLExecutor::selectPathMapValueRead();
    } else if (mlir::isa<storage::MapType>(elementType)) {
        return NLExecutor::selectNestedMapValueRead();
    } else if (mlir::isa<storage::ListType>(elementType)) {
        return NLExecutor::selectNestedListMapValueRead();
    } else if (mlir::isa<storage::AnyType>(elementType)) {
        return NLExecutor::selectTaggedMapValueRead();
    } else if (isNullableList(elementType)) {
        return NLExecutor::selectOptNestedListMapValueRead();
    }

    const auto nullableType = mlir::dyn_cast<storage::NullableType>(elementType);
    if (!nullableType) {
        throw IRException("nl.make_map requires a nullable value chunk for a scalar value column");
    }

    const mlir::Type valueElement = nullableType.getValueType();
    return NLExecutor::selectValueMapValueRead(chunkKindFromNullableValue(valueElement));
}

void NLTranslator::translateMakeMap(nl::MakeMap makeMap, NLStmtContainer* body) {
    const mlir::Value resultValue = makeMap.getResult();

    Column* const result = allocColumnForChunkType(resultValue.getType());
    _valueSlots[resultValue] = result;

    NLMakeMapData* data = _program->allocFunctionData<NLMakeMapData>(result, _program->allocStepBuffers());

    const mlir::ArrayAttr keys = makeMap.getKeys();
    for (const auto& [key, valueChunk] : llvm::zip_equal(keys, makeMap.getValues())) {
        const llvm::StringRef keyName = mlir::cast<mlir::StringAttr>(key).getValue();

        data->addEntry(NLMakeMapData::Entry {
            ._key = ownedCharacters(keyName),
            ._column = getColumn(valueChunk),
            ._read = selectMapValueRead(valueChunk.getType()),
        });
    }

    body->emplaceStmt(&NLExecutor::runMakeMap, data);
}

void NLTranslator::translateMergeMaps(nl::MergeMaps merge, NLStmtContainer* body) {
    const mlir::Value resultValue = merge.getResult();
    const mlir::Type resultType = resultValue.getType();

    Column* const result = allocColumnForChunkType(resultType);
    _valueSlots[resultValue] = result;

    NLMergeMapsData* data = _program->allocFunctionData<NLMergeMapsData>(result, _program->allocStepBuffers());

    for (const mlir::Value mapChunk : merge.getMaps()) {
        const Column* const map = getColumn(mapChunk);

        data->addOperand(NLMergeMapsData::Operand {
            ._column = map,
            ._read = NLExecutor::selectKeyedRead(map),
        });
    }

    const mlir::Type resultElement = mlir::cast<nl::ChunkType>(resultType).getElementType();

    if (mlir::isa<storage::NullableType>(resultElement)) {
        body->emplaceStmt(&NLExecutor::runMergeMaps<std::optional<MapView>>, data);
    } else {
        body->emplaceStmt(&NLExecutor::runMergeMaps<MapView>, data);
    }
}

NLRangeBoundReadFunction NLTranslator::selectRangeBoundRead(mlir::Type chunkType) {
    const mlir::Type elementType = mlir::cast<nl::ChunkType>(chunkType).getElementType();

    const storage::NullableType nullableType = mlir::dyn_cast<storage::NullableType>(elementType);
    if (!nullableType) {
        throw IRException("nl.range requires a nullable integer chunk for each bound");
    }

    return NLExecutor::selectRangeBoundRead(valueTypeFromElementType(nullableType.getValueType()));
}

NLRangeBound NLTranslator::rangeBound(mlir::Value chunk) {
    if (!chunk) {
        return NLRangeBound {};
    }

    const Column* const column = getColumn(chunk);
    const mlir::Type chunkType = chunk.getType();

    return NLRangeBound {
        ._column = column,
        ._read = isAnyChunk(chunkType) ? NLExecutor::selectCellBoundRead(column) : selectRangeBoundRead(chunkType),
    };
}

void NLTranslator::translateListSlice(nl::ListSlice slice, NLStmtContainer* body) {
    const mlir::Value resultValue = slice.getResult();

    Column* const result = allocColumnForChunkType(resultValue.getType());
    _valueSlots[resultValue] = result;

    const Column* const list = getColumn(slice.getList());

    NLListSliceData* data = _program->allocFunctionData<NLListSliceData>(list,
                                                                         NLExecutor::selectListRead(list),
                                                                         rangeBound(slice.getFrom()),
                                                                         rangeBound(slice.getTo()),
                                                                         result);

    body->emplaceStmt(&NLExecutor::runListSlice, data);
}

void NLTranslator::translateSortMulti(nl::SortMulti sortMulti, NLStmtContainer* body) {
    const mlir::Value resultValue = sortMulti.getResult();

    Column* const result = allocColumnForChunkType(resultValue.getType());
    _valueSlots[resultValue] = result;

    const Column* const list = getColumn(sortMulti.getList());

    const mlir::Value fieldsValue = sortMulti.getFields();
    const Column* const fields = fieldsValue ? getColumn(fieldsValue) : nullptr;
    const NLListReadFunction fieldsRead = fields ? NLExecutor::selectListRead(fields) : nullptr;

    NLSortMultiData* data = _program->allocFunctionData<NLSortMultiData>(list,
                                                                         NLExecutor::selectListRead(list),
                                                                         fields,
                                                                         fieldsRead,
                                                                         rangeBound(sortMulti.getLimit()),
                                                                         rangeBound(sortMulti.getSkip()),
                                                                         _program->allocStepBuffers(),
                                                                         result);

    body->emplaceStmt(&NLExecutor::runSortMulti, data);
}

void NLTranslator::translatePercentile(mlir::Operation* operation, NLStmtContainer* body) {
    const mlir::Value resultValue = operation->getResult(0);
    const mlir::Value percentileValue = operation->getOperand(1);

    Column* const result = allocColumnForChunkType(resultValue.getType());
    _valueSlots[resultValue] = result;

    const Column* const list = getColumn(operation->getOperand(0));
    const Column* const percentile = getColumn(percentileValue);
    const mlir::Type percentileType = percentileValue.getType();
    const NLPercentileReadFunction percentileRead = isAnyChunk(percentileType)
                                                      ? NLExecutor::selectCellPercentileRead(percentile)
                                                      : NLExecutor::selectPercentileRead(nullableChunkValueType(percentileType));

    NLPercentileData* data = _program->allocFunctionData<NLPercentileData>(list,
                                                                           NLExecutor::selectListRead(list),
                                                                           percentile,
                                                                           percentileRead,
                                                                           result);

    if (mlir::isa<nl::PercentileCont>(operation)) {
        body->emplaceStmt(&NLExecutor::runPercentileCont, data);
        return;
    }

    const mlir::Type resultElement = mlir::cast<nl::ChunkType>(resultValue.getType()).getElementType();
    const auto nullable = mlir::dyn_cast<storage::NullableType>(resultElement);

    if (!nullable) {
        body->emplaceStmt(&NLExecutor::runPercentileDisc<CellView>, data);
        return;
    }

    switch (valueTypeFromElementType(nullable.getValueType())) {
        case ValueType::Int64:
            body->emplaceStmt(&NLExecutor::runPercentileDisc<std::optional<types::Int64::Primitive>>, data);
        break;

        case ValueType::UInt64:
            body->emplaceStmt(&NLExecutor::runPercentileDisc<std::optional<types::UInt64::Primitive>>, data);
        break;

        case ValueType::Double:
            body->emplaceStmt(&NLExecutor::runPercentileDisc<std::optional<types::Double::Primitive>>, data);
        break;

        default:
            throw IRException("nl.percentile_disc answers a number or a tagged cell");
        break;
    }
}

void NLTranslator::translateRange(nl::Range range, NLStmtContainer* body) {
    const mlir::Value resultValue = range.getResult();

    Column* const result = allocColumnForChunkType(resultValue.getType());
    _valueSlots[resultValue] = result;

    NLRangeData* data = _program->allocFunctionData<NLRangeData>(result,
                                                                 _program->allocStepBuffers(),
                                                                 rangeBound(range.getStart()),
                                                                 rangeBound(range.getEnd()),
                                                                 rangeBound(range.getStep()));

    body->emplaceStmt(&NLExecutor::runRange, data);
}
