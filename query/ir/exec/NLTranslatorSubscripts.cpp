#include "NLTranslator.h"

#include <optional>
#include <string_view>

#include "NLOps.h"

#include "IRConstantColumn.h"
#include "llvm/ADT/DenseMap.h"

#include "NLExecutor.h"

using namespace db;

namespace nl = mlir::nl;
namespace storage = mlir::storage;

namespace {

template <typename Part, typename Owner>
std::optional<Part> componentNamed(std::string_view name) {
    Part part {};
    if (!Owner::partNamed(name, part)) {
        return std::nullopt;
    }

    return part;
}

}

void NLTranslator::translateListIndex(nl::ListIndex index, NLStmtContainer* body) {
    const Column* lhs = getColumn(index.getLhs());
    const Column* rhs = getColumn(index.getRhs());

    const auto resultChunk = mlir::cast<nl::ChunkType>(index.getResult().getType());
    const mlir::Type resultElement = resultChunk.getElementType();
    const bool readsAnEntity = mlir::isa<storage::NodeIDType, storage::EdgeIDType>(resultElement);

    Column* result = nullptr;
    NLBinaryFn fn = nullptr;

    if (readsAnEntity) {
        fn = NLExecutor::selectEntityListIndex(chunkKindFromElementType(resultElement),
                                               lhs,
                                               rhs,
                                               _memory,
                                               result);
    } else if (mlir::isa<storage::AnyType>(resultElement)) {
        fn = NLExecutor::selectBinary<OP_INDEX>(lhs, rhs, _memory, result);
    } else {
        const auto nullableType = mlir::cast<storage::NullableType>(resultElement);
        fn = NLExecutor::selectValueListIndex(chunkKindFromNullableValue(nullableType.getValueType()), lhs, rhs, _memory, result);
    }

    bindKernelResult(index.getResult(), result);

    NLBinaryData* data = _program->allocFunctionData<NLBinaryData>(lhs, rhs, result, fn, _program->allocStepBuffers());
    body->emplaceStmt(&NLExecutor::runBinary, data);
}

void NLTranslator::translateStaticMapKey(nl::StaticMapKey mapKey, NLStmtContainer* body) {
    const mlir::Value resultValue = mapKey.getResult();

    const Column* input = getColumn(mapKey.getMap());
    Column* const result = yieldsConstantColumn(resultValue) ? allocConstCellColumn()
                                                             : allocCellColumn();
    _valueSlots[resultValue] = result;

    const std::string_view key = ownedCharacters(mapKey.getKey());
    const NLStaticMapKeyData::ComponentParts parts {componentNamed<DateTimePart, DateTime>(key),
                                                    componentNamed<DurationPart, Duration>(key),
                                                    componentNamed<PointPart, Point>(key)};

    NLStaticMapKeyData* data = _program->allocFunctionData<NLStaticMapKeyData>(input,
                                                                   result,
                                                                   NLExecutor::selectKeyedRead(input),
                                                                   key,
                                                                   parts,
                                                                   _program->allocStepBuffers());

    body->emplaceStmt(&NLExecutor::runStaticMapKey, data);
}

void NLTranslator::translateDynamicMapKey(nl::DynamicMapKey mapKey, NLStmtContainer* body) {
    const mlir::Value resultValue = mapKey.getResult();

    const Column* input = getColumn(mapKey.getMap());
    const Column* key = getColumn(mapKey.getKey());

    Column* const result = yieldsConstantColumn(resultValue) ? allocConstCellColumn()
                                                             : allocCellColumn();
    _valueSlots[resultValue] = result;

    NLDynamicMapKeyData* data = _program->allocFunctionData<NLDynamicMapKeyData>(input,
                                                                                 result,
                                                                                 NLExecutor::selectKeyedRead(input),
                                                                                 key,
                                                                                 NLExecutor::selectStringRead(key),
                                                                                 _program->allocStepBuffers());

    body->emplaceStmt(&NLExecutor::runDynamicMapKey, data);
}

void NLTranslator::translateContainerIndex(nl::ContainerIndex index, NLStmtContainer* body) {
    const mlir::Value resultValue = index.getResult();
    const mlir::Type resultChunk = resultValue.getType();
    const mlir::Type resultElement = mlir::cast<nl::ChunkType>(resultChunk).getElementType();

    const Column* base = getColumn(index.getBase());
    const Column* subscripts = getColumn(index.getIndex());

    Column* const result = allocColumnForChunkType(resultChunk);
    bindKernelResult(resultValue, result);

    NLHandlerFunction run = nullptr;

    if (mlir::isa<storage::AnyType>(resultElement)) {
        run = NLExecutor::selectCellContainerIndex();
    } else if (isEntityIDElement(resultElement)) {
        run = NLExecutor::selectEntityContainerIndex(chunkKindFromElementType(resultElement));
    } else {
        const auto nullableType = mlir::cast<storage::NullableType>(resultElement);
        run = NLExecutor::selectValueContainerIndex(chunkKindFromNullableValue(nullableType.getValueType()));
    }

    NLContainerIndexData* data = _program->allocFunctionData<NLContainerIndexData>(base,
                                                                                   NLExecutor::selectContainerRead(base),
                                                                                   subscripts,
                                                                                   NLExecutor::selectSubscriptRead(subscripts),
                                                                                   result,
                                                                                   _program->allocStepBuffers());

    body->emplaceStmt(run, data);
}
