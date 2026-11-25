#include "DBLowering.h"

#include <array>
#include <mlir/IR/Location.h>

#include "mlir/IR/Block.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/Types.h"

#include "NLOps.h"
#include "DBLoweringChunkTypes.h"

#include "DBColumnElement.h"
#include "IRRowAlignment.h"
#include "NullLiteralType.h"

#include "metadata/GraphMetadata.h"

#include "IRValueTypes.h"

#include "IRException.h"
#include "llvm/Support/Casting.h"

using namespace db;

namespace nl = mlir::nl;
namespace storage = mlir::storage;

namespace {

// A region of a CASE or a coalesce takes the columns the regions before it forward, then the
// carried ones. Across the regions they are slots: every forwarded column, in the order the
// regions forward them, then every carried column.
size_t countForwardedColumns(mlir::Operation* op) {
    mlir::db::ColumnCarrierOpInterface carrier = mlir::cast<mlir::db::ColumnCarrierOpInterface>(op);

    size_t count = 0;
    for (mlir::Region& region : op->getRegions()) {
        if (carrier.forwardsColumns(region)) {
            count += carrier.getForwardedColumnsMutable(region).size();
        }
    }

    return count;
}

void collectArgumentChunks(mlir::Region& region,
                           llvm::ArrayRef<mlir::Value> slots,
                           size_t forwardedCount,
                           llvm::SmallVectorImpl<mlir::Value>& chunks) {
    const size_t regionForwarded = mlir::db::forwardedArgumentCount(region);

    chunks.assign(slots.begin(), slots.begin() + regionForwarded);
    chunks.append(slots.begin() + forwardedCount, slots.end());
}

bool readsSlot(mlir::Region& region, size_t slot, size_t forwardedCount) {
    const size_t regionForwarded = mlir::db::forwardedArgumentCount(region);
    if (slot < forwardedCount && slot >= regionForwarded) {
        return false;
    }

    const size_t argumentIndex = slot < forwardedCount ? slot : regionForwarded + slot - forwardedCount;

    return !region.getArgument(static_cast<unsigned>(argumentIndex)).use_empty();
}

void collectLastReaders(mlir::MutableArrayRef<mlir::Region> regions,
                        size_t slotCount,
                        size_t forwardedCount,
                        llvm::SmallVectorImpl<size_t>& lastReaders) {
    lastReaders.assign(slotCount, 0);

    for (size_t regionIndex = 0; regionIndex < regions.size(); regionIndex++) {
        for (size_t slot = 0; slot < slotCount; slot++) {
            if (readsSlot(regions[regionIndex], slot, forwardedCount)) {
                lastReaders[slot] = regionIndex;
            }
        }
    }
}

struct NumericOperand {
    mlir::Type numeric;
    bool nullable {false};
};

NumericOperand numericOperand(mlir::Type chunkType) {
    const auto chunk = mlir::dyn_cast<nl::ChunkType>(chunkType);
    if (!chunk) {
        throw IRException("db.<op> operand must be a value column");
    }

    mlir::Type element = chunk.getElementType();
    bool nullable = false;
    if (const auto nullableType = mlir::dyn_cast<storage::NullableType>(element)) {
        nullable = true;
        element = nullableType.getValueType();
    }

    // A type-erased cell is numeric only once read. '^' answers a double whatever it
    // reads, so a cell reaching here computes in f64 and answers null on a row holding no
    // number; every other operator keeps the cell and never asks.
    if (mlir::isa<storage::AnyType>(element)) {
        return {.numeric = mlir::Float64Type::get(chunkType.getContext()), .nullable = true};
    }

    const bool isFloat = mlir::isa<mlir::Float64Type>(element);
    const auto integerType = mlir::dyn_cast<mlir::IntegerType>(element);
    const bool isInt64 = integerType && integerType.getWidth() == 64;
    if (!isFloat && !isInt64) {
        throw IRException("db.<op> requires numeric operands");
    }

    return {.numeric = element, .nullable = nullable};
}

bool isIntegerChunk(mlir::OpBuilder& builder, mlir::Type chunkType) {
    const auto integerType = mlir::dyn_cast<mlir::IntegerType>(chunkValueElement(builder, chunkType));
    return integerType && integerType.getWidth() == 64;
}

// A node or edge ID chunk: the one plain chunk whose rows can be null, an OPTIONAL MATCH
// leaving an invalid ID where the pattern missed.
bool isEntityChunk(mlir::Type chunkType) {
    const nl::ChunkType chunk = mlir::dyn_cast<nl::ChunkType>(chunkType);
    if (!chunk) {
        return false;
    }

    return mlir::isa<storage::NodeIDType, storage::EdgeIDType>(chunk.getElementType());
}

bool isPathChunk(mlir::Type chunkType) {
    const nl::ChunkType chunk = mlir::dyn_cast<nl::ChunkType>(chunkType);
    return chunk && mlir::isa<storage::PathType>(chunk.getElementType());
}

// A map column, read through the nullable a stored map wears, as isListChunk reads a list
bool isMapChunk(mlir::Type chunkType) {
    const nl::ChunkType chunk = mlir::dyn_cast<nl::ChunkType>(chunkType);
    if (!chunk) {
        return false;
    }

    const mlir::Type element = chunk.getElementType();
    const auto nullable = mlir::dyn_cast<storage::NullableType>(element);
    const mlir::Type mapped = nullable ? nullable.getValueType() : element;

    return mlir::isa<storage::MapType>(mapped);
}

bool isIndexableChunk(mlir::Type chunkType) {
    const nl::ChunkType chunk = mlir::dyn_cast<nl::ChunkType>(chunkType);
    if (!chunk) {
        return false;
    }

    const mlir::Type element = chunk.getElementType();
    const auto nullable = mlir::dyn_cast<storage::NullableType>(element);
    const mlir::Type indexed = nullable ? nullable.getValueType() : element;

    return mlir::isa<storage::ListType, storage::AnyType>(indexed);
}

// The element types a list gathers entities under, which an index reads back out as the
// entity column they came from.
bool namesAnEntityType(mlir::Type element) {
    if (!element) {
        return false;
    }

    return mlir::isa<storage::NodeIDType, storage::EdgeIDType>(element);
}

// The element types an index reads out as a value column rather than as a tagged cell:
// the scalars a value column holds, the schema IDs, and the lists a list of lists is made of.
bool namesAnIndexedValueType(mlir::Type element) {
    if (!element) {
        return false;
    }

    if (const auto integerType = mlir::dyn_cast<mlir::IntegerType>(element)) {
        return integerType.getWidth() == 1 || integerType.getWidth() == 64;
    }

    return mlir::isa<mlir::Float64Type, storage::StringType, storage::EmbeddingType, storage::ListType>(element)
        || storage::namesASchemaID(element);
}

// Internal type of listChunk
mlir::Type listInternalType(mlir::Type chunkType) {
    const nl::ChunkType chunk = mlir::dyn_cast<nl::ChunkType>(chunkType);
    if (!chunk) {
        return {};
    }

    const mlir::Type element = chunk.getElementType();
    const auto nullable = mlir::dyn_cast<storage::NullableType>(element);
    const mlir::Type listed = nullable ? nullable.getValueType() : element;

    const auto listType = mlir::dyn_cast<storage::ListType>(listed);
    return listType ? listType.getElementType() : mlir::Type {};
}

}

void DBLowering::lowerConstant(mlir::db::ConstantOp constant) {
    // Constants are loop-invariant so hoist, each behind the ones hoisted before it: an op
    // goes after its own operands, and landing in front of an earlier constant would put it
    // - and every region hanging under it - above a constant its body still reads.
    setInsertionAfterHoisted();

    nl::Constant nlConstant = _builder.create<nl::Constant>(_builder.getUnknownLoc(), constant.getValue());
    _valueMap[constant.getResult()] = nlConstant.getResult();
    _lastHoisted = nlConstant.getOperation();
}

void DBLowering::lowerBroadcastConstant(mlir::db::BroadcastConstant broadcast) {
    const mlir::Value driver = broadcast.getDriver();
    const mlir::Value driverChunk = driver ? mapValue(driver) : mlir::Value();

    _valueMap[broadcast.getResult()] = rowAlignedChunk(mapValue(broadcast.getValue()), driverChunk);
}

void DBLowering::lowerRowConstant(mlir::db::RowConstant rowConstant) {
    const mlir::Value row = mapValue(rowConstant.getRow());

    _builder.setInsertionPointAfterValue(row);
    _valueMap[rowConstant.getResult()] = _builder.create<nl::RowConstant>(_builder.getUnknownLoc(), row).getResult();
}

void DBLowering::lowerToNullable(mlir::db::ToNullable toNullable) {
    const mlir::Value chunk = mapValue(toNullable.getOperand());
    const mlir::Type element = mlir::cast<nl::ChunkType>(chunk.getType()).getElementType();

    _valueMap[toNullable.getResult()] = toNullableChunk(chunk, element);
}

void DBLowering::lowerCheckedCast(mlir::db::CheckedCast checkedCast) {
    const mlir::Value chunk = mapValue(checkedCast.getOperand());
    const mlir::Type operandElement = mlir::cast<nl::ChunkType>(chunk.getType()).getElementType();
    const mlir::Type entityElement = mlir::cast<mlir::db::ColumnType>(checkedCast.getResult().getType()).getType();

    // A column whose type lowering resolved to the entity holds no row to check
    if (operandElement == entityElement) {
        _valueMap[checkedCast.getResult()] = chunk;
        return;
    }

    const nl::ChunkType resultType = nl::ChunkType::get(_builder.getContext(), entityElement);

    setInsertionForUnaryOp(chunk);

    nl::CheckedCast entities = _builder.create<nl::CheckedCast>(_builder.getUnknownLoc(), resultType, chunk);
    _valueMap[checkedCast.getResult()] = entities.getResult();
}

mlir::Type DBLowering::binaryResultElement(BinaryResultKind kind,
                                           mlir::Type lhsType,
                                           mlir::Type rhsType) {
    mlir::MLIRContext* const ctx = _builder.getContext();

    const bool operandNullable = isNullableChunk(lhsType) || isNullableChunk(rhsType);

    switch (kind) {
        case BinaryResultKind::Boolean:
        case BinaryResultKind::Comparison: {
            // List and map comparison always nullable; either holding a null => null
            const bool comparesTwoLists = isListChunk(lhsType) && isListChunk(rhsType);
            const bool comparesTwoMaps = isMapChunk(lhsType) && isMapChunk(rhsType);

            // A cell may hold a null, or a value of another type than the one it meets
            const bool comparesATaggedCell = isTaggedCellChunk(lhsType) || isTaggedCellChunk(rhsType);

            // An ID column carries its null in the ID: the invalid one an OPTIONAL MATCH leaves
            const bool lhsIsAnEntity = isEntityChunk(lhsType);
            const bool rhsIsAnEntity = isEntityChunk(rhsType);
            const bool comparesAnEntity = kind == BinaryResultKind::Comparison
                                       && ((lhsIsAnEntity && (rhsIsAnEntity || isIntegerChunk(_builder, rhsType)))
                                           || (rhsIsAnEntity && isIntegerChunk(_builder, lhsType)));

            // A path carries its null as the path with no node
            const bool comparesTwoPaths = kind == BinaryResultKind::Comparison && isPathChunk(lhsType) && isPathChunk(rhsType);

            const bool alwaysNullable = operandNullable
                                     || comparesTwoLists
                                     || comparesTwoMaps
                                     || comparesATaggedCell
                                     || comparesAnEntity
                                     || comparesTwoPaths;

            if (alwaysNullable) {
                return storage::NullableType::get(ctx, _builder.getI1Type());
            }

            return storage::BoolType::get(ctx);
        }
        break;

        case BinaryResultKind::Numeric: {
            if (isTaggedCellChunk(lhsType) || isTaggedCellChunk(rhsType)) {
                const mlir::Type cell = storage::AnyType::get(ctx);

                // A cell beside a list joins it, as the list it holds or as one element
                if (isListChunk(lhsType) || isListChunk(rhsType)) {
                    return storage::NullableType::get(ctx, storage::ListType::get(ctx, cell));
                }

                return cell;
            }

            const mlir::Type lhsElement = chunkValueElement(_builder, lhsType);
            const mlir::Type rhsElement = chunkValueElement(_builder, rhsType);

            const bool lhsIsAnInstant = mlir::isa<storage::DateTimeType>(lhsElement);
            const bool rhsIsAnInstant = mlir::isa<storage::DateTimeType>(rhsElement);
            const bool lhsIsADuration = mlir::isa<storage::DurationType>(lhsElement);
            const bool rhsIsADuration = mlir::isa<storage::DurationType>(rhsElement);
            const bool computesTime = lhsIsAnInstant || rhsIsAnInstant || lhsIsADuration || rhsIsADuration;

            if (computesTime) {
                const mlir::Type duration = storage::DurationType::get(ctx);
                const mlir::Type instant = storage::DateTimeType::get(ctx);
                const mlir::Type temporal = lhsIsAnInstant != rhsIsAnInstant ? instant : duration;

                return operandNullable ? storage::NullableType::get(ctx, temporal) : temporal;
            }

            const NumericOperand lhs = numericOperand(lhsType);
            const NumericOperand rhs = numericOperand(rhsType);
            const mlir::Type promoted = promoteNumeric(_builder, lhs.numeric, rhs.numeric);
            const bool nullable = operandNullable || lhs.nullable || rhs.nullable;
            return nullable ? storage::NullableType::get(ctx, promoted) : promoted;
        }
        break;

        case BinaryResultKind::Double: {
            const NumericOperand lhs = numericOperand(lhsType);
            const NumericOperand rhs = numericOperand(rhsType);
            const bool nullable = operandNullable || lhs.nullable || rhs.nullable;

            const mlir::Type doubleElement = _builder.getF64Type();
            return nullable ? storage::NullableType::get(ctx, doubleElement) : doubleElement;
        }
        break;

        case BinaryResultKind::Concat: {
            const bool lhsIsAList = isListChunk(lhsType);
            const bool rhsIsAList = isListChunk(rhsType);
            const bool lhsIsACell = isTaggedCellChunk(lhsType);
            const bool rhsIsACell = isTaggedCellChunk(rhsType);
            const bool readsACell = lhsIsACell || rhsIsACell;

            // A type-erased cell holds a string or a list only where its row says so, and
            // a row holding a null concatenates to null
            const mlir::Type cell = storage::AnyType::get(ctx);

            if (lhsIsACell && rhsIsACell) {
                return cell;
            } else if (readsACell && (lhsIsAList || rhsIsAList)) {
                return storage::NullableType::get(ctx, storage::ListType::get(ctx, cell));
            }

            const bool concatenatesLists = lhsIsAList && rhsIsAList;

            if (!concatenatesLists) {
                if (lhsIsAList || rhsIsAList) {
                    throw IRException("db.concat joins two lists or two strings, not one of each");
                }

                const mlir::Type stringElement = storage::StringType::get(ctx);
                return operandNullable || readsACell ? storage::NullableType::get(ctx, stringElement)
                                                     : stringElement;
            }

            // list concat
            const mlir::Type lhsInnerType = listInternalType(lhsType);
            const mlir::Type rhsInnerType = listInternalType(rhsType);
            const mlir::Type concatenated = storage::ListType::get(ctx, storage::joinedElementType(lhsInnerType, rhsInnerType));

            return operandNullable ? storage::NullableType::get(ctx, concatenated)
                                   : concatenated;

        }
        break;

        case BinaryResultKind::Index: {
            if (!isIndexableChunk(lhsType)) {
                throw IRException("db.list_index requires a list as its indexed operand");
            }

            const mlir::Type element = indexedListElementType(lhsType);

            // An entity column carries its null in the ID, so a list of nodes or edges
            // hands its elements back as the entity column they were gathered from
            if (namesAnEntityType(element)) {
                return element;
            }

            const mlir::Type indexed = namesAnIndexedValueType(element)
                                           ? element
                                           : storage::AnyType::get(ctx);

            return nullableElementType(indexed);
        }
        break;

        case BinaryResultKind::Membership: {
            // A tagged cell holds whatever its row put there, so the test reads the list the
            // cell is iterated as rather than the column's own type
            if (!isListChunk(rhsType) && !isTaggedCellChunk(rhsType)) {
                throw IRException("db.in requires a list as its right operand");
            }

            return storage::NullableType::get(ctx, _builder.getI1Type());
        }
        break;

        case BinaryResultKind::Position: {
            if (!isListChunk(lhsType) && !isTaggedCellChunk(lhsType)) {
                throw IRException("db.index_of requires a list as its searched operand");
            }

            return storage::NullableType::get(ctx, _builder.getI64Type());
        }
        break;
    }

    bioassert(false, "Unhandled binary result kind");
}

mlir::Type DBLowering::caseResultElement(llvm::ArrayRef<mlir::Value> valueChunks) {
    mlir::Type unified;

    for (const mlir::Value chunk : valueChunks) {
        const mlir::Type chunkType = chunk.getType();

        // A null branch names no type of its own; what the others share is the column
        if (isUntypedNullChunk(chunkType)) {
            continue;
        }

        const mlir::Type element = chunkValueElement(_builder, chunkType);
        if (!unified) {
            unified = element;
            continue;
        }

        const mlir::Type shared = selectedValueElement(_builder, unified, element);
        unified = shared ? shared : storage::AnyType::get(_builder.getContext());
    }

    // Every branch is the null literal, so no branch says what the column holds: the
    // selection is that literal, laid out over its rows
    if (!unified) {
        return _builder.getNoneType();
    }

    return unified;
}

void DBLowering::lowerCase(mlir::db::Case caseOp) {
    llvm::SmallVector<mlir::Value, 4> conditions;
    for (const mlir::Value condition : caseOp.getConditions()) {
        conditions.push_back(mapValue(condition));
    }

    llvm::SmallVector<mlir::Value, 4> values;
    for (const mlir::Value value : caseOp.getValues()) {
        values.push_back(mapValue(value));
    }

    const mlir::Value dbDefault = caseOp.getDefaultValue();
    mlir::Value defaultValue = dbDefault ? mapValue(dbDefault) : mlir::Value();

    const auto gatherOperands = [&](llvm::SmallVectorImpl<mlir::Value>& gathered) {
        gathered.assign(conditions.begin(), conditions.end());
        gathered.append(values.begin(), values.end());

        if (defaultValue) {
            gathered.push_back(defaultValue);
        }
    };

    llvm::SmallVector<mlir::Value, 8> operands;
    gatherOperands(operands);

    // A selection walks rows and a constant carries none of its own, so every branch is
    // first laid out over the relation driving the CASE - the kernel then reads one cell
    // of each condition and each value per row it writes
    const mlir::Value cardinality = cardinalityDriver(operands);

    for (mlir::Value& condition : conditions) {
        condition = rowAlignedChunk(condition, cardinality);
    }

    for (mlir::Value& value : values) {
        value = rowAlignedChunk(value, cardinality);
    }

    if (defaultValue) {
        defaultValue = rowAlignedChunk(defaultValue, cardinality);
    }

    llvm::SmallVector<mlir::Value, 5> valueChunks(values.begin(), values.end());
    if (defaultValue) {
        valueChunks.push_back(defaultValue);
    }

    const mlir::Type resultElement = caseResultElement(valueChunks);

    for (mlir::Value& value : values) {
        value = chunkAsElement(value, resultElement);
    }

    if (defaultValue) {
        defaultValue = chunkAsElement(defaultValue, resultElement);
    }

    gatherOperands(operands);

    const nl::ChunkType resultType = caseResultChunkType(resultElement);

    setInsertionForNaryOp(operands);

    nl::Case nlCase = _builder.create<nl::Case>(_builder.getUnknownLoc(),
                                                resultType,
                                                conditions,
                                                values,
                                                defaultValue);
    _valueMap[caseOp.getResult()] = nlCase.getResult();
}

nl::ChunkType DBLowering::caseResultChunkType(mlir::Type resultElement) {
    // A row matching no branch of a defaultless CASE is absent, and so is one taking a
    // null branch, so the selection always lands in a column that can hold an absent row.
    // An entity column already can - it carries its null in the ID an OPTIONAL MATCH left
    // invalid - and so can a !storage.any column, in the tag; every scalar lands in a
    // nullable one.
    mlir::MLIRContext* const context = _builder.getContext();

    return nl::ChunkType::get(context, nullableElementType(resultElement));
}

void DBLowering::lowerLazyCase(mlir::db::LazyCase caseOp) {
    const mlir::Location loc = _builder.getUnknownLoc();
    mlir::MLIRContext* const context = _builder.getContext();
    const nl::ChunkType maskType = nl::ChunkType::get(context, storage::BoolType::get(context));

    const size_t forwardedCount = countForwardedColumns(caseOp.getOperation());

    llvm::SmallVector<mlir::Value, 8> reaching(forwardedCount);
    for (const mlir::Value column : caseOp.getColumnsToFilter()) {
        reaching.push_back(mapValue(column));
    }

    mlir::Value reachingCardinality = cardinalityDriver(llvm::ArrayRef<mlir::Value>(reaching).drop_front(forwardedCount));

    const mlir::MutableArrayRef<mlir::Region> regions = caseOp.getBranches();
    const size_t branchCount = regions.size() / 2;
    const bool hasDefault = regions.size() % 2 == 1;

    llvm::SmallVector<size_t, 8> lastReaders;
    collectLastReaders(regions, reaching.size(), forwardedCount, lastReaders);

    llvm::SmallVector<mlir::Value, 4> conditions;
    llvm::SmallVector<mlir::Value, 4> values;
    llvm::SmallVector<mlir::Value, 8> arguments;

    for (size_t branchIndex = 0; branchIndex < branchCount; branchIndex++) {
        mlir::Region& conditionRegion = regions[2 * branchIndex];
        collectArgumentChunks(conditionRegion, reaching, forwardedCount, arguments);

        const mlir::Value condition = lowerCaseRegion(conditionRegion, arguments, reachingCardinality);
        conditions.push_back(condition);
        forwardCaseRegionChunks(conditionRegion, reaching, reachingCardinality);

        const size_t valueIndex = 2 * branchIndex + 1;
        mlir::Region& valueRegion = regions[valueIndex];
        const auto readByTheValue = [&valueRegion, forwardedCount](size_t slot) {
            return readsSlot(valueRegion, slot, forwardedCount);
        };

        llvm::SmallVector<mlir::Value, 8> taken;
        const mlir::Value takenCardinality = cutCarriedChunks(condition, reaching, readByTheValue, taken);
        collectArgumentChunks(valueRegion, taken, forwardedCount, arguments);
        values.push_back(lowerCaseRegion(valueRegion, arguments, takenCardinality));

        const bool rowsFallThrough = hasDefault || branchIndex + 1 < branchCount;
        if (!rowsFallThrough) {
            break;
        }

        setInsertionForUnaryOp(condition);
        const mlir::Value fallsThrough = _builder.create<nl::IsNotTrue>(loc, maskType, condition).getResult();

        const auto readLater = [&lastReaders, &reaching, valueIndex](size_t slot) {
            return reaching[slot] && lastReaders[slot] > valueIndex;
        };

        llvm::SmallVector<mlir::Value, 8> rest;
        reachingCardinality = cutCarriedChunks(fallsThrough, reaching, readLater, rest);
        reaching.assign(rest.begin(), rest.end());
    }

    mlir::Value defaultValue;
    if (hasDefault) {
        collectArgumentChunks(regions.back(), reaching, forwardedCount, arguments);
        defaultValue = lowerCaseRegion(regions.back(), arguments, reachingCardinality);
    }

    llvm::SmallVector<mlir::Value, 5> valueChunks(values.begin(), values.end());
    if (defaultValue) {
        valueChunks.push_back(defaultValue);
    }

    const mlir::Type resultElement = caseResultElement(valueChunks);

    for (mlir::Value& value : values) {
        value = chunkAsElement(value, resultElement);
    }

    if (defaultValue) {
        defaultValue = chunkAsElement(defaultValue, resultElement);
    }

    llvm::SmallVector<mlir::Value, 8> operands(conditions.begin(), conditions.end());
    operands.append(values.begin(), values.end());
    if (defaultValue) {
        operands.push_back(defaultValue);
    }

    setInsertionForNaryOp(operands);

    nl::CaseMerge merge = _builder.create<nl::CaseMerge>(loc,
                                                         caseResultChunkType(resultElement),
                                                         conditions,
                                                         values,
                                                         defaultValue);
    _valueMap[caseOp.getResult()] = merge.getResult();
}

void DBLowering::lowerCoalesce(mlir::db::Coalesce coalesce) {
    mlir::MLIRContext* const context = _builder.getContext();
    const nl::ChunkType maskType = nl::ChunkType::get(context, storage::BoolType::get(context));

    const mlir::MutableArrayRef<mlir::Region> arguments = coalesce.getArguments();
    const size_t forwardedCount = countForwardedColumns(coalesce.getOperation());

    llvm::SmallVector<mlir::Value, 8> reaching(forwardedCount);
    for (const mlir::Value column : coalesce.getColumnsToFilter()) {
        reaching.push_back(mapValue(column));
    }

    mlir::Value reachingCardinality = cardinalityDriver(llvm::ArrayRef<mlir::Value>(reaching).drop_front(forwardedCount));

    llvm::SmallVector<size_t, 8> lastReaders;
    collectLastReaders(arguments, reaching.size(), forwardedCount, lastReaders);

    llvm::SmallVector<mlir::Value, 4> values;
    llvm::SmallVector<mlir::Value, 4> nullRows;
    llvm::SmallVector<mlir::Value, 8> argumentChunks;
    for (size_t argumentIndex = 0; argumentIndex < arguments.size(); argumentIndex++) {
        mlir::Region& argument = arguments[argumentIndex];
        collectArgumentChunks(argument, reaching, forwardedCount, argumentChunks);

        const mlir::Value value = lowerCaseRegion(argument, argumentChunks, reachingCardinality);
        values.push_back(value);
        nullRows.emplace_back();
        forwardCaseRegionChunks(argument, reaching, reachingCardinality);

        const bool isLast = argumentIndex + 1 == arguments.size();
        const bool leavesEveryRowNull = isUntypedNullChunk(value.getType());
        if (isLast || leavesEveryRowNull) {
            continue;
        }

        setInsertionForUnaryOp(value);
        const mlir::Value stillNull = _builder.create<nl::IsNull>(_builder.getUnknownLoc(), maskType, value).getResult();
        nullRows.back() = stillNull;

        const auto readLater = [&lastReaders, &reaching, argumentIndex](size_t slot) {
            return reaching[slot] && lastReaders[slot] > argumentIndex;
        };

        llvm::SmallVector<mlir::Value, 8> rest;
        reachingCardinality = cutCarriedChunks(stillNull, reaching, readLater, rest);
        reaching.assign(rest.begin(), rest.end());
    }

    _valueMap[coalesce.getResult()] = mergeCoalescedValues(values, nullRows);
}

mlir::Value DBLowering::mergeCoalescedValues(llvm::ArrayRef<mlir::Value> values, llvm::ArrayRef<mlir::Value> nullRows) {
    const mlir::Type resultElement = caseResultElement(values);
    const nl::ChunkType resultType = caseResultChunkType(resultElement);

    mlir::Value merged;
    for (size_t valueIndex = values.size(); valueIndex > 0; valueIndex--) {
        const mlir::Value value = values[valueIndex - 1];
        if (isUntypedNullChunk(value.getType())) {
            continue;
        }

        const mlir::Value converted = branchChunkAs(chunkAsElement(value, resultElement), resultType);

        if (merged) {
            const mlir::Value valueNullRows = nullRows[valueIndex - 1];
            const std::array<mlir::Value, 3> operands {converted, valueNullRows, merged};
            setInsertionForNaryOp(operands);

            merged = _builder.create<nl::FillNulls>(_builder.getUnknownLoc(), resultType, converted, valueNullRows, merged).getResult();
        } else {
            merged = converted;
        }
    }

    return merged ? merged : values.front();
}

mlir::Value DBLowering::lowerCaseRegion(mlir::Region& region,
                                        llvm::ArrayRef<mlir::Value> chunks,
                                        mlir::Value cardinality) {
    mlir::Block& block = region.front();

    for (size_t argumentIndex = 0; argumentIndex < chunks.size(); argumentIndex++) {
        const mlir::Value chunk = chunks[argumentIndex];
        if (chunk) {
            _valueMap[block.getArgument(static_cast<unsigned>(argumentIndex))] = chunk;
        }
    }

    return lowerBlockOverRows(block, cardinality);
}

void DBLowering::forwardCaseRegionChunks(mlir::Region& region,
                                         llvm::SmallVectorImpl<mlir::Value>& slots,
                                         mlir::Value cardinality) {
    mlir::db::ColumnCarrierOpInterface carrier = mlir::cast<mlir::db::ColumnCarrierOpInterface>(region.getParentOp());
    if (!carrier.forwardsColumns(region)) {
        return;
    }

    const mlir::OperandRange forwarded = carrier.getForwardedColumnsMutable(region);
    const size_t forwardOffset = mlir::db::forwardedArgumentCount(region);

    for (size_t forwardIndex = 0; forwardIndex < forwarded.size(); forwardIndex++) {
        slots[forwardOffset + forwardIndex] = rowAlignedChunk(mapValue(forwarded[forwardIndex]), cardinality);
    }
}

mlir::Value DBLowering::lowerBlockOverRows(mlir::Block& block, mlir::Value cardinality) {
    const mlir::Value previousInnermostCardinality = _innermostCardinality;
    _innermostCardinality = cardinality;

    mlir::Value value;
    for (mlir::Operation& operation : block) {
        if (operation.hasTrait<mlir::OpTrait::IsTerminator>()) {
            value = mapValue(operation.getOperand(0));
        } else {
            lowerOperation(operation);
        }
    }

    _innermostCardinality = previousInnermostCardinality;

    return rowAlignedChunk(value, cardinality);
}

mlir::Value DBLowering::cutCarriedChunks(mlir::Value mask,
                                         llvm::ArrayRef<mlir::Value> chunks,
                                         llvm::function_ref<bool(size_t)> isRead,
                                         llvm::SmallVectorImpl<mlir::Value>& cut) {
    llvm::SmallVector<size_t, 8> readIndices;
    llvm::SmallVector<mlir::Value, 8> readChunks;
    for (size_t carriedIndex = 0; carriedIndex < chunks.size(); carriedIndex++) {
        if (isRead(carriedIndex)) {
            readIndices.push_back(carriedIndex);
            readChunks.push_back(chunks[carriedIndex]);
        }
    }

    llvm::SmallVector<mlir::Value, 8> filtered;
    filterCaseRows(mask, readChunks, filtered);

    cut.assign(chunks.size(), mlir::Value());
    for (size_t readIndex = 0; readIndex < readIndices.size(); readIndex++) {
        cut[readIndices[readIndex]] = filtered[readIndex + 1];
    }

    return filtered.front();
}

void DBLowering::filterCaseRows(mlir::Value mask,
                                llvm::ArrayRef<mlir::Value> chunks,
                                llvm::SmallVectorImpl<mlir::Value>& filtered) {
    llvm::SmallVector<mlir::Value, 8> columns {mask};
    columns.append(chunks.begin(), chunks.end());

    llvm::SmallVector<mlir::Type, 8> types;
    for (const mlir::Value column : columns) {
        types.push_back(column.getType());
    }

    setInsertionForNaryOp(columns);

    nl::Filter filter = _builder.create<nl::Filter>(_builder.getUnknownLoc(), types, mask, columns);

    const mlir::ResultRange results = filter.getResults();
    filtered.assign(results.begin(), results.end());
}

void DBLowering::lowerNot(mlir::db::NotOp notOp) {
    const mlir::Value operandChunk = laidOutNullAs(mapValue(notOp.getOperand()), _builder.getI1Type());

    const mlir::Type operandType = operandChunk.getType();

    // A cell may hold a null, so its negation may be one
    const bool resultNull = isNullableChunk(operandType) || isTaggedCellChunk(operandType);

    mlir::MLIRContext* bldCtxt = _builder.getContext();

    mlir::Type resultElement = storage::BoolType::get(bldCtxt);
    if (resultNull) {
        resultElement = storage::NullableType::get(bldCtxt, _builder.getI1Type());
    }

    // NOT of an unknown truth value is unknown, and an untyped null names no boolean
    // column to carry it: the negation is the same untyped null its operand is
    if (isUntypedNullChunk(operandType)) {
        resultElement = mlir::cast<nl::ChunkType>(operandType).getElementType();
    }

    const nl::ChunkType resultType = nl::ChunkType::get(bldCtxt, resultElement);

    setInsertionForUnaryOp(operandChunk);

    nl::Not nlNotOp = _builder.create<nl::Not>(_builder.getUnknownLoc(), resultType, operandChunk);
    _valueMap[notOp.getResult()] = nlNotOp.getResult();
}
