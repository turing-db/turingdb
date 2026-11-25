#include "DBLowering.h"

#include <array>
#include <mlir/IR/Location.h>

#include "mlir/IR/Block.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/Types.h"
#include "mlir/Transforms/RegionUtils.h"
#include "llvm/ADT/SetVector.h"

#include "NLOps.h"
#include "DBLoweringChunkTypes.h"

#include "DBColumnElement.h"

#include "IRValueTypes.h"

#include "IRException.h"
#include "llvm/Support/Casting.h"

using namespace db;

namespace nl = mlir::nl;
namespace storage = mlir::storage;

namespace {

void collectPresentChunks(llvm::ArrayRef<mlir::Value> chunks, llvm::SmallVectorImpl<mlir::Value>& present) {
    present.clear();

    for (const mlir::Value chunk : chunks) {
        if (chunk) {
            present.push_back(chunk);
        }
    }
}

// A list over the elements @param listChunk's lists hold, read as nullable: a row with no
// list gets none. A type-erased cell holds a list of cells in its turn.
nl::ChunkType listOfSameElementsChunkType(mlir::MLIRContext* context, mlir::Value listChunk) {
    const mlir::Type listElement = mlir::cast<nl::ChunkType>(listChunk.getType()).getElementType();
    const auto nullable = mlir::dyn_cast<storage::NullableType>(listElement);
    const mlir::Type value = nullable ? nullable.getValueType() : listElement;

    const mlir::Type list =
        mlir::isa<storage::AnyType>(value)
            ? storage::ListType::get(context, storage::AnyType::get(context))
            : value;

    return nl::ChunkType::get(context, storage::NullableType::get(context, list));
}

// Whether an element type is one arithmetic promotes: the two Cypher number columns.
bool isNumericElement(mlir::Type element) {
    if (mlir::isa<mlir::Float64Type>(element)) {
        return true;
    }

    const auto integerType = mlir::dyn_cast<mlir::IntegerType>(element);

    return integerType && integerType.getWidth() == 64;
}

// The chunk shapes a key read accepts: a column of maps, and a tagged cell the row may
// have put a map in - which is what a read of a nested container hands back
bool holdsAMapPerRow(mlir::Type chunkType) {
    const nl::ChunkType chunk = mlir::dyn_cast<nl::ChunkType>(chunkType);
    if (!chunk) {
        return false;
    }

    const mlir::Type element = chunk.getElementType();
    const auto nullable = mlir::dyn_cast<storage::NullableType>(element);
    const mlir::Type read = nullable ? nullable.getValueType() : element;

    return mlir::isa<storage::MapType, storage::AnyType>(read);
}

bool holdsAnEntityPerRow(mlir::Type chunkType) {
    const nl::ChunkType chunk = mlir::dyn_cast<nl::ChunkType>(chunkType);

    return chunk && mlir::isa<storage::NodeIDType, storage::EdgeIDType>(chunk.getElementType());
}

// The element of one column holding both @param lhs and @param rhs. A value of the column
// is converted where it is chosen, so a number is promoted, which an element of a list
// never is.
mlir::Type unifiedValueElement(mlir::OpBuilder& builder, mlir::Type lhs, mlir::Type rhs) {
    if (lhs == rhs) {
        return lhs;
    } else if (isNumericElement(lhs) && isNumericElement(rhs)) {
        return promoteNumeric(builder, lhs, rhs);
    }

    return storage::joinedElementType(lhs, rhs);
}

}

void DBLowering::lowerElementOperands(mlir::Operation* op,
                                      mlir::Value source,
                                      mlir::OperandRange carried,
                                      ElementOperands& operands) {
    const mlir::Location loc = _builder.getUnknownLoc();
    mlir::MLIRContext* const context = _builder.getContext();

    for (const mlir::Value carriedColumn : carried) {
        operands._carriedChunks.push_back(mapValue(carriedColumn));
    }

    // A constant source holds one cell standing for every row rather than one per row, so
    // it is laid out over the rows it is read against - as lowerUnwind lays its own out
    const mlir::Value cardinality = cardinalityDriver(operands._carriedChunks);
    operands._sourceChunk = rowAlignedChunk(mapValue(source), cardinality);

    const mlir::Type sourceElement = mlir::cast<nl::ChunkType>(operands._sourceChunk.getType()).getElementType();

    const nl::ChunkType rowTagType = nl::ChunkType::get(context,
                                                        _builder.getIntegerType(64, /*isSigned=*/false));

    operands._argumentTypes = {nl::ChunkType::get(context, ::db::unwoundElementType(context, sourceElement)), rowTagType};
    operands._argumentLocations = {loc, loc};

    for (const mlir::Value carriedChunk : operands._carriedChunks) {
        operands._argumentTypes.push_back(carriedChunk.getType());
        operands._argumentLocations.push_back(loc);
    }

    llvm::SetVector<mlir::Value> readAbove;
    mlir::getUsedValuesDefinedAbove(op->getRegions(), readAbove);

    for (const mlir::Value value : readAbove) {
        operands._readAboveChunks.push_back(mapValue(value));
    }

    llvm::SmallVector<mlir::Value, 8> operandChunks {operands._sourceChunk};
    operandChunks.append(operands._carriedChunks.begin(), operands._carriedChunks.end());
    operandChunks.append(operands._readAboveChunks.begin(), operands._readAboveChunks.end());

    setInsertionForNaryOp(operandChunks);
}

mlir::Operation* DBLowering::lowerElementBody(mlir::Block& dbBody,
                                              mlir::Block* nlBody,
                                              mlir::Value& rowTagChunk,
                                              mlir::Value& valueChunk) {
    const mlir::Location loc = _builder.getUnknownLoc();
    const mlir::Value elementChunk = nlBody->getArgument(0);

    // Every insertion into a block goes before its terminator, so the body needs one
    // before anything is lowered into it. This one stands for the yield the lowered body
    // decides, which the caller replaces.
    _builder.setInsertionPointToEnd(nlBody);
    mlir::Operation* const placeholderYield =
        _builder.create<nl::ComprehensionYield>(loc, nlBody->getArgument(1), elementChunk);

    for (size_t argumentIndex = 0; argumentIndex < nlBody->getNumArguments(); argumentIndex++) {
        const unsigned index = static_cast<unsigned>(argumentIndex);
        _valueMap[dbBody.getArgument(index)] = nlBody->getArgument(index);
    }

    // The rows the body computes over are the elements of one row's list, so a constant it
    // reads is laid out over those rather than over the rows the op is read on
    mlir::Block* const previousInnermostLoopBody = _innermostLoopBody;
    mlir::Block* const previousInnermostRegionBody = _innermostRegionBody;
    const mlir::Value previousInnermostCardinality = _innermostCardinality;
    _innermostLoopBody = nlBody;
    _innermostRegionBody = nlBody;
    _innermostCardinality = elementChunk;

    for (mlir::Operation& operation : dbBody) {
        mlir::db::ComprehensionYield yield = mlir::dyn_cast<mlir::db::ComprehensionYield>(operation);
        if (!yield) {
            lowerOperation(operation);
            continue;
        }

        rowTagChunk = mapValue(yield.getRowTags());
        valueChunk = mapValue(yield.getValue());
    }

    _innermostLoopBody = previousInnermostLoopBody;
    _innermostRegionBody = previousInnermostRegionBody;
    _innermostCardinality = previousInnermostCardinality;

    // ComprehensionYield::verify guarantees both, so an empty one here means unverified
    // IR - the defensive backstop lowerOptionalMatch keeps too.
    if (!rowTagChunk || !valueChunk) {
        throw IRException("The body of an op over the elements of a list yields no value");
    }

    // A value computed from constants alone holds one cell standing for every element, so
    // it is laid out over the elements it is read against - the ones the tag counts, since
    // a WHERE has cut both together
    valueChunk = rowAlignedChunk(valueChunk, rowTagChunk);

    return placeholderYield;
}

void DBLowering::lowerListComprehension(mlir::db::ListComprehension comprehension) {
    const mlir::Location loc = _builder.getUnknownLoc();
    mlir::MLIRContext* const context = _builder.getContext();

    ElementOperands operands;
    lowerElementOperands(comprehension, comprehension.getSource(), comprehension.getColumnsToFilter(), operands);

    // What the body yields names the type of the lists, and that is only known once the
    // body is lowered. The op is created before it all the same, with the source's type
    // standing in: an op in the body reading a chunk bound above it takes the deeper of
    // the two blocks, which only holds when the body region hangs under them.
    nl::ListComprehension lists = _builder.create<nl::ListComprehension>(loc,
                                                                         operands._sourceChunk.getType(),
                                                                         operands._sourceChunk,
                                                                         operands._carriedChunks);

    mlir::Block* const bodyBlock = &lists.getBody().emplaceBlock();
    bodyBlock->addArguments(operands._argumentTypes, operands._argumentLocations);

    mlir::Value rowTagChunk;
    mlir::Value valueChunk;
    mlir::Operation* const placeholderYield =
        lowerElementBody(comprehension.getBody().front(), bodyBlock, rowTagChunk, valueChunk);

    valueChunk = listItemChunk(valueChunk);

    _builder.setInsertionPointToEnd(bodyBlock);
    _builder.create<nl::ComprehensionYield>(loc, rowTagChunk, valueChunk);
    eraseOp(placeholderYield);

    const mlir::Type listType = storage::ListType::get(context, listedElementType(context, valueChunk));
    const nl::ChunkType resultType = nl::ChunkType::get(context,
                                                        storage::NullableType::get(context, listType));

    mlir::Value listsChunk = lists.getResult();
    listsChunk.setType(resultType);

    _valueMap[comprehension.getResult()] = listsChunk;
}

void DBLowering::lowerListPredicate(mlir::db::ListPredicate predicate) {
    const mlir::Location loc = _builder.getUnknownLoc();
    mlir::MLIRContext* const context = _builder.getContext();

    ElementOperands operands;
    lowerElementOperands(predicate, predicate.getSource(), predicate.getColumnsToFilter(), operands);

    const nl::ChunkType resultType =
        nl::ChunkType::get(context, storage::NullableType::get(context, _builder.getI1Type()));

    nl::ListPredicate truths = _builder.create<nl::ListPredicate>(loc,
                                                                  resultType,
                                                                  operands._sourceChunk,
                                                                  operands._carriedChunks,
                                                                  predicate.getKind());

    mlir::Block* const bodyBlock = &truths.getBody().emplaceBlock();
    bodyBlock->addArguments(operands._argumentTypes, operands._argumentLocations);

    mlir::Value rowTagChunk;
    mlir::Value valueChunk;
    mlir::Operation* const placeholderYield =
        lowerElementBody(predicate.getBody().front(), bodyBlock, rowTagChunk, valueChunk);

    _builder.setInsertionPointToEnd(bodyBlock);
    _builder.create<nl::ComprehensionYield>(loc, rowTagChunk, valueChunk);
    eraseOp(placeholderYield);

    _valueMap[predicate.getResult()] = truths.getResult();
}

template <typename NLFoldYield>
mlir::Operation* DBLowering::lowerFoldRegion(mlir::Block& dbBlock, mlir::Block* nlBlock, mlir::Value& valueChunk) {
    const mlir::Value elementChunk = nlBlock->getArgument(0);

    // The yield the region decides is only known once it is lowered, and every insertion
    // goes before a block's terminator, so one stands in for it meanwhile
    _builder.setInsertionPointToEnd(nlBlock);
    mlir::Operation* const placeholderYield = _builder.create<NLFoldYield>(_builder.getUnknownLoc(), elementChunk);

    for (size_t argumentIndex = 0; argumentIndex < nlBlock->getNumArguments(); argumentIndex++) {
        const unsigned index = static_cast<unsigned>(argumentIndex);
        _valueMap[dbBlock.getArgument(index)] = nlBlock->getArgument(index);
    }

    mlir::Block* const previousInnermostLoopBody = _innermostLoopBody;
    mlir::Block* const previousInnermostRegionBody = _innermostRegionBody;
    const mlir::Value previousInnermostCardinality = _innermostCardinality;
    _innermostLoopBody = nlBlock;
    _innermostRegionBody = nlBlock;
    _innermostCardinality = elementChunk;

    for (mlir::Operation& operation : dbBlock.without_terminator()) {
        lowerOperation(operation);
    }

    valueChunk = mapValue(dbBlock.getTerminator()->getOperand(0));

    _innermostLoopBody = previousInnermostLoopBody;
    _innermostRegionBody = previousInnermostRegionBody;
    _innermostCardinality = previousInnermostCardinality;

    valueChunk = rowAlignedChunk(valueChunk, elementChunk);

    return placeholderYield;
}

template <typename NLFoldOp, typename DBFoldOp>
NLFoldOp DBLowering::lowerFold(DBFoldOp fold, FoldResultType resultTypeOf) {
    const mlir::Location loc = _builder.getUnknownLoc();

    ElementOperands operands;
    lowerElementOperands(fold, fold.getSource(), fold.getColumnsToFilter(), operands);

    const mlir::Value initialChunk = rowAlignedChunk(mapValue(fold.getInitialValue()), operands._sourceChunk);

    // The body is lowered over an accumulator of the type the initial value has, and again
    // over a wider one whenever what it computes does not fit: `s + x` over an integer s
    // and a float x needs a float s. Each round widens, so this settles.
    mlir::Type accumulated = accumulatedElement(mlir::Type {}, initialChunk.getType());

    while (true) {
        const nl::ChunkType accumulatorType = accumulatorChunkType(accumulated);
        const mlir::Value initialAccumulator = chunkAsElement(initialChunk, accumulated);

        llvm::SmallVector<mlir::Value, 8> operandChunks {operands._sourceChunk, initialAccumulator};
        operandChunks.append(operands._carriedChunks.begin(), operands._carriedChunks.end());
        operandChunks.append(operands._readAboveChunks.begin(), operands._readAboveChunks.end());
        setInsertionForNaryOp(operandChunks);

        NLFoldOp folded = _builder.create<NLFoldOp>(loc,
                                                    resultTypeOf(accumulatorType),
                                                    operands._sourceChunk,
                                                    initialAccumulator,
                                                    operands._carriedChunks);

        llvm::SmallVector<mlir::Type, 4> argumentTypes = operands._argumentTypes;
        argumentTypes[1] = accumulatorType;

        mlir::Block* const bodyBlock = &folded.getBody().emplaceBlock();
        bodyBlock->addArguments(argumentTypes, operands._argumentLocations);

        mlir::Value valueChunk;
        mlir::Operation* const placeholderYield = lowerFoldRegion<nl::ReduceYield>(fold.getBody().front(), bodyBlock, valueChunk);

        const mlir::Type widened = accumulatedElement(accumulated, valueChunk.getType());
        if (widened != accumulated) {
            eraseOp(folded);
            accumulated = widened;
            continue;
        }

        valueChunk = chunkAsElement(valueChunk, accumulated);

        _builder.setInsertionPointToEnd(bodyBlock);
        _builder.create<nl::ReduceYield>(loc, valueChunk);
        eraseOp(placeholderYield);

        return folded;
    }
}

void DBLowering::lowerReduce(mlir::db::Reduce reduce) {
    nl::Reduce reduced = lowerFold<nl::Reduce>(reduce, [](nl::ChunkType accumulatorType) {
        return accumulatorType;
    });

    _valueMap[reduce.getResult()] = reduced.getResult();
}

void DBLowering::lowerAllReduce(mlir::db::AllReduce allReduce) {
    const mlir::Location loc = _builder.getUnknownLoc();
    mlir::MLIRContext* const context = _builder.getContext();
    const nl::ChunkType resultType =
        nl::ChunkType::get(context, storage::NullableType::get(context, _builder.getI1Type()));

    nl::AllReduce decided = lowerFold<nl::AllReduce>(allReduce, [resultType](nl::ChunkType) {
        return resultType;
    });

    mlir::Block& bodyBlock = decided.getBody().front();
    mlir::Block* const predicateBlock = &decided.getPredicate().emplaceBlock();
    const llvm::SmallVector<mlir::Location, 4> argumentLocations(bodyBlock.getNumArguments(), loc);
    predicateBlock->addArguments(bodyBlock.getArgumentTypes(), argumentLocations);

    mlir::Value holdsChunk;
    mlir::Operation* const placeholderYield =
        lowerFoldRegion<nl::AllReduceYield>(allReduce.getPredicate().front(), predicateBlock, holdsChunk);

    _builder.setInsertionPointToEnd(predicateBlock);
    _builder.create<nl::AllReduceYield>(loc, holdsChunk);
    eraseOp(placeholderYield);

    _valueMap[allReduce.getResult()] = decided.getResult();
}

mlir::Type DBLowering::accumulatedElement(mlir::Type accumulated, mlir::Type chunkType) {
    if (isUntypedNullChunk(chunkType)) {
        return accumulated;
    }

    const mlir::Type element = chunkValueElement(_builder, chunkType);
    if (!accumulated) {
        return element;
    }

    return unifiedValueElement(_builder, accumulated, element);
}

nl::ChunkType DBLowering::accumulatorChunkType(mlir::Type accumulated) {
    mlir::MLIRContext* const context = _builder.getContext();

    // An accumulator that only ever held the null literal is that literal, which a CASE
    // branch or a null test reads as the null it is rather than as a value of some type
    if (!accumulated) {
        return nl::ChunkType::get(context, storage::NullableType::get(context, _builder.getNoneType()));
    }

    return caseResultChunkType(accumulated);
}

mlir::Type DBLowering::listedElementType(mlir::MLIRContext* context, llvm::ArrayRef<mlir::Value> chunks) {
    mlir::Type shared = mlir::NoneType::get(context);

    for (const mlir::Value chunk : chunks) {
        mlir::Type element = mlir::cast<nl::ChunkType>(chunk.getType()).getElementType();

        // A cell that is absent is a tagged null of the list its neighbours are in, so a
        // nullable column names the type of the value it holds.
        if (const auto nullable = mlir::dyn_cast<storage::NullableType>(element)) {
            element = nullable.getValueType();
        }

        shared = storage::joinedElementType(shared, element);
    }

    return shared;
}

void DBLowering::lowerDynamicMapKey(mlir::db::DynamicMapKey mapKey) {
    const mlir::Value mapChunk = mapValue(mapKey.getMap());
    const mlir::Value keyChunk = mapValue(mapKey.getKey());

    if (!holdsAMapPerRow(mapChunk.getType())) {
        throw IRException("db.dynamic_map_key reads from a map column, or from a column of tagged cells");
    }

    const mlir::Type keyElement = mlir::cast<nl::ChunkType>(keyChunk.getType()).getElementType();
    const auto nullableKey = mlir::dyn_cast<storage::NullableType>(keyElement);
    const mlir::Type key = nullableKey ? nullableKey.getValueType() : keyElement;

    const bool readsAStringKey = mlir::isa<storage::StringType>(key);
    const bool readsANullKey = mlir::isa<mlir::NoneType>(key);

    if (!readsAStringKey && !readsANullKey) {
        throw IRException("db.dynamic_map_key reads its key out of a string column");
    }

    // A cell carries its own null in its tag, so the result is never wrapped in a
    // nullable: a missing key, an absent map and a null key all read back as a Null-tagged
    // cell
    const nl::ChunkType resultType = nl::ChunkType::get(_builder.getContext(),
                                                        storage::AnyType::get(_builder.getContext()));

    setInsertionForBinaryOp(mapChunk, keyChunk);

    nl::DynamicMapKey value = _builder.create<nl::DynamicMapKey>(_builder.getUnknownLoc(),
                                                                 resultType,
                                                                 mapChunk,
                                                                 keyChunk);

    _valueMap[mapKey.getResult()] = value.getResult();
}

void DBLowering::lowerContainerIndex(mlir::db::ContainerIndex index) {
    const mlir::Value baseChunk = mapValue(index.getBase());
    const mlir::Value indexChunk = mapValue(index.getIndex());

    const mlir::Type baseType = baseChunk.getType();
    mlir::MLIRContext* const context = _builder.getContext();

    const mlir::Type resultElement = isListChunk(baseType)
                                         ? binaryResultElement(BinaryResultKind::Index, baseType, indexChunk.getType())
                                         : storage::AnyType::get(context);

    const nl::ChunkType resultType = nl::ChunkType::get(context, resultElement);

    setInsertionForBinaryOp(baseChunk, indexChunk);

    nl::ContainerIndex element = _builder.create<nl::ContainerIndex>(_builder.getUnknownLoc(),
                                                                     resultType,
                                                                     baseChunk,
                                                                     indexChunk);

    _valueMap[index.getResult()] = element.getResult();
}

void DBLowering::lowerStaticMapKey(mlir::db::StaticMapKey mapKey) {
    mlir::Value mapChunk = mapValue(mapKey.getMap());

    // A cell carries its own null in its tag, so the result is never wrapped in a
    // nullable: a missing key and an absent map both read back as a Null-tagged cell
    const nl::ChunkType resultType = nl::ChunkType::get(_builder.getContext(),
                                                        storage::AnyType::get(_builder.getContext()));

    const mlir::Type mapChunkType = mapChunk.getType();
    if (holdsAnEntityPerRow(mapChunkType)) {
        mapChunk = toAnyChunk(mapChunk, resultType);
    } else if (!holdsAMapPerRow(mapChunkType)) {
        throw IRException("db.static_map_key reads from a map column, a column of tagged cells, or a column of nodes or edges");
    }

    setInsertionForUnaryOp(mapChunk);

    nl::StaticMapKey value = _builder.create<nl::StaticMapKey>(_builder.getUnknownLoc(),
                                                   resultType,
                                                   mapChunk,
                                                   mapKey.getKeyAttr());

    _valueMap[mapKey.getResult()] = value.getResult();
}

void DBLowering::containerCellChunks(mlir::ValueRange columns, llvm::SmallVectorImpl<mlir::Value>& chunks) {
    for (const mlir::Value column : columns) {
        chunks.push_back(mapValue(column));
    }

    // An element holding one value for every row rather than one per row is laid out over
    // the rows the others carry, so every cell of a container is read at the same row index.
    const mlir::Value cardinality = cardinalityDriver(chunks);

    for (mlir::Value& chunk : chunks) {
        chunk = listItemChunk(rowAlignedChunk(chunk, cardinality));
    }
}

void DBLowering::lowerMakeList(mlir::db::MakeList makeList) {
    // MakeList::verify guarantees an element column, so an empty operand list here means
    // unverified IR - the defensive backstop lowerCollect keeps too.
    if (makeList.getElements().empty()) {
        throw IRException("db.make_list requires at least one element column");
    }

    llvm::SmallVector<mlir::Value, 4> chunks;
    containerCellChunks(makeList.getElements(), chunks);

    mlir::MLIRContext* const context = _builder.getContext();
    const mlir::Type listType = storage::ListType::get(context, listedElementType(context, chunks));
    const nl::ChunkType resultType = nl::ChunkType::get(context, listType);

    setInsertionForNaryOp(chunks);

    nl::MakeList lists = _builder.create<nl::MakeList>(_builder.getUnknownLoc(), resultType, chunks);
    _valueMap[makeList.getResult()] = lists.getResult();
}

void DBLowering::rowAlignOptionalChunks(llvm::MutableArrayRef<mlir::Value> chunks) {
    llvm::SmallVector<mlir::Value, 4> present;
    collectPresentChunks(chunks, present);

    const mlir::Value cardinality = cardinalityDriver(present);

    for (mlir::Value& chunk : chunks) {
        if (chunk) {
            chunk = rowAlignedChunk(chunk, cardinality);
        }
    }
}

void DBLowering::lowerListSlice(mlir::db::ListSlice slice) {
    const mlir::Location loc = _builder.getUnknownLoc();
    mlir::MLIRContext* const context = _builder.getContext();

    const mlir::Value from = slice.getFrom();
    const mlir::Value to = slice.getTo();

    // A list or a bound holding one value for every row rather than one per row is laid
    // out over the rows the others carry, as lowerRange lays its bounds out
    std::array<mlir::Value, 3> chunks {
        mapValue(slice.getList()),
        from ? mapValue(from) : mlir::Value {},
        to ? mapValue(to) : mlir::Value {},
    };
    rowAlignOptionalChunks(chunks);

    auto& [listChunk, fromChunk, toChunk] = chunks;

    if (fromChunk) {
        fromChunk = numberOperandChunk(fromChunk);
    }

    if (toChunk) {
        toChunk = numberOperandChunk(toChunk);
    }

    const nl::ChunkType resultType = listOfSameElementsChunkType(context, listChunk);

    // The op goes where the chunks it reads stand, which are the laid-out ones
    llvm::SmallVector<mlir::Value, 3> operands;
    collectPresentChunks(chunks, operands);
    setInsertionForNaryOp(operands);

    nl::ListSlice run = _builder.create<nl::ListSlice>(loc, resultType, listChunk, fromChunk, toChunk);

    _valueMap[slice.getResult()] = run.getResult();
}

void DBLowering::lowerSortMulti(mlir::db::SortMulti sortMulti) {
    const mlir::Location loc = _builder.getUnknownLoc();
    mlir::MLIRContext* const context = _builder.getContext();

    const mlir::Value fields = sortMulti.getFields();
    const mlir::Value limit = sortMulti.getLimit();
    const mlir::Value skip = sortMulti.getSkip();

    std::array<mlir::Value, 4> chunks {
        mapValue(sortMulti.getList()),
        fields ? mapValue(fields) : mlir::Value {},
        limit ? mapValue(limit) : mlir::Value {},
        skip ? mapValue(skip) : mlir::Value {},
    };
    rowAlignOptionalChunks(chunks);

    auto& [listChunk, fieldsChunk, limitChunk, skipChunk] = chunks;

    if (limitChunk) {
        limitChunk = numberOperandChunk(limitChunk);
    }

    if (skipChunk) {
        skipChunk = numberOperandChunk(skipChunk);
    }

    const nl::ChunkType resultType = listOfSameElementsChunkType(context, listChunk);

    llvm::SmallVector<mlir::Value, 4> operands;
    collectPresentChunks(chunks, operands);
    setInsertionForNaryOp(operands);

    nl::SortMulti sorted = _builder.create<nl::SortMulti>(loc, resultType, listChunk, fieldsChunk, limitChunk, skipChunk);

    _valueMap[sortMulti.getResult()] = sorted.getResult();
}

void DBLowering::lowerPercentile(mlir::Operation* operation) {
    const mlir::Location loc = _builder.getUnknownLoc();
    mlir::MLIRContext* const context = _builder.getContext();

    mlir::Value listChunk = mapValue(operation->getOperand(0));
    mlir::Value percentileChunk = mapValue(operation->getOperand(1));

    const mlir::Value cardinality = cardinalityDriver({listChunk, percentileChunk});
    listChunk = rowAlignedChunk(listChunk, cardinality);
    percentileChunk = numberOperandChunk(rowAlignedChunk(percentileChunk, cardinality));

    // percentileDisc answers one of the values, which keeps the number type a list of
    // numbers names; a list of tagged cells - or of untyped nulls - answers the tagged
    // cell, which carries its own null
    const mlir::Type listElement = mlir::cast<nl::ChunkType>(listChunk.getType()).getElementType();
    const auto listType = mlir::dyn_cast<storage::ListType>(listElement);
    const mlir::Type valueType = listType ? listType.getElementType() : mlir::Type {};

    const auto integerType = mlir::dyn_cast_if_present<mlir::IntegerType>(valueType);
    const bool listsIntegers = integerType && integerType.getWidth() == 64;
    const bool listsDoubles = valueType && mlir::isa<mlir::Float64Type>(valueType);

    const bool interpolates = mlir::isa<mlir::db::PercentileCont>(operation);
    const bool listsNumbers = listsIntegers || listsDoubles;

    mlir::Type resultElement = storage::AnyType::get(context);
    if (interpolates) {
        resultElement = storage::NullableType::get(context, _builder.getF64Type());
    } else if (listsNumbers) {
        resultElement = storage::NullableType::get(context, valueType);
    }

    const nl::ChunkType resultType = nl::ChunkType::get(context, resultElement);

    setInsertionForNaryOp({listChunk, percentileChunk});

    mlir::Value result;
    if (interpolates) {
        result = _builder.create<nl::PercentileCont>(loc, resultType, listChunk, percentileChunk).getResult();
    } else {
        result = _builder.create<nl::PercentileDisc>(loc, resultType, listChunk, percentileChunk).getResult();
    }

    _valueMap[operation->getResult(0)] = result;
}

void DBLowering::lowerMakeMap(mlir::db::MakeMap makeMap) {
    if (makeMap.getValues().empty()) {
        throw IRException("db.make_map requires at least one value column");
    }

    llvm::SmallVector<mlir::Value, 4> chunks;
    containerCellChunks(makeMap.getValues(), chunks);

    setInsertionForNaryOp(chunks);

    nl::MakeMap maps = _builder.create<nl::MakeMap>(_builder.getUnknownLoc(), chunks, makeMap.getKeys());
    _valueMap[makeMap.getResult()] = maps.getResult();
}

void DBLowering::lowerMergeMaps(mlir::db::MergeMaps merge) {
    llvm::SmallVector<mlir::Value, 4> maps;
    for (const mlir::Value column : merge.getMaps()) {
        maps.push_back(mapValue(column));
    }

    const mlir::Value cardinality = cardinalityDriver(maps);

    for (mlir::Value& map : maps) {
        map = rowAlignedChunk(map, cardinality);

        if (!holdsAMapPerRow(map.getType())) {
            throw IRException("db.merge_maps merges map columns, or columns of tagged cells");
        }
    }

    const mlir::Type resultType = nl::MergeMaps::resultChunkType(_builder.getContext(), mlir::ValueRange(maps).getTypes());

    setInsertionForNaryOp(maps);

    nl::MergeMaps merged = _builder.create<nl::MergeMaps>(_builder.getUnknownLoc(), resultType, maps);
    _valueMap[merge.getResult()] = merged.getResult();
}

void DBLowering::lowerRange(mlir::db::Range range) {
    llvm::SmallVector<mlir::Value, 3> bounds {mapValue(range.getStart()), mapValue(range.getEnd())};

    const mlir::Value step = range.getStep();
    if (step) {
        bounds.push_back(mapValue(step));
    }

    // A bound holding one value for every row rather than one per row is laid out over the
    // rows the others carry, as lowerMakeList lays its element columns out
    const mlir::Value cardinality = cardinalityDriver(bounds);

    for (mlir::Value& bound : bounds) {
        bound = numberOperandChunk(rowAlignedChunk(bound, cardinality));
    }

    mlir::MLIRContext* const context = _builder.getContext();
    const mlir::Type listType = storage::ListType::get(context, _builder.getI64Type());
    const nl::ChunkType resultType = nl::ChunkType::get(context,
                                                        storage::NullableType::get(context, listType));

    setInsertionForNaryOp(bounds);

    nl::Range lists = _builder.create<nl::Range>(_builder.getUnknownLoc(),
                                                 resultType,
                                                 bounds[0],
                                                 bounds[1],
                                                 step ? bounds[2] : mlir::Value {});

    _valueMap[range.getResult()] = lists.getResult();
}
