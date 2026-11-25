#include "NLOps.h"

#include <array>

#include "llvm/ADT/DenseSet.h"
#include "llvm/ADT/STLExtras.h"

#include "NLChunkTypes.h"

using namespace mlir;
using namespace mlir::nl;

namespace storage = mlir::storage;

namespace {

// The body of an op over the elements of one row's list: one block taking the element,
// the row tag and one chunk per carried column, ending in a yield
LogicalResult verifyElementBody(Operation* op, Block& bodyBlock, OperandRange carried) {
    auto yield = dyn_cast_or_null<ComprehensionYield>(bodyBlock.empty() ? nullptr : &bodyBlock.back());
    if (!yield) {
        return op->emitOpError("body region must end with an nl.comprehension_yield");
    }

    const size_t expectedArguments = carried.size() + 2;

    if (bodyBlock.getNumArguments() != expectedArguments) {
        return op->emitOpError("body region takes the element and the row tag plus one chunk per "
                               "carried column, ")
               << "expected " << expectedArguments << " but has " << bodyBlock.getNumArguments();
    }

    if (!llvm::isa<ChunkType>(bodyBlock.getArgument(0).getType())) {
        return op->emitOpError("body argument 0 must be the chunk of elements");
    }

    const auto rowTagType = llvm::dyn_cast<ChunkType>(bodyBlock.getArgument(1).getType());

    if (!rowTagType || !rowTagType.getElementType().isUnsignedInteger(64)) {
        return op->emitOpError("body argument 1 must be the ui64 chunk of row tags");
    }

    for (size_t carriedIndex = 0; carriedIndex < carried.size(); carriedIndex++) {
        const Type argumentType = bodyBlock.getArgument(carriedIndex + 2).getType();

        if (argumentType != carried[carriedIndex].getType()) {
            return op->emitOpError("body argument ") << carriedIndex + 2
                                                     << " must have the type of carried chunk "
                                                     << carriedIndex;
        }
    }

    return success();
}

// A region of a fold over the elements of one row's list: one block taking the element,
// the accumulator and one chunk per carried column, ending in a @param YieldOp
template <typename YieldOp>
LogicalResult verifyFoldRegion(Operation* op, Block& block, OperandRange carried, llvm::StringLiteral yieldName) {
    if (block.empty() || !isa<YieldOp>(block.back())) {
        return op->emitOpError("region must end with an ") << yieldName;
    }

    const size_t expectedArguments = carried.size() + 2;

    if (block.getNumArguments() != expectedArguments) {
        return op->emitOpError("region takes the element and the accumulator plus one chunk per "
                               "carried column, ")
               << "expected " << expectedArguments << " but has " << block.getNumArguments();
    }

    if (!llvm::isa<ChunkType>(block.getArgument(0).getType())) {
        return op->emitOpError("argument 0 must be the chunk of elements");
    }

    if (!llvm::isa<ChunkType>(block.getArgument(1).getType())) {
        return op->emitOpError("argument 1 must be the chunk of accumulators");
    }

    for (size_t carriedIndex = 0; carriedIndex < carried.size(); carriedIndex++) {
        const Type argumentType = block.getArgument(carriedIndex + 2).getType();

        if (argumentType != carried[carriedIndex].getType()) {
            return op->emitOpError("argument ") << carriedIndex + 2
                                                << " must have the type of carried chunk "
                                                << carriedIndex;
        }
    }

    return success();
}

bool holdsNullableBooleans(Value chunk) {
    const Type elementType = llvm::cast<ChunkType>(chunk.getType()).getElementType();
    const auto nullableType = llvm::dyn_cast<storage::NullableType>(elementType);

    return nullableType && nullableType.getValueType().isInteger(1);
}

template <typename FoldOp>
LogicalResult verifyAccumulatorWrites(FoldOp fold) {
    Block& bodyBlock = fold.getBody().front();
    const Value yielded = cast<ReduceYield>(bodyBlock.getTerminator()).getValue();
    const std::array<Type, 2> writtenTypes {fold.getInitialValue().getType(), yielded.getType()};

    return verifyCellWrites(fold.getOperation(), bodyBlock.getArgument(1).getType(), writtenTypes);
}

}

// An nl.make_list must read at least one element chunk: there would be no cell to build a
// list out of, and nothing to size the step from. A list known without reading a row is an
// nl.constant instead.
LogicalResult MakeList::verify() {
    if (getElements().empty()) {
        return emitOpError("requires at least one element chunk");
    }

    const Type elementType = llvm::cast<ChunkType>(getResult().getType()).getElementType();
    if (!llvm::isa<storage::ListType>(elementType)) {
        return emitOpError("result must be a chunk of lists");
    }

    return verifyReadAsCells(getOperation(), getElements().getTypes());
}

LogicalResult StaticMapKey::verify() {
    if (!holdsAMapPerRow(getMap().getType())) {
        return emitOpError("reads a chunk of maps or of tagged cells, optionally nullable");
    }

    if (getKey().empty()) {
        return emitOpError("reads a key, which cannot be empty");
    }

    return success();
}

LogicalResult DynamicMapKey::verify() {
    if (!holdsAMapPerRow(getMap().getType())) {
        return emitOpError("reads a chunk of maps or of tagged cells, optionally nullable");
    }

    const Type keyElement = llvm::cast<ChunkType>(getKey().getType()).getElementType();
    const auto keyNullable = llvm::dyn_cast<storage::NullableType>(keyElement);
    const Type key = keyNullable ? keyNullable.getValueType() : keyElement;

    const bool readsAStringKey = llvm::isa<storage::StringType>(key);
    const bool readsANullKey = llvm::isa<mlir::NoneType>(key);

    if (!readsAStringKey && !readsANullKey) {
        return emitOpError("reads its key out of a chunk of strings, optionally nullable");
    }

    return success();
}

LogicalResult SortMulti::verify() {
    const Value fields = getFields();
    const Value limit = getLimit();
    const Value skip = getSkip();

    const auto readsAList = [](Value chunk) {
        return isa<storage::ListType, storage::AnyType>(valueElementOf(chunk.getType()));
    };

    const auto readsAnInteger = [](Value chunk) {
        return isa<IntegerType, storage::AnyType>(valueElementOf(chunk.getType()));
    };

    const Type list = valueElementOf(getList().getType());
    const Type sorted = isa<storage::AnyType>(list) ? storage::ListType::get(getContext(), list) : list;
    const Type result = cast<ChunkType>(getResult().getType()).getElementType();
    const auto nullableResult = dyn_cast<storage::NullableType>(result);

    const bool readsLists = readsAList(getList()) && (!fields || readsAList(fields));
    const bool readsIntegerBounds = (!limit || readsAnInteger(limit)) && (!skip || readsAnInteger(skip));
    const bool answersTheInputList = nullableResult && nullableResult.getValueType() == sorted;

    if (!readsLists) {
        return emitOpError("sorts a chunk of lists or of tagged cells on one of lists of fields, optionally nullable");
    } else if (!readsIntegerBounds) {
        return emitOpError("reads its limit and its skip out of chunks of integers or of tagged cells");
    } else if (!answersTheInputList) {
        return emitOpError("result must be a chunk of nullable lists of the input list's elements");
    }

    return success();
}

LogicalResult ContainerIndex::verify() {
    const Type base = valueElementOf(getBase().getType());
    if (!isa<storage::ListType, storage::MapType, storage::AnyType>(base)) {
        return emitOpError("reads a chunk of lists, of maps or of tagged cells, optionally nullable");
    }

    if (!isa<storage::AnyType>(valueElementOf(getIndex().getType()))) {
        return emitOpError("reads its index out of a chunk of tagged cells, optionally nullable");
    }

    const Type result = cast<ChunkType>(getResult().getType()).getElementType();
    const bool answersCells = isa<storage::AnyType>(result);
    const bool answersListElements = isa<storage::ListType>(base)
                                  && isa<storage::NullableType, storage::NodeIDType, storage::EdgeIDType>(result);

    if (!answersCells && !answersListElements) {
        return emitOpError("answers tagged cells, or for a list base the nullable value, the node or the edge it holds");
    }

    return success();
}

LogicalResult MakeMap::verify() {
    const size_t valueCount = getValues().size();
    if (valueCount == 0) {
        return emitOpError("requires at least one value chunk");
    }

    const ArrayAttr keys = getKeys();
    if (keys.size() != valueCount) {
        return emitOpError("requires one key per value chunk");
    }

    llvm::SmallDenseSet<StringRef, 8> seenKeys;
    for (const Attribute key : keys) {
        if (!seenKeys.insert(llvm::cast<StringAttr>(key).getValue()).second) {
            return emitOpError("requires distinct keys");
        }
    }

    return verifyReadAsCells(getOperation(), getValues().getTypes());
}

LogicalResult MergeMaps::verify() {
    const OperandRange maps = getMaps();
    if (maps.size() < 2) {
        return emitOpError("requires at least two map chunks");
    }

    const auto operandTypes = maps.getTypes();

    if (!llvm::all_of(operandTypes, holdsAMapPerRow)) {
        return emitOpError("operands must be chunks of maps or of tagged cells");
    } else if (getResult().getType() != resultChunkType(getContext(), operandTypes)) {
        return emitOpError("result must be a chunk of maps, nullable unless every operand holds a map in every row");
    }

    return success();
}

// Only a plain chunk of maps holds one in every row, so a result over any other operand is
// nullable.
Type MergeMaps::resultChunkType(MLIRContext* context, TypeRange mapTypes) {
    const bool everyRowHoldsAMap = llvm::all_of(mapTypes, [](Type mapType) {
        return isa<storage::MapType>(cast<ChunkType>(mapType).getElementType());
    });

    const Type map = storage::MapType::get(context);
    const Type element = everyRowHoldsAMap ? map : storage::NullableType::get(context, map);

    return ChunkType::get(context, element);
}

// A bound is read as a nullable value chunk, so a row missing one gets no list, and what
// the lists hold is integers whatever the bounds were.
LogicalResult Range::verify() {
    const Type elementType = llvm::cast<ChunkType>(getResult().getType()).getElementType();

    const storage::NullableType nullableType = llvm::dyn_cast<storage::NullableType>(elementType);
    if (!nullableType) {
        return emitOpError("result must be a chunk of nullable lists");
    }

    const storage::ListType listType = llvm::dyn_cast<storage::ListType>(nullableType.getValueType());
    if (!listType) {
        return emitOpError("result must be a chunk of nullable lists");
    }

    if (!llvm::isa<mlir::IntegerType>(listType.getElementType())) {
        return emitOpError("result must be a chunk of integer lists");
    }

    return success();
}

// The body binds the element, the row tag and one chunk per carried column, and ends
// naming what each element contributes. The result holds one list per row of the source
// chunk.
LogicalResult ListComprehension::verify() {
    if (failed(verifyElementBody(getOperation(), getBody().front(), getColumnsToFilter()))) {
        return failure();
    }

    const Type elementType = llvm::cast<ChunkType>(getResult().getType()).getElementType();
    const auto nullableType = llvm::dyn_cast<storage::NullableType>(elementType);

    if (!nullableType || !llvm::isa<storage::ListType>(nullableType.getValueType())) {
        return emitOpError("result must be a chunk of nullable lists");
    }

    const Value yielded = llvm::cast<ComprehensionYield>(getBody().front().getTerminator()).getValue();

    return verifyReadAsCells(getOperation(), yielded.getType());
}

LogicalResult Reduce::verify() {
    Block& bodyBlock = getBody().front();

    if (failed(verifyFoldRegion<ReduceYield>(getOperation(), bodyBlock, getColumnsToFilter(), "nl.reduce_yield"))) {
        return failure();
    }

    if (bodyBlock.getArgument(1).getType() != getResult().getType()) {
        return emitOpError("body argument 1 must be the accumulator, of the result's type");
    }

    return verifyAccumulatorWrites(*this);
}

LogicalResult AllReduce::verify() {
    Block& bodyBlock = getBody().front();
    Block& predicateBlock = getPredicate().front();
    const OperandRange carried = getColumnsToFilter();

    if (failed(verifyFoldRegion<ReduceYield>(getOperation(), bodyBlock, carried, "nl.reduce_yield"))) {
        return failure();
    }

    if (failed(verifyFoldRegion<AllReduceYield>(getOperation(), predicateBlock, carried, "nl.all_reduce_yield"))) {
        return failure();
    }

    if (predicateBlock.getArgumentTypes() != bodyBlock.getArgumentTypes()) {
        return emitOpError("predicate region must take the arguments of the body");
    }

    if (!holdsNullableBooleans(getResult())) {
        return emitOpError("result must be a chunk of nullable booleans");
    }

    return verifyAccumulatorWrites(*this);
}

// The same body, ending on the predicate's value for each element. The result holds one
// truth value per row of the source chunk, null where the elements leave it unknown.
LogicalResult ListPredicate::verify() {
    if (failed(verifyElementBody(getOperation(), getBody().front(), getColumnsToFilter()))) {
        return failure();
    }

    if (!holdsNullableBooleans(getResult())) {
        return emitOpError("result must be a chunk of nullable booleans");
    }

    return success();
}
