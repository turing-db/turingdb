#include "DBLowering.h"

#include <array>
#include <mlir/IR/Location.h>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>

#include "mlir/IR/Block.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/Types.h"
#include "mlir/IR/Verifier.h"

#include "NLOps.h"
#include "DBLoweringChunkTypes.h"
#include "DBLoweringFunctionEmitters.h"
#include "DBLoweringRowFlow.h"

#include "DBColumnElement.h"
#include "IRConstantColumn.h"
#include "IRRowAlignment.h"
#include "MergePatternShape.h"
#include "NullLiteralType.h"
#include "ProcedureManager.h"

#include "views/GraphView.h"
#include "metadata/GraphMetadata.h"
#include "metadata/PropertyType.h"

#include "IRValueTypes.h"

#include "IRException.h"
#include "llvm/ADT/SmallPtrSet.h"
#include "llvm/Support/Casting.h"

using namespace db;

namespace nl = mlir::nl;
namespace storage = mlir::storage;

namespace {

bool isDefinedWithin(mlir::Value value, mlir::Operation* operation) {
    mlir::Operation* const definingOp = value.getDefiningOp();
    mlir::Operation* const definer = definingOp ? definingOp : value.getParentBlock()->getParentOp();

    return operation->isAncestor(definer);
}

template <typename Key>
void forgetValuesDefinedWithin(mlir::Operation* operation, llvm::DenseMap<Key, mlir::Value>& map) {
    map.remove_if([operation](const auto& entry) {
        return isDefinedWithin(entry.second, operation);
    });
}

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

// The name a result column's value type goes out under. A rejection is read by whoever
// wrote the query, so it names the type the value has rather than the chunk spelling it
// is carried in.
void describeColumnType(mlir::Type chunkType, std::string& out) {
    out.clear();

    mlir::Type element = mlir::cast<nl::ChunkType>(chunkType).getElementType();
    if (const auto nullable = mlir::dyn_cast<storage::NullableType>(element)) {
        element = nullable.getValueType();
    }

    if (mlir::isa<storage::StringType>(element)) {
        out = "String";
    } else if (mlir::isa<storage::BoolType>(element)) {
        out = "Bool";
    } else if (mlir::isa<mlir::NoneType>(element)) {
        out = "Null";
    } else if (mlir::isa<mlir::Float64Type>(element)) {
        out = "Double";
    } else if (mlir::isa<storage::NodeIDType>(element)) {
        out = "Node";
    } else if (mlir::isa<storage::EdgeIDType>(element)) {
        out = "Edge";
    } else if (mlir::isa<storage::ListType>(element)) {
        out = "List";
    } else if (mlir::isa<storage::EmbeddingType>(element)) {
        out = "Embedding";
    } else if (mlir::isa<storage::PathType>(element)) {
        out = "Path";
    } else if (const auto integer = mlir::dyn_cast<mlir::IntegerType>(element)) {
        if (integer.getWidth() == 1) {
            out = "Bool";
        } else if (integer.isUnsigned()) {
            out = "UInt64";
        } else {
            out = "Int64";
        }
    } else {
        llvm::raw_string_ostream stream(out);
        stream << element;
    }
}

// The nl sibling of each db system command. They are copied across one for one -
// same attributes under the same names, each db column result becoming the chunk
// the command fills - so the whole family lowers through one table instead of a
// branch apiece.
const llvm::StringMap<llvm::StringRef> systemCommandSiblings = {
    {mlir::db::LoadGraph::getOperationName(),           nl::LoadGraph::getOperationName()},
    {mlir::db::CreateGraph::getOperationName(),         nl::CreateGraph::getOperationName()},
    {mlir::db::ImportGraph::getOperationName(),         nl::ImportGraph::getOperationName()},
    {mlir::db::ListGraphs::getOperationName(),          nl::ListGraphs::getOperationName()},
    {mlir::db::ListAvailableGraphs::getOperationName(), nl::ListAvailableGraphs::getOperationName()},
    {mlir::db::ChangeCommand::getOperationName(),       nl::ChangeCommand::getOperationName()},
    {mlir::db::CommitChange::getOperationName(),        nl::CommitChange::getOperationName()},
    {mlir::db::LoadCommit::getOperationName(),          nl::LoadCommit::getOperationName()},
    {mlir::db::MergeDataParts::getOperationName(),      nl::MergeDataParts::getOperationName()},
    {mlir::db::S3Connect::getOperationName(),           nl::S3Connect::getOperationName()},
    {mlir::db::S3Transfer::getOperationName(),          nl::S3Transfer::getOperationName()},
    {mlir::db::ShowProcedures::getOperationName(),      nl::ShowProcedures::getOperationName()},
    {mlir::db::InstallExtension::getOperationName(),    nl::InstallExtension::getOperationName()},
    {mlir::db::ShowExtensions::getOperationName(),      nl::ShowExtensions::getOperationName()},
    {mlir::db::CreateVectorIndex::getOperationName(),   nl::CreateVectorIndex::getOperationName()},
    {mlir::db::DeleteVectorIndex::getOperationName(),   nl::DeleteVectorIndex::getOperationName()},
    {mlir::db::ShowVectorIndexes::getOperationName(),   nl::ShowVectorIndexes::getOperationName()},
    {mlir::db::LoadVector::getOperationName(),          nl::LoadVector::getOperationName()},
    {mlir::db::LoadEmbedding::getOperationName(),       nl::LoadEmbedding::getOperationName()},
    {mlir::db::CreatePropertyIndex::getOperationName(), nl::CreatePropertyIndex::getOperationName()},
    {mlir::db::DropIndex::getOperationName(),           nl::DropIndex::getOperationName()},
    {mlir::db::Explain::getOperationName(),             nl::Explain::getOperationName()},
};

// The accumulator (and result) element type of an aggregate over inputElement, the value
// type its nullable chunk wraps or the tagged cell itself: stdev is an f64, avg is one over
// numbers, and the rest keep the input's type. Throws for a value type the reduction cannot
// take, so a string sum or an embedding min is rejected as Cypher rejects it.
mlir::Type aggregateResultElementType(mlir::OpBuilder& builder,
                                      storage::AggregateKind kind,
                                      mlir::Type inputElement) {
    const bool isFloat = mlir::isa<mlir::Float64Type>(inputElement);
    const auto integerType = mlir::dyn_cast<mlir::IntegerType>(inputElement);
    const bool isBool = integerType && integerType.getWidth() == 1;
    const bool isInteger = integerType && !isBool;
    const bool isNumeric = isFloat || isInteger;
    const bool isString = mlir::isa<storage::StringType>(inputElement);
    const bool isDateTime = mlir::isa<storage::DateTimeType>(inputElement);
    const bool isDuration = mlir::isa<storage::DurationType>(inputElement);
    const bool isPoint = mlir::isa<storage::PointType>(inputElement);
    const bool isTaggedCell = mlir::isa<storage::AnyType>(inputElement);

    // An untyped null holds no value to reduce - a name no property in the graph carries,
    // or the null literal - so every reduction over it sees nothing: min, max and avg
    // answer null, sum answers 0 and stdev 0.0. It names no value type either, so the
    // answer rides the integer column an untyped null is laid out over anywhere else,
    // except avg's and stdev's, which are floats whatever they reduced.
    if (mlir::isa<mlir::NoneType>(inputElement)) {
        if (storage::reducesToADouble(kind, inputElement)) {
            return builder.getF64Type();
        }

        return builder.getIntegerType(64);
    }

    switch (kind) {
        case storage::AggregateKind::Sum: {
            if (!isNumeric && !isDuration && !isTaggedCell) {
                throw IRException("db.sum requires a numeric or a duration column");
            }
            return inputElement;
        }
        break;

        case storage::AggregateKind::Avg: {
            if (!isNumeric && !isDuration && !isPoint && !isTaggedCell) {
                throw IRException("db.avg requires a numeric, a duration or a point column");
            }
            return storage::reducesToADouble(kind, inputElement) ? builder.getF64Type() : inputElement;
        }
        break;

        case storage::AggregateKind::StDev:
        case storage::AggregateKind::StDevP: {
            if (!isNumeric && !isTaggedCell) {
                throw IRException("db.stdev/db.stdevp requires a numeric column");
            }
            return builder.getF64Type();
        }
        break;

        case storage::AggregateKind::Min:
        case storage::AggregateKind::Max: {
            // min/max order the values, so anything Cypher orders is fine - numbers,
            // strings, bools, instants, points, lists, maps, and cells of different types -
            // but an embedding has no order.
            const bool isList = mlir::isa<storage::ListType>(inputElement);
            const bool isMap = mlir::isa<storage::MapType>(inputElement);
            const bool isOrderable = isNumeric || isString || isBool || isDateTime || isDuration
                                  || isPoint || isList || isMap || isTaggedCell;
            if (!isOrderable) {
                throw IRException("db.min/db.max requires an orderable column");
            }
            return inputElement;
        }
        break;
    }

    throw IRException("Unhandled aggregate kind");
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

// Whether a value of @param element can be held as a tagged cell
bool holdsAsTaggedCell(mlir::Type element) {
    const bool isContainer = mlir::isa<storage::ListType, storage::MapType>(element);
    const bool isEntityOrCell = mlir::isa<storage::NodeIDType, storage::EdgeIDType, storage::AnyType>(element);

    return isContainer
        || isEntityOrCell
        || storage::namesASchemaID(element)
        || elementTypeToValueType(element) != ValueType::Invalid;
}

// The value-reduction AggregateKind matching a grouped aggregate's kind, so the
// grouped result-type resolution reuses aggregateResultElementType. count and
// count_distinct tally rows rather than reducing values, so they have no
// value-reduction kind and never reach here (the caller resolves their result type -
// a ui64 - directly).
storage::AggregateKind groupKindToAggregateKind(storage::GroupAggregateKind kind) {
    switch (kind) {
        case storage::GroupAggregateKind::Sum:
        case storage::GroupAggregateKind::SumDistinct:
            return storage::AggregateKind::Sum;
        break;

        case storage::GroupAggregateKind::Min:
            return storage::AggregateKind::Min;
        break;

        case storage::GroupAggregateKind::Max:
            return storage::AggregateKind::Max;
        break;

        case storage::GroupAggregateKind::Avg:
        case storage::GroupAggregateKind::AvgDistinct:
            return storage::AggregateKind::Avg;
        break;

        case storage::GroupAggregateKind::StDev:
        case storage::GroupAggregateKind::StDevDistinct:
            return storage::AggregateKind::StDev;
        break;

        case storage::GroupAggregateKind::StDevP:
        case storage::GroupAggregateKind::StDevPDistinct:
            return storage::AggregateKind::StDevP;
        break;

        case storage::GroupAggregateKind::Count:
        case storage::GroupAggregateKind::CountDistinct:
        case storage::GroupAggregateKind::CountRows:
            throw IRException("count has no value-reduction kind");
        break;

        case storage::GroupAggregateKind::First:
            throw IRException("first has no value-reduction kind");
        break;
    }

    throw IRException("Unhandled group aggregate kind");
}

// Whether a grouped aggregate reduces the values of its input column rather than tallying
// its rows: the reductions read a nullable value chunk, while the counts read the chunk
// they are anchored on as it comes - an ID chunk for count(*).
bool reducesValues(storage::GroupAggregateKind kind) {
    switch (kind) {
        case storage::GroupAggregateKind::Sum:
        case storage::GroupAggregateKind::SumDistinct:
        case storage::GroupAggregateKind::Min:
        case storage::GroupAggregateKind::Max:
        case storage::GroupAggregateKind::Avg:
        case storage::GroupAggregateKind::AvgDistinct:
        case storage::GroupAggregateKind::StDev:
        case storage::GroupAggregateKind::StDevP:
        case storage::GroupAggregateKind::StDevDistinct:
        case storage::GroupAggregateKind::StDevPDistinct:
        case storage::GroupAggregateKind::First:
            return true;
        break;

        case storage::GroupAggregateKind::Count:
        case storage::GroupAggregateKind::CountDistinct:
        case storage::GroupAggregateKind::CountRows:
            return false;
        break;
    }

    throw IRException("Unhandled group aggregate kind");
}

// The nl chunk type of one grouped aggregate's result column, resolved from its
// kind and input chunk. A switch (not an if/else) over every GroupAggregateKind so
// a new kind is a compile error here rather than silently taking the value-reduction
// path: count and count_distinct are a single non-null unsigned i64 per group;
// sum/min/max/avg reduce the input's values, so they require a nullable value column
// (an ID column is rejected) and follow aggregateResultElementType - sum/min/max keep
// the value type, avg widens numbers to f64.
nl::ChunkType groupAggregateResultChunkType(mlir::OpBuilder& builder,
                                            storage::GroupAggregateKind kind,
                                            mlir::Value inputChunk,
                                            nl::ChunkType countChunkType) {
    mlir::MLIRContext* const context = builder.getContext();

    switch (kind) {
        case storage::GroupAggregateKind::Count:
        case storage::GroupAggregateKind::CountDistinct:
        case storage::GroupAggregateKind::CountRows:
            return countChunkType;
        break;

        case storage::GroupAggregateKind::Sum:
        case storage::GroupAggregateKind::SumDistinct:
        case storage::GroupAggregateKind::Min:
        case storage::GroupAggregateKind::Max:
        case storage::GroupAggregateKind::Avg:
        case storage::GroupAggregateKind::AvgDistinct:
        case storage::GroupAggregateKind::StDev:
        case storage::GroupAggregateKind::StDevP:
        case storage::GroupAggregateKind::StDevDistinct:
        case storage::GroupAggregateKind::StDevPDistinct:
        case storage::GroupAggregateKind::First: {
            const nl::ChunkType inputChunkType = mlir::cast<nl::ChunkType>(inputChunk.getType());
            const mlir::Type inputElement = inputChunkType.getElementType();
            const auto inputNullable = mlir::dyn_cast<storage::NullableType>(inputElement);
            const bool taggedCells = mlir::isa<storage::AnyType>(inputElement);
            if (!inputNullable && !taggedCells) {
                throw IRException("db.group_aggregate sum/min/max/avg/stdev/first requires a property value column");
            }

            const mlir::Type inputValue = taggedCells ? inputElement : inputNullable.getValueType();
            const bool keepsTheValue = kind == storage::GroupAggregateKind::First;
            const mlir::Type resultElement = keepsTheValue ? inputValue
                                                           : aggregateResultElementType(builder, groupKindToAggregateKind(kind), inputValue);
            return nl::ChunkType::get(context, nullableElementType(resultElement));
        }
        break;
    }

    throw IRException("Unhandled group aggregate kind");
}

}

DBLowering::DBLowering(mlir::MLIRContext* context,
                       const GraphView* view,
                       const ProcedureManager* procedures)
    : _builder(context),
    _view(view),
    _procedures(procedures)
{
}

DBLowering::~DBLowering() {
}

mlir::func::FuncOp DBLowering::lower(mlir::func::FuncOp dbFunction, mlir::ModuleOp module) {
    // Check that we didn't failed MLIR verifier
    if (mlir::failed(mlir::verify(dbFunction))) {
        throw IRException("db function failed MLIR verification");
    }

    mlir::Region& dbBody = dbFunction->getRegion(0);
    if (!dbBody.hasOneBlock()) {
        throw IRException("DBLowering expects a db function with a single block");
    }

    mlir::MLIRContext* context = _builder.getContext();
    const mlir::Location loc = _builder.getUnknownLoc();

    // Create nl target function
    llvm::SmallVector<mlir::Type> argumentTypes;
    for (const mlir::BlockArgument argument : dbFunction.getArguments()) {
        argumentTypes.push_back(nl::ChunkType::get(context, columnType(argument)));
    }

    _builder.setInsertionPointToEnd(module.getBody());
    const auto functionType = mlir::FunctionType::get(context, argumentTypes, {});
    auto nlFunction = _builder.create<mlir::func::FuncOp>(loc, dbFunction.getSymName(), functionType);
    if (const mlir::ArrayAttr argumentAttributes = dbFunction.getAllArgAttrs()) {
        nlFunction.setAllArgAttrs(argumentAttributes);
    }
    _entryBlock = nlFunction.addEntryBlock();

    // Create the ReturnOp of the target function right away
    _builder.setInsertionPointToStart(_entryBlock);
    _builder.create<mlir::func::ReturnOp>(loc);

    // Lower each operation of the db function. Top-level scans root their loop
    // in the entry block; a cross product retargets the root per factor.
    _valueMap.clear();
    _propertyTypes.clear();
    _edgeTypeSets.clear();
    _lastHoisted = nullptr;
    _constantColumns.clear();
    _rootBlock = _entryBlock;
    _innermostLoopBody = nullptr;
    _innermostRegionBody = nullptr;
    _innermostCardinality = mlir::Value();
    _limitHandles.clear();
    _loopLimits.clear();
    _optionalMatchLimits.clear();
    _sortTopK.clear();
    _fusedLimits.clear();
    _loweredAhead.clear();

    for (const auto [dbArgument, nlArgument] : llvm::zip_equal(dbFunction.getArguments(), nlFunction.getArguments())) {
        _valueMap[dbArgument] = nlArgument;
    }

    lowerBoundValuesAhead(dbFunction);

    // Find ORDER BY ... LIMIT k: a db.limit capping a db.sort's result fuses into
    // a bounded top-K, so the limit gets no streaming handle and the sort carries
    // the bound. Detect before the limit pre-scan so the fused ones are skipped.
    detectTopKFusion(dbFunction);

    hoistLimitHandles(dbFunction.getBody(), _entryBlock, nullptr);

    for (mlir::Operation& operation : dbBody.front()) {
        lowerOperation(operation);
    }

    // Peephole: a terminal LIMIT lowers to an nl.limit_truncate whose only
    // consumer is the nl.output right after it. Fold that pair into a single
    // limit-bearing nl.output, which emits the budgeted prefix off the handle
    // instead of copying it - the copy-free path for a LIMIT that feeds the sink.
    foldTruncatesIntoOutputs(nlFunction);

    // The skip sibling: a terminal SKIP folds its nl.skip_truncate into a
    // skip-bearing nl.output that emits the surviving suffix in place (at an
    // offset) instead of copying it to the front - the copy-free post-skip tail.
    foldSkipTruncatesIntoOutputs(nlFunction);

    foldBroadcastsIntoOutputs(nlFunction);

    // Run MLIR verifier on the nlFunction
    if (mlir::failed(mlir::verify(nlFunction))) {
        throw IRException("DBLowering produced an invalid nl function");
    }

    return nlFunction;
}

void DBLowering::lowerOperation(mlir::Operation& operation) {
    if (_loweredAhead.contains(&operation)) {
        return;
    }

    for (const LaidOutOperand& laidOut : llvm::reverse(_laidOutOperands)) {
        _valueMap[laidOut._operand] = laidOut._constantChunk;
    }

    if (computesOnceOverConstants(operation)) {
        lowerOnceOverConstants(operation);
    } else if (raisesOverConstants(operation)) {
        lowerOverReachingRows(operation);
    } else if (readsConstantsAsRows(operation)) {
        lowerWithConstantsLaidOut(operation, rowsReadBy(operation), [this](mlir::Value chunk) {
            return isConstantReadAsRows(chunk);
        });
    } else {
        lowerOperationByKind(operation);
    }

    for (const LaidOutOperand& laidOut : _laidOutOperands) {
        _valueMap[laidOut._operand] = laidOut._laidOutChunk;
    }
}

void DBLowering::eraseOp(mlir::Operation* operation) {
    forgetErasedValues(operation, _constantColumns);
    forgetValuesDefinedWithin(operation, _valueMap);
    forgetValuesDefinedWithin(operation, _limitHandles);

    operation->erase();
}

void DBLowering::lowerOperationByKind(mlir::Operation& operation) {
    if (mlir::db::ScanNodes scanNodes = mlir::dyn_cast<mlir::db::ScanNodes>(operation)) {
        lowerScanNodes(scanNodes);
    } else if (mlir::db::ScanNodesByLabel scanNodesByLabel = mlir::dyn_cast<mlir::db::ScanNodesByLabel>(operation)) {
        lowerScanNodesByLabel(scanNodesByLabel);
    } else if (mlir::db::ConstScanNodes constScanNodes = mlir::dyn_cast<mlir::db::ConstScanNodes>(operation)) {
        lowerConstScanNodes(constScanNodes);
    } else if (mlir::db::ScanNodesByPropertyValue scanNodesByPropertyValue = mlir::dyn_cast<mlir::db::ScanNodesByPropertyValue>(operation)) {
        lowerScanNodesByPropertyValue(scanNodesByPropertyValue);
    } else if (mlir::db::UnwindConst unwindConst = mlir::dyn_cast<mlir::db::UnwindConst>(operation)) {
        lowerUnwindConst(unwindConst);
    } else if (mlir::db::LoadCSV loadCSV = mlir::dyn_cast<mlir::db::LoadCSV>(operation)) {
        lowerLoadCSV(loadCSV);
    } else if (mlir::db::VectorSearch vectorSearch = mlir::dyn_cast<mlir::db::VectorSearch>(operation)) {
        lowerVectorSearch(vectorSearch);
    } else if (mlir::db::Unwind unwind = mlir::dyn_cast<mlir::db::Unwind>(operation)) {
        lowerUnwind(unwind);
    } else if (mlir::db::UnwindRange unwindRange = mlir::dyn_cast<mlir::db::UnwindRange>(operation)) {
        lowerUnwindRange(unwindRange);
    } else if (mlir::db::UnwindComputed unwindComputed = mlir::dyn_cast<mlir::db::UnwindComputed>(operation)) {
        lowerUnwindComputed(unwindComputed);
    } else if (mlir::db::MakeList makeList = mlir::dyn_cast<mlir::db::MakeList>(operation)) {
        lowerMakeList(makeList);
    } else if (mlir::db::MakeMap makeMap = mlir::dyn_cast<mlir::db::MakeMap>(operation)) {
        lowerMakeMap(makeMap);
    } else if (mlir::db::MergeMaps merge = mlir::dyn_cast<mlir::db::MergeMaps>(operation)) {
        lowerMergeMaps(merge);
    } else if (mlir::db::StaticMapKey mapKey = mlir::dyn_cast<mlir::db::StaticMapKey>(operation)) {
        lowerStaticMapKey(mapKey);
    } else if (mlir::db::DynamicMapKey dynamicMapKey = mlir::dyn_cast<mlir::db::DynamicMapKey>(operation)) {
        lowerDynamicMapKey(dynamicMapKey);
    } else if (mlir::db::ContainerIndex containerIndex = mlir::dyn_cast<mlir::db::ContainerIndex>(operation)) {
        lowerContainerIndex(containerIndex);
    } else if (mlir::db::Range range = mlir::dyn_cast<mlir::db::Range>(operation)) {
        lowerRange(range);
    } else if (mlir::db::Substring substring = mlir::dyn_cast<mlir::db::Substring>(operation)) {
        lowerSubstring(substring);
    } else if (mlir::db::Split split = mlir::dyn_cast<mlir::db::Split>(operation)) {
        lowerSplit(split);
    } else if (mlir::db::Left left = mlir::dyn_cast<mlir::db::Left>(operation)) {
        lowerStringEnd<nl::Left>(left);
    } else if (mlir::db::Right right = mlir::dyn_cast<mlir::db::Right>(operation)) {
        lowerStringEnd<nl::Right>(right);
    } else if (mlir::db::Replace replace = mlir::dyn_cast<mlir::db::Replace>(operation)) {
        lowerReplace(replace);
    } else if (mlir::isa<mlir::db::PointDistance>(operation)) {
        lowerPointFunction<nl::PointDistance>(&operation, _builder.getF64Type());
    } else if (mlir::isa<mlir::db::PointWithinBBox>(operation)) {
        lowerPointFunction<nl::PointWithinBBox>(&operation, _builder.getI1Type());
    } else if (mlir::db::ListSlice listSlice = mlir::dyn_cast<mlir::db::ListSlice>(operation)) {
        lowerListSlice(listSlice);
    } else if (mlir::db::SortMulti sortMulti = mlir::dyn_cast<mlir::db::SortMulti>(operation)) {
        lowerSortMulti(sortMulti);
    } else if (mlir::isa<mlir::db::PercentileCont, mlir::db::PercentileDisc>(operation)) {
        lowerPercentile(&operation);
    } else if (mlir::db::ListComprehension listComprehension = mlir::dyn_cast<mlir::db::ListComprehension>(operation)) {
        lowerListComprehension(listComprehension);
    } else if (mlir::db::ListPredicate listPredicate = mlir::dyn_cast<mlir::db::ListPredicate>(operation)) {
        lowerListPredicate(listPredicate);
    } else if (mlir::db::Reduce reduce = mlir::dyn_cast<mlir::db::Reduce>(operation)) {
        lowerReduce(reduce);
    } else if (mlir::db::AllReduce allReduce = mlir::dyn_cast<mlir::db::AllReduce>(operation)) {
        lowerAllReduce(allReduce);
    } else if (mlir::db::PatternComprehension patternComprehension = mlir::dyn_cast<mlir::db::PatternComprehension>(operation)) {
        lowerPatternComprehension(patternComprehension);
    } else if (mlir::db::ScanEdges scanEdges = mlir::dyn_cast<mlir::db::ScanEdges>(operation)) {
        lowerScanEdges(scanEdges);
    } else if (mlir::db::ConstScanEdges constScanEdges = mlir::dyn_cast<mlir::db::ConstScanEdges>(operation)) {
        lowerConstScanEdges(constScanEdges);
    } else if (mlir::db::ScanEdgesByType scanEdgesByType = mlir::dyn_cast<mlir::db::ScanEdgesByType>(operation)) {
        lowerScanEdgesByType(scanEdgesByType);
    } else if (mlir::db::ScanOutEdgesByLabelSrc scanOutEdgesByLabelSrc = mlir::dyn_cast<mlir::db::ScanOutEdgesByLabelSrc>(operation)) {
        lowerScanOutEdgesByLabelSrc(scanOutEdgesByLabelSrc);
    } else if (mlir::db::ScanInEdgesByLabelTgt scanInEdgesByLabelTgt = mlir::dyn_cast<mlir::db::ScanInEdgesByLabelTgt>(operation)) {
        lowerScanInEdgesByLabelTgt(scanInEdgesByLabelTgt);
    } else if (mlir::db::ScanOutEdgesByLabelTgt scanOutEdgesByLabelTgt = mlir::dyn_cast<mlir::db::ScanOutEdgesByLabelTgt>(operation)) {
        lowerScanOutEdgesByLabelTgt(scanOutEdgesByLabelTgt);
    } else if (mlir::db::ScanInEdgesByLabelSrc scanInEdgesByLabelSrc = mlir::dyn_cast<mlir::db::ScanInEdgesByLabelSrc>(operation)) {
        lowerScanInEdgesByLabelSrc(scanInEdgesByLabelSrc);
    } else if (mlir::db::ScanOutEdgesByLabelSrcTgt scanOutEdgesByLabelSrcTgt = mlir::dyn_cast<mlir::db::ScanOutEdgesByLabelSrcTgt>(operation)) {
        lowerScanOutEdgesByLabelSrcTgt(scanOutEdgesByLabelSrcTgt);
    } else if (mlir::db::ScanInEdgesByLabelSrcTgt scanInEdgesByLabelSrcTgt = mlir::dyn_cast<mlir::db::ScanInEdgesByLabelSrcTgt>(operation)) {
        lowerScanInEdgesByLabelSrcTgt(scanInEdgesByLabelSrcTgt);
    } else if (mlir::db::GetOutEdges getOutEdges = mlir::dyn_cast<mlir::db::GetOutEdges>(operation)) {
        lowerGetOutEdges(getOutEdges);
    } else if (mlir::db::GetInEdges getInEdges = mlir::dyn_cast<mlir::db::GetInEdges>(operation)) {
        lowerGetInEdges(getInEdges);
    } else if (mlir::db::GetEdges getEdges = mlir::dyn_cast<mlir::db::GetEdges>(operation)) {
        lowerGetEdges(getEdges);
    } else if (mlir::db::GetOutEdgesByType getOutEdgesByType = mlir::dyn_cast<mlir::db::GetOutEdgesByType>(operation)) {
        lowerGetOutEdgesByType(getOutEdgesByType);
    } else if (mlir::db::GetInEdgesByType getInEdgesByType = mlir::dyn_cast<mlir::db::GetInEdgesByType>(operation)) {
        lowerGetInEdgesByType(getInEdgesByType);
    } else if (mlir::db::GetOutEdgesByLabel getOutEdgesByLabel = mlir::dyn_cast<mlir::db::GetOutEdgesByLabel>(operation)) {
        lowerGetOutEdgesByLabel(getOutEdgesByLabel);
    } else if (mlir::db::GetInEdgesByLabel getInEdgesByLabel = mlir::dyn_cast<mlir::db::GetInEdgesByLabel>(operation)) {
        lowerGetInEdgesByLabel(getInEdgesByLabel);
    } else if (mlir::db::GetOutEdgesByTypeAndLabel getOutEdgesByTypeAndLabel = mlir::dyn_cast<mlir::db::GetOutEdgesByTypeAndLabel>(operation)) {
        lowerGetOutEdgesByTypeAndLabel(getOutEdgesByTypeAndLabel);
    } else if (mlir::db::GetInEdgesByTypeAndLabel getInEdgesByTypeAndLabel = mlir::dyn_cast<mlir::db::GetInEdgesByTypeAndLabel>(operation)) {
        lowerGetInEdgesByTypeAndLabel(getInEdgesByTypeAndLabel);
    } else if (mlir::db::ExplorePaths explorePaths = mlir::dyn_cast<mlir::db::ExplorePaths>(operation)) {
        lowerExplorePaths(explorePaths);
    } else if (mlir::db::ExpandPath expandPath = mlir::dyn_cast<mlir::db::ExpandPath>(operation)) {
        lowerExpandPath(expandPath);
    } else if (mlir::db::PathLength pathLength = mlir::dyn_cast<mlir::db::PathLength>(operation)) {
        lowerPathLength(pathLength);
    } else if (mlir::db::PathElements pathElements = mlir::dyn_cast<mlir::db::PathElements>(operation)) {
        lowerPathElements(pathElements);
    } else if (mlir::db::MakePath makePath = mlir::dyn_cast<mlir::db::MakePath>(operation)) {
        lowerMakePath(makePath);
    } else if (mlir::db::GetNodeProperties getNodeProperties = mlir::dyn_cast<mlir::db::GetNodeProperties>(operation)) {
        lowerGetNodeProperties(getNodeProperties);
    } else if (mlir::db::GetEdgeProperties getEdgeProperties = mlir::dyn_cast<mlir::db::GetEdgeProperties>(operation)) {
        lowerGetEdgeProperties(getEdgeProperties);
    } else if (mlir::db::GetNodeLabelSet getNodeLabelSet = mlir::dyn_cast<mlir::db::GetNodeLabelSet>(operation)) {
        lowerGetNodeLabelSet(getNodeLabelSet);
    } else if (mlir::db::GetEdgeTypes getEdgeTypes = mlir::dyn_cast<mlir::db::GetEdgeTypes>(operation)) {
        lowerGetEdgeTypes(getEdgeTypes);
    } else if (mlir::db::CheckLabelConstraint checkLabelConstraint = mlir::dyn_cast<mlir::db::CheckLabelConstraint>(operation)) {
        lowerCheckLabelConstraint(checkLabelConstraint);
    } else if (mlir::db::CheckEdgeTypeConstraint checkEdgeTypeConstraint = mlir::dyn_cast<mlir::db::CheckEdgeTypeConstraint>(operation)) {
        lowerCheckEdgeTypeConstraint(checkEdgeTypeConstraint);
    } else if (mlir::db::CreateNode createNode = mlir::dyn_cast<mlir::db::CreateNode>(operation)) {
        lowerCreateNode(createNode);
    } else if (mlir::db::CreateEdge createEdge = mlir::dyn_cast<mlir::db::CreateEdge>(operation)) {
        lowerCreateEdge(createEdge);
    } else if (mlir::db::Merge merge = mlir::dyn_cast<mlir::db::Merge>(operation)) {
        lowerMerge(merge);
    } else if (mlir::db::SetNodeProperty setNodeProperty = mlir::dyn_cast<mlir::db::SetNodeProperty>(operation)) {
        lowerSetNodeProperty(setNodeProperty);
    } else if (mlir::db::SetEdgeProperty setEdgeProperty = mlir::dyn_cast<mlir::db::SetEdgeProperty>(operation)) {
        lowerSetEdgeProperty(setEdgeProperty);
    } else if (mlir::db::SetNodeProperties setNodeProperties = mlir::dyn_cast<mlir::db::SetNodeProperties>(operation)) {
        lowerSetProperties<nl::SetNodeProperties>(setNodeProperties, setNodeProperties.getInputNodes());
    } else if (mlir::db::SetEdgeProperties setEdgeProperties = mlir::dyn_cast<mlir::db::SetEdgeProperties>(operation)) {
        lowerSetProperties<nl::SetEdgeProperties>(setEdgeProperties, setEdgeProperties.getInputEdges());
    } else if (mlir::db::DeleteNode deleteNode = mlir::dyn_cast<mlir::db::DeleteNode>(operation)) {
        lowerDeleteNode(deleteNode);
    } else if (mlir::db::DeleteEdge deleteEdge = mlir::dyn_cast<mlir::db::DeleteEdge>(operation)) {
        lowerDeleteEdge(deleteEdge);
    } else if (mlir::db::CrossProduct crossProduct = mlir::dyn_cast<mlir::db::CrossProduct>(operation)) {
        lowerCrossProduct(crossProduct);
    } else if (mlir::db::HashJoin hashJoin = mlir::dyn_cast<mlir::db::HashJoin>(operation)) {
        lowerHashJoin(hashJoin);
    } else if (mlir::db::Union unionOp = mlir::dyn_cast<mlir::db::Union>(operation)) {
        lowerUnion(unionOp);
    } else if (mlir::db::Conditional conditional = mlir::dyn_cast<mlir::db::Conditional>(operation)) {
        lowerConditional(conditional);
    } else if (mlir::db::DistinctSet distinctSet = mlir::dyn_cast<mlir::db::DistinctSet>(operation)) {
        lowerDistinctSet(distinctSet);
    } else if (mlir::db::Materialize materialize = mlir::dyn_cast<mlir::db::Materialize>(operation)) {
        lowerMaterialize(materialize);
    } else if (mlir::db::ScanMaterialized scan = mlir::dyn_cast<mlir::db::ScanMaterialized>(operation)) {
        lowerScanMaterialized(scan);
    } else if (mlir::db::OptionalMatch optionalMatch = mlir::dyn_cast<mlir::db::OptionalMatch>(operation)) {
        lowerOptionalMatch(optionalMatch);
    } else if (mlir::db::CallSubquery callSubquery = mlir::dyn_cast<mlir::db::CallSubquery>(operation)) {
        lowerCallSubquery(callSubquery);
    } else if (mlir::db::ExistsSubquery existsSubquery = mlir::dyn_cast<mlir::db::ExistsSubquery>(operation)) {
        lowerSubqueryExpression<mlir::db::ExistsSubquery, mlir::db::ExistsYield>(existsSubquery);
    } else if (mlir::db::CountSubquery countSubquery = mlir::dyn_cast<mlir::db::CountSubquery>(operation)) {
        lowerSubqueryExpression<mlir::db::CountSubquery, mlir::db::CountSubqueryYield>(countSubquery);
    } else if (mlir::db::CollectSubquery collectSubquery = mlir::dyn_cast<mlir::db::CollectSubquery>(operation)) {
        lowerSubqueryExpression<mlir::db::CollectSubquery, mlir::db::CollectSubqueryYield>(collectSubquery);
    } else if (mlir::db::Limit limit = mlir::dyn_cast<mlir::db::Limit>(operation)) {
        lowerLimit(limit);
    } else if (mlir::db::Skip skip = mlir::dyn_cast<mlir::db::Skip>(operation)) {
        lowerSkip(skip);
    } else if (mlir::db::Sort sort = mlir::dyn_cast<mlir::db::Sort>(operation)) {
        lowerSort(sort);
    } else if (mlir::db::RemoveDuplicates distinct = mlir::dyn_cast<mlir::db::RemoveDuplicates>(operation)) {
        lowerRemoveDuplicates(distinct);
    } else if (mlir::db::Count count = mlir::dyn_cast<mlir::db::Count>(operation)) {
        lowerCount(count);
    } else if (mlir::db::CountScanRows countScanRows = mlir::dyn_cast<mlir::db::CountScanRows>(operation)) {
        lowerCountScanRows(countScanRows);
    } else if (mlir::db::Sum sum = mlir::dyn_cast<mlir::db::Sum>(operation)) {
        lowerAggregate(sum.getInput(), sum.getResult(), storage::AggregateKind::Sum, sum.getDistinct());
    } else if (mlir::db::Min min = mlir::dyn_cast<mlir::db::Min>(operation)) {
        lowerAggregate(min.getInput(), min.getResult(), storage::AggregateKind::Min, min.getDistinct());
    } else if (mlir::db::Max max = mlir::dyn_cast<mlir::db::Max>(operation)) {
        lowerAggregate(max.getInput(), max.getResult(), storage::AggregateKind::Max, max.getDistinct());
    } else if (mlir::db::Avg avg = mlir::dyn_cast<mlir::db::Avg>(operation)) {
        lowerAggregate(avg.getInput(), avg.getResult(), storage::AggregateKind::Avg, avg.getDistinct());
    } else if (mlir::db::StDev stDev = mlir::dyn_cast<mlir::db::StDev>(operation)) {
        lowerAggregate(stDev.getInput(), stDev.getResult(), storage::AggregateKind::StDev, stDev.getDistinct());
    } else if (mlir::db::StDevP stDevP = mlir::dyn_cast<mlir::db::StDevP>(operation)) {
        lowerAggregate(stDevP.getInput(), stDevP.getResult(), storage::AggregateKind::StDevP, stDevP.getDistinct());
    } else if (mlir::db::ConstantOp constant = mlir::dyn_cast<mlir::db::ConstantOp>(operation)) {
        lowerConstant(constant);
    } else if (mlir::db::CurrentDateTime currentDateTime = mlir::dyn_cast<mlir::db::CurrentDateTime>(operation)) {
        lowerCurrentDateTime(currentDateTime);
    } else if (mlir::db::BroadcastConstant broadcast = mlir::dyn_cast<mlir::db::BroadcastConstant>(operation)) {
        lowerBroadcastConstant(broadcast);
    } else if (mlir::db::RowConstant rowConstant = mlir::dyn_cast<mlir::db::RowConstant>(operation)) {
        lowerRowConstant(rowConstant);
    } else if (mlir::db::ToNullable toNullable = mlir::dyn_cast<mlir::db::ToNullable>(operation)) {
        lowerToNullable(toNullable);
    } else if (mlir::db::CheckedCast checkedCast = mlir::dyn_cast<mlir::db::CheckedCast>(operation)) {
        lowerCheckedCast(checkedCast);
    } else if (mlir::isa<mlir::db::AddOp>(operation)) {
        lowerBinaryOp<nl::Add>(operation, BinaryResultKind::Numeric);
    } else if (mlir::isa<mlir::db::ConcatOp>(operation)) {
        lowerBinaryOp<nl::Concat>(operation, BinaryResultKind::Concat);
    } else if (mlir::isa<mlir::db::ListIndex>(operation)) {
        lowerBinaryOp<nl::ListIndex>(operation, BinaryResultKind::Index);
    } else if (mlir::isa<mlir::db::SubOp>(operation)) {
        lowerBinaryOp<nl::Sub>(operation, BinaryResultKind::Numeric);
    } else if (mlir::isa<mlir::db::MulOp>(operation)) {
        lowerBinaryOp<nl::Mul>(operation, BinaryResultKind::Numeric);
    } else if (mlir::isa<mlir::db::DivOp>(operation)) {
        lowerBinaryOp<nl::Div>(operation, BinaryResultKind::Numeric);
    } else if (mlir::isa<mlir::db::ModOp>(operation)) {
        lowerBinaryOp<nl::Mod>(operation, BinaryResultKind::Numeric);
    } else if (mlir::isa<mlir::db::PowOp>(operation)) {
        lowerBinaryOp<nl::Pow>(operation, BinaryResultKind::Double);
    } else if (mlir::isa<mlir::db::EqOp>(operation)) {
        lowerBinaryOp<nl::Eq>(operation, BinaryResultKind::Comparison);
    } else if (mlir::isa<mlir::db::NeqOp>(operation)) {
        lowerBinaryOp<nl::Neq>(operation, BinaryResultKind::Comparison);
    } else if (mlir::isa<mlir::db::GtOp>(operation)) {
        lowerBinaryOp<nl::Gt>(operation, BinaryResultKind::Comparison);
    } else if (mlir::isa<mlir::db::LtOp>(operation)) {
        lowerBinaryOp<nl::Lt>(operation, BinaryResultKind::Comparison);
    } else if (mlir::isa<mlir::db::GteOp>(operation)) {
        lowerBinaryOp<nl::Gte>(operation, BinaryResultKind::Comparison);
    } else if (mlir::isa<mlir::db::LteOp>(operation)) {
        lowerBinaryOp<nl::Lte>(operation, BinaryResultKind::Comparison);
    } else if (mlir::isa<mlir::db::StartsWithOp>(operation)) {
        lowerBinaryOp<nl::StartsWith>(operation, BinaryResultKind::Boolean);
    } else if (mlir::isa<mlir::db::EndsWithOp>(operation)) {
        lowerBinaryOp<nl::EndsWith>(operation, BinaryResultKind::Boolean);
    } else if (mlir::isa<mlir::db::ContainsOp>(operation)) {
        lowerBinaryOp<nl::Contains>(operation, BinaryResultKind::Boolean);
    } else if (mlir::isa<mlir::db::InOp>(operation)) {
        lowerBinaryOp<nl::In>(operation, BinaryResultKind::Membership);
    } else if (mlir::isa<mlir::db::IndexOf>(operation)) {
        lowerBinaryOp<nl::IndexOf>(operation, BinaryResultKind::Position);
    } else if (mlir::isa<mlir::db::AndOp>(operation)) {
        lowerBinaryOp<nl::And>(operation, BinaryResultKind::Boolean);
    } else if (mlir::isa<mlir::db::OrOp>(operation)) {
        lowerBinaryOp<nl::Or>(operation, BinaryResultKind::Boolean);
    } else if (mlir::isa<mlir::db::XorOp>(operation)) {
        lowerBinaryOp<nl::Xor>(operation, BinaryResultKind::Boolean);
    } else if (mlir::db::NotOp notOp = mlir::dyn_cast<mlir::db::NotOp>(operation)) {
        lowerNot(notOp);
    } else if (mlir::db::IsNullOp isNull = mlir::dyn_cast<mlir::db::IsNullOp>(operation)) {
        lowerNullTest<nl::IsNull>(isNull.getOperand(), isNull.getResult());
    } else if (mlir::db::IsNotNullOp isNotNull = mlir::dyn_cast<mlir::db::IsNotNullOp>(operation)) {
        lowerNullTest<nl::IsNotNull>(isNotNull.getOperand(), isNotNull.getResult());
    } else if (mlir::db::Case caseOp = mlir::dyn_cast<mlir::db::Case>(operation)) {
        lowerCase(caseOp);
    } else if (mlir::db::LazyCase lazyCase = mlir::dyn_cast<mlir::db::LazyCase>(operation)) {
        lowerLazyCase(lazyCase);
    } else if (mlir::db::Coalesce coalesce = mlir::dyn_cast<mlir::db::Coalesce>(operation)) {
        lowerCoalesce(coalesce);
    } else if (mlir::db::FilterOp filter = mlir::dyn_cast<mlir::db::FilterOp>(operation)) {
        lowerFilter(filter);
    } else if (mlir::db::FetchNodes fetch = mlir::dyn_cast<mlir::db::FetchNodes>(operation)) {
        lowerFetchNodes(fetch);
    } else if (mlir::db::FetchEdges fetch = mlir::dyn_cast<mlir::db::FetchEdges>(operation)) {
        lowerFetchEdges(fetch);
    } else if (mlir::db::EntityIDOf entityIDOf = mlir::dyn_cast<mlir::db::EntityIDOf>(operation)) {
        lowerEntityIDOf(entityIDOf);
    } else if (mlir::db::GroupAggregate groupAggregate = mlir::dyn_cast<mlir::db::GroupAggregate>(operation)) {
        lowerGroupAggregate(groupAggregate);
    } else if (mlir::db::Collect collect = mlir::dyn_cast<mlir::db::Collect>(operation)) {
        lowerCollect(collect);
    } else if (mlir::db::UnwindCollect unwindCollect = mlir::dyn_cast<mlir::db::UnwindCollect>(operation)) {
        lowerUnwindCollect(unwindCollect);
    } else if (mlir::db::ShortestPath shortestPath = mlir::dyn_cast<mlir::db::ShortestPath>(operation)) {
        lowerShortestPath(shortestPath);
    } else if (mlir::db::CallProcedure call = mlir::dyn_cast<mlir::db::CallProcedure>(operation)) {
        lowerCallProcedure(call);
    } else if (mlir::db::Output output = mlir::dyn_cast<mlir::db::Output>(operation)) {
        lowerOutput(output);
    } else if (mlir::isa<mlir::db::Head, mlir::db::Last>(operation)) {
        lowerHeadOrLast(operation);
    } else if (lookupUnaryFunctionLowering(operation)) {
        lowerUnaryFunction(&operation);
    } else if (lookupBinaryFunctionLowering(operation)) {
        lowerBinaryFunction(&operation);
    } else if (mlir::isa<mlir::func::ReturnOp>(operation)) {
        // We already added a ReturnOp to the nl function
    } else if (!lowerSystemCommand(operation)) {
        throw IRException("DBLowering cannot lower operation '"
                          + operation.getName().getStringRef().str() + "'");
    }
}

bool DBLowering::lowerSystemCommand(mlir::Operation& operation) {
    const auto siblingIt = systemCommandSiblings.find(operation.getName().getStringRef());
    if (siblingIt == systemCommandSiblings.end()) {
        return false;
    }

    // A system command reads no column, so nothing constrains where it sits, and it
    // opens no loop: it materializes its whole (small) result table in one step at
    // function scope, the way nl.count_result materializes its single row.
    setInsertionInto(_entryBlock);

    mlir::MLIRContext* const context = _builder.getContext();

    mlir::OperationState state(_builder.getUnknownLoc(), siblingIt->second);
    state.addAttributes(operation.getAttrs());

    for (const mlir::Value result : operation.getResults()) {
        const mlir::db::ColumnType columnType = mlir::cast<mlir::db::ColumnType>(result.getType());
        state.addTypes(nl::ChunkType::get(context, columnType.getType()));
    }

    mlir::Operation* const nlOperation = _builder.create(state);

    for (size_t resultIndex = 0; resultIndex < operation.getNumResults(); resultIndex++) {
        _valueMap[operation.getResult(resultIndex)] = nlOperation->getResult(resultIndex);
    }

    return true;
}

void DBLowering::lowerPatternComprehension(mlir::db::PatternComprehension comprehension) {
    const mlir::Location loc = _builder.getUnknownLoc();
    mlir::MLIRContext* const context = _builder.getContext();

    llvm::SmallVector<mlir::Value, 4> inputChunks;
    for (const mlir::Value column : comprehension.getInputColumns()) {
        inputChunks.push_back(mapValue(column));
    }

    // The step the accumulator covers is the one binding the columns the comprehension is
    // read beside, so it is emptied once per chunk of them - and the pattern's nest and
    // the build both go there, the build after the nest.
    mlir::Block* const stepBlock = deepestOwnerBlock(inputChunks, _rootBlock);

    // Any chunk of the step counts its rows; a comprehension read where nothing is in
    // flight has none, and covers the single empty row instead.
    const mlir::Value cardinality = inputChunks.empty() ? mlir::Value {} : cardinalityDriver(inputChunks);

    setInsertionInto(stepBlock);
    nl::RowListsBuffer buffer = _builder.create<nl::RowListsBuffer>(loc, cardinality);
    const mlir::Value state = buffer.getState();

    // The pattern reads this step's rows through its block arguments, and the row tag
    // through the trailing one; a pattern with nothing to join onto has neither.
    mlir::Block& patternBlock = comprehension.getPattern().front();
    for (size_t inputIndex = 0; inputIndex < inputChunks.size(); inputIndex++) {
        _valueMap[patternBlock.getArgument(static_cast<unsigned>(inputIndex))] = inputChunks[inputIndex];
    }

    if (!inputChunks.empty()) {
        _valueMap[patternBlock.getArgument(static_cast<unsigned>(inputChunks.size()))] = buffer.getTag();
    }

    // A dataflow of its own, rooted where the accumulator sits so its loops nest inside
    // this step; the caller's root and innermost loop are restored once it is lowered.
    mlir::Block* const previousRoot = _rootBlock;
    mlir::Block* const previousInnermostLoopBody = _innermostLoopBody;
    const mlir::Value previousInnermostCardinality = _innermostCardinality;
    _rootBlock = stepBlock;
    _innermostLoopBody = nullptr;
    _innermostCardinality = mlir::Value();

    mlir::Value rowTagChunk;
    mlir::Value valueChunk;

    for (mlir::Operation& operation : patternBlock) {
        mlir::db::ComprehensionYield yield = mlir::dyn_cast<mlir::db::ComprehensionYield>(operation);
        if (!yield) {
            lowerOperation(operation);
            continue;
        }

        if (yield.getRowTags()) {
            rowTagChunk = mapValue(yield.getRowTags());
        }

        valueChunk = mapValue(yield.getValue());
    }

    // PatternComprehension::verify guarantees the yield, so an empty one here means
    // unverified IR - the defensive backstop lowerOptionalMatch keeps too.
    if (!valueChunk) {
        throw IRException("db.pattern_comprehension yields no value");
    }

    // The matches the value is read against are the ones the tag counts, since a WHERE has
    // cut both together
    valueChunk = rowListsValueChunk(valueChunk, rowTagChunk ? rowTagChunk : rowsInFlight());

    llvm::SmallVector<mlir::Value, 2> stagedChunks {valueChunk};
    if (rowTagChunk) {
        stagedChunks.push_back(rowTagChunk);
    }

    setInsertionInto(deepestOwnerBlock(stagedChunks, stepBlock));
    _builder.create<nl::RowListsCollect>(loc, state, rowTagChunk, mlir::Value(), valueChunk);

    _rootBlock = previousRoot;
    _innermostLoopBody = previousInnermostLoopBody;
    _innermostCardinality = previousInnermostCardinality;

    // Every row gets a list, the empty one where the pattern matched nothing, so the
    // lists are a container chunk rather than the nullable one a list comprehension builds
    const mlir::Type listType = storage::ListType::get(context, listedElementType(context, valueChunk));
    const nl::ChunkType resultType = nl::ChunkType::get(context, listType);

    setInsertionInto(stepBlock);
    nl::RowListsResult lists = _builder.create<nl::RowListsResult>(loc, resultType, state, cardinality);

    _valueMap[comprehension.getResult()] = lists.getResult();
}

mlir::Value DBLowering::rowListsValueChunk(mlir::Value valueChunk, mlir::Value rows) {
    // A value computed from constants alone holds one cell standing for every row, so it is
    // laid out over the rows it is read against
    const mlir::Value aligned = rowAlignedChunk(valueChunk, rows);

    return listItemChunk(aligned);
}

void DBLowering::lowerSubstring(mlir::db::Substring substring) {
    const mlir::Type stringChunkType = mapValue(substring.getString()).getType();
    const bool stringMayBeNull = mayHoldANull(stringChunkType);

    llvm::SmallVector<mlir::Value, 3> columns {substring.getString(), substring.getStart()};

    const mlir::Value length = substring.getLength();
    if (length) {
        columns.push_back(length);
    }

    llvm::SmallVector<mlir::Value, 3> arguments;
    containerCellChunks(columns, arguments);

    mlir::MLIRContext* const context = _builder.getContext();
    const mlir::Type stringType = storage::StringType::get(context);
    const mlir::Type resultElement = stringMayBeNull ? storage::NullableType::get(context, stringType) : stringType;
    const nl::ChunkType resultType = nl::ChunkType::get(context, resultElement);

    setInsertionForNaryOp(arguments);

    nl::Substring substrings = _builder.create<nl::Substring>(_builder.getUnknownLoc(),
                                                              resultType,
                                                              arguments[0],
                                                              arguments[1],
                                                              length ? arguments[2] : mlir::Value {});

    _valueMap[substring.getResult()] = substrings.getResult();
}

void DBLowering::lowerSplit(mlir::db::Split split) {
    const mlir::Type stringChunkType = mapValue(split.getString()).getType();
    const mlir::Type delimiterChunkType = mapValue(split.getDelimiter()).getType();

    const bool stringMayBeNull = mayHoldANull(stringChunkType);

    // A list may hold a null whatever its element type, and a null delimiter splits into null
    const bool delimiterMayBeNull = mayHoldANull(delimiterChunkType) || isListChunk(delimiterChunkType);

    llvm::SmallVector<mlir::Value, 2> arguments;
    containerCellChunks(split->getOperands(), arguments);

    mlir::MLIRContext* const context = _builder.getContext();
    const mlir::Type stringType = storage::StringType::get(context);
    const mlir::Type listType = storage::ListType::get(context, stringType);

    const bool resultMayBeNull = stringMayBeNull || delimiterMayBeNull;
    const mlir::Type resultElement = resultMayBeNull ? storage::NullableType::get(context, listType) : listType;
    const nl::ChunkType resultType = nl::ChunkType::get(context, resultElement);

    setInsertionForNaryOp(arguments);

    nl::Split parts = _builder.create<nl::Split>(_builder.getUnknownLoc(),
                                                 resultType,
                                                 arguments[0],
                                                 arguments[1]);

    _valueMap[split.getResult()] = parts.getResult();
}

template <typename NLStringEndOp, typename StringEndOp>
void DBLowering::lowerStringEnd(StringEndOp stringEnd) {
    const mlir::Type stringChunkType = mapValue(stringEnd.getString()).getType();
    const bool stringMayBeNull = mayHoldANull(stringChunkType);

    llvm::SmallVector<mlir::Value, 2> arguments;
    containerCellChunks(stringEnd->getOperands(), arguments);

    mlir::MLIRContext* const context = _builder.getContext();
    const mlir::Type stringType = storage::StringType::get(context);
    const mlir::Type resultElement = stringMayBeNull ? storage::NullableType::get(context, stringType) : stringType;
    const nl::ChunkType resultType = nl::ChunkType::get(context, resultElement);

    setInsertionForNaryOp(arguments);

    NLStringEndOp ends = _builder.create<NLStringEndOp>(_builder.getUnknownLoc(),
                                                        resultType,
                                                        arguments[0],
                                                        arguments[1]);

    _valueMap[stringEnd.getResult()] = ends.getResult();
}

void DBLowering::lowerReplace(mlir::db::Replace replace) {
    const bool anArgumentMayBeNull = llvm::any_of(replace->getOperands(), [this](mlir::Value operand) {
        return mayHoldANull(mapValue(operand).getType());
    });

    llvm::SmallVector<mlir::Value, 3> arguments;
    containerCellChunks(replace->getOperands(), arguments);

    mlir::MLIRContext* const context = _builder.getContext();
    const mlir::Type stringType = storage::StringType::get(context);
    const mlir::Type resultElement = anArgumentMayBeNull ? storage::NullableType::get(context, stringType) : stringType;
    const nl::ChunkType resultType = nl::ChunkType::get(context, resultElement);

    setInsertionForNaryOp(arguments);

    nl::Replace replaced = _builder.create<nl::Replace>(_builder.getUnknownLoc(),
                                                        resultType,
                                                        arguments[0],
                                                        arguments[1],
                                                        arguments[2]);

    _valueMap[replace.getResult()] = replaced.getResult();
}

// Two points in different CRSs have no distance and no box, so the result is nullable
// whatever its arguments are
template <typename NLOp>
void DBLowering::lowerPointFunction(mlir::Operation* operation, mlir::Type valueType) {
    llvm::SmallVector<mlir::Value, 3> arguments;
    containerCellChunks(operation->getOperands(), arguments);

    mlir::MLIRContext* const context = _builder.getContext();
    const nl::ChunkType resultType = nl::ChunkType::get(context, storage::NullableType::get(context, valueType));

    setInsertionForNaryOp(arguments);

    NLOp lowered = _builder.create<NLOp>(_builder.getUnknownLoc(), mlir::TypeRange {resultType}, arguments);

    _valueMap[operation->getResult(0)] = lowered.getResult();
}

void DBLowering::lowerOptionalMatch(mlir::db::OptionalMatch optionalMatch) {
    const mlir::Location loc = _builder.getUnknownLoc();

    llvm::SmallVector<mlir::Value, 4> inputChunks;
    for (const mlir::Value column : optionalMatch.getInputColumns()) {
        inputChunks.push_back(mapValue(column));
    }

    // The step the accumulator covers is the one binding the columns the pattern joins
    // onto, so it is emptied once per chunk of them - and the pattern's nest and the drain
    // loop both go there, the drain after the nest.
    mlir::Block* const stepBlock = deepestOwnerBlock(inputChunks, _rootBlock);

    setInsertionInto(stepBlock);
    nl::OptionalBuffer buffer = _builder.create<nl::OptionalBuffer>(loc, inputChunks);
    const mlir::Value state = buffer.getState();

    // Every row drained reaches the cut, so once the pattern has matched as many rows as
    // the cut still admits, the drain spends the budget before it reaches a missed row.
    mlir::Operation* const optionalMatchOp = optionalMatch.getOperation();
    mlir::Value patternLimit;
    if (mlir::Operation* const cut = _optionalMatchLimits.lookup(optionalMatchOp)) {
        patternLimit = _builder.create<nl::LimitRemaining>(loc, _limitHandles.lookup(cut)).getState();
        _limitHandles[optionalMatchOp] = patternLimit;
    }

    // The pattern reads this step's rows through its block arguments, and the row tag
    // through the trailing one; a pattern with nothing to join onto has neither.
    mlir::Block& patternBlock = optionalMatch.getPattern().front();
    for (size_t inputIndex = 0; inputIndex < inputChunks.size(); inputIndex++) {
        _valueMap[patternBlock.getArgument(static_cast<unsigned>(inputIndex))] = inputChunks[inputIndex];
    }

    if (!inputChunks.empty()) {
        _valueMap[patternBlock.getArgument(static_cast<unsigned>(inputChunks.size()))] = buffer.getTag();
    }

    // A dataflow of its own, rooted where the accumulator sits so its loops nest inside
    // this step; the caller's root and innermost loop are restored once it is lowered.
    mlir::Block* const previousRoot = _rootBlock;
    mlir::Block* const previousInnermostLoopBody = _innermostLoopBody;
    const mlir::Value previousInnermostCardinality = _innermostCardinality;
    _rootBlock = stepBlock;
    _innermostLoopBody = nullptr;
    _innermostCardinality = mlir::Value();

    if (patternLimit) {
        mlir::db::OptionalYield patternYield = mlir::cast<mlir::db::OptionalYield>(patternBlock.getTerminator());

        _producerWalkVisits.clear();
        for (const mlir::Value column : patternYield.getColumns()) {
            assignProducerLoops(column, optionalMatchOp, /*rowsDroppedBeforeTheCut=*/false, optionalMatchOp);
        }
    }

    llvm::SmallVector<mlir::Value, 4> matchedChunks;
    mlir::Value matchedTag;

    for (mlir::Operation& operation : patternBlock) {
        mlir::db::OptionalYield yield = mlir::dyn_cast<mlir::db::OptionalYield>(operation);
        if (!yield) {
            lowerOperation(operation);
            continue;
        }

        for (const mlir::Value column : yield.getColumns()) {
            matchedChunks.push_back(mapValue(column));
        }

        if (yield.getTag()) {
            matchedTag = mapValue(yield.getTag());
        }
    }

    // OptionalMatch::verify rejects a pattern yielding no column, so reaching this means
    // unverified IR - a defensive backstop, as in lowerFactor and lowerSort.
    if (matchedChunks.empty()) {
        throw IRException("db.optional_match pattern yields no column");
    }

    // The drain rebuilds a missed row's carried columns out of this step's input chunks, so
    // the two must be the same chunk type however the db types were spelled: a column a
    // CALL yielded enters type-erased and comes back refined, and the chunk behind it is
    // the same one either way.
    for (size_t inputIndex = 0; inputIndex < inputChunks.size(); inputIndex++) {
        if (matchedChunks[inputIndex].getType() != inputChunks[inputIndex].getType()) {
            throw IRException("db.optional_match carries an input column back as another chunk type");
        }
    }

    for (size_t chunkIndex = inputChunks.size(); chunkIndex < matchedChunks.size(); chunkIndex++) {
        matchedChunks[chunkIndex] = paddedColumnChunk(matchedChunks[chunkIndex]);
    }

    setInsertionInto(deepestOwnerBlock(matchedChunks, stepBlock));
    _builder.create<nl::OptionalCollect>(loc, state, matchedTag, matchedChunks);

    if (patternLimit) {
        _builder.create<nl::LimitUpdate>(loc, patternLimit, matchedChunks.front());
    }

    _rootBlock = previousRoot;
    _innermostLoopBody = previousInnermostLoopBody;
    _innermostCardinality = previousInnermostCardinality;

    llvm::SmallVector<mlir::Type, 4> chunkTypes;
    for (const mlir::Value chunk : matchedChunks) {
        chunkTypes.push_back(chunk.getType());
    }

    const nl::IteratorType iteratorType = nl::IteratorType::get(_builder.getContext(), chunkTypes);
    setInsertionInto(stepBlock);
    nl::OptionalDrain drain = _builder.create<nl::OptionalDrain>(loc, iteratorType, state);

    buildLoopForSource(drain.getResult(), optionalMatchOp);
}

void DBLowering::lowerCallSubquery(mlir::db::CallSubquery call) {
    llvm::SmallVector<mlir::Value, 4> inputChunks;
    for (const mlir::Value column : call.getInputColumns()) {
        inputChunks.push_back(mapValue(column));
    }

    // The step the body runs over is the one binding the columns it reads, so its loops
    // nest in that block and re-run once per chunk of them.
    mlir::Block* const stepBlock = deepestOwnerBlock(inputChunks, _rootBlock);

    const bool returning = !call.getUnit();
    if (returning && call.getOptional()) {
        lowerOptionalSubquery(call, stepBlock, inputChunks);
        return;
    } else if (runsPerRow(call)) {
        lowerSubqueryPerRow(call, stepBlock, inputChunks);
        return;
    }

    mlir::Block* const previousInnermostLoopBody = _innermostLoopBody;
    const mlir::Value previousInnermostCardinality = _innermostCardinality;

    llvm::SmallVector<mlir::Value, 4> yieldedChunks;
    mlir::Value yieldedTag;
    lowerSubqueryBody(call, stepBlock, inputChunks, yieldedChunks, yieldedTag);

    // A unit body leaves the rows in flight as they were, so what follows the op reads the
    // step's own chunks again. A body carrying its scope hands the rows on: its yielded
    // chunks are the results, bound in the body's innermost loop, which is where the rest
    // of the query goes on from - unless the body opened no loop and the step is still the
    // relation in flight.
    const bool bodyOpenedALoop = _innermostLoopBody != nullptr;
    if (!returning || !bodyOpenedALoop) {
        _innermostLoopBody = previousInnermostLoopBody;
        _innermostCardinality = previousInnermostCardinality;
    }

    if (!returning) {
        setInsertionInto(stepBlock);
        return;
    }

    const mlir::ResultRange results = call.getResults();
    for (size_t resultIndex = 0; resultIndex < results.size(); resultIndex++) {
        _valueMap[results[resultIndex]] = yieldedChunks[resultIndex];
    }
}

void DBLowering::lowerSubqueryBody(mlir::db::CallSubquery call,
                                   mlir::Block* stepBlock,
                                   llvm::ArrayRef<mlir::Value> inputChunks,
                                   llvm::SmallVectorImpl<mlir::Value>& yieldedChunks,
                                   mlir::Value& yieldedTag) {
    mlir::Block& bodyBlock = call.getBody().front();
    for (size_t inputIndex = 0; inputIndex < inputChunks.size(); inputIndex++) {
        _valueMap[bodyBlock.getArgument(static_cast<unsigned>(inputIndex))] = inputChunks[inputIndex];
    }

    // A dataflow of its own, rooted in the step so its loops nest inside it - and so its
    // accumulators, hoisted to the root, reset once per step. The caller's root comes back
    // once the body is lowered; the innermost loop is left as the body set it.
    mlir::Block* const previousRoot = _rootBlock;
    _rootBlock = stepBlock;
    _innermostLoopBody = nullptr;
    _innermostCardinality = mlir::Value();

    for (mlir::Operation& operation : bodyBlock) {
        mlir::db::SubqueryYield yield = mlir::dyn_cast<mlir::db::SubqueryYield>(operation);
        if (!yield) {
            lowerOperation(operation);
            continue;
        }

        for (const mlir::Value column : yield.getColumns()) {
            yieldedChunks.push_back(mapValue(column));
        }

        if (yield.getTag()) {
            yieldedTag = mapValue(yield.getTag());
        }
    }

    _rootBlock = previousRoot;
}

void DBLowering::lowerSubqueryPerRow(mlir::db::CallSubquery call,
                                     mlir::Block* stepBlock,
                                     llvm::ArrayRef<mlir::Value> inputChunks) {
    const mlir::Location loc = _builder.getUnknownLoc();
    const bool unit = call.getUnit();

    mlir::Block* const previousInnermostLoopBody = _innermostLoopBody;
    const mlir::Value previousInnermostCardinality = _innermostCardinality;

    llvm::SmallVector<mlir::Value, 4> yieldedChunks;

    // No input column means the step is the single empty row Cypher starts from: the body
    // runs once, in the step block, and what it yields is the result
    if (inputChunks.empty()) {
        hoistLimitHandles(call.getBody(), stepBlock, call);

        mlir::Value yieldedTag;
        lowerSubqueryBody(call, stepBlock, inputChunks, yieldedChunks, yieldedTag);

        if (unit) {
            _innermostLoopBody = nullptr;
            _innermostCardinality = mlir::Value();

            setInsertionInto(stepBlock);
        } else if (!_innermostLoopBody) {
            _innermostLoopBody = previousInnermostLoopBody;
            _innermostCardinality = previousInnermostCardinality;

            setInsertionInto(stepBlock);
        }

        const mlir::ResultRange results = call.getResults();
        for (size_t resultIndex = 0; resultIndex < results.size(); resultIndex++) {
            _valueMap[results[resultIndex]] = yieldedChunks[resultIndex];
        }

        return;
    }

    // The loop over the step's rows carries the handle of a limit downstream of the op,
    // as the loops feeding that limit do, so a spent budget stops the walk over the rows
    const mlir::Value limitHandle = loopLimitHandle(call.getOperation());

    setInsertionInto(stepBlock);
    nl::EachRow eachRow = _builder.create<nl::EachRow>(loc, inputChunks);
    nl::For rowLoop = _builder.create<nl::For>(loc, eachRow.getResult(), limitHandle);
    mlir::Block* const rowBody = rowLoop.getBody();

    llvm::SmallVector<mlir::Value, 4> rowChunks;
    for (const mlir::BlockArgument rowChunk : rowBody->getArguments()) {
        rowChunks.push_back(rowChunk);
    }

    // The body roots in the row loop, so its accumulators and the handles of its own
    // limits are hoisted into that body and cover one input row at a time
    hoistLimitHandles(call.getBody(), rowBody, call);

    mlir::Value yieldedTag;
    lowerSubqueryBody(call, rowBody, rowChunks, yieldedChunks, yieldedTag);

    // A unit body hands nothing back, so there is no chunk to pair the row with and no
    // relation for what follows to walk: the step's own rows are in flight still
    if (unit) {
        _innermostLoopBody = previousInnermostLoopBody;
        _innermostCardinality = previousInnermostCardinality;

        setInsertionInto(stepBlock);
        return;
    }

    // A constant the body returned stands for one value over the rows beside it rather
    // than for a row of its own, and the product reads its row count off the first chunk
    // it is handed, so the constants are laid out over those rows first.
    rowAlignFactorChunks(yieldedChunks);

    // One row against N pairs the input row with each of the N rows the body yielded for
    // it, which is the op's result: the inputs then the body's columns
    setInsertionInto(deepestOwnerBlock(yieldedChunks, rowBody));
    nl::CrossProduct cross = _builder.create<nl::CrossProduct>(loc, rowChunks, yieldedChunks);

    buildLoopForSource(cross.getResult(), call.getOperation());
}

void DBLowering::lowerOptionalSubquery(mlir::db::CallSubquery call,
                                       mlir::Block* stepBlock,
                                       llvm::ArrayRef<mlir::Value> inputChunks) {
    const mlir::Location loc = _builder.getUnknownLoc();

    // A body run one row at a time makes the accumulator's step one input row: the loop
    // over the rows is opened first and the body roots in it. A body carrying the scope,
    // and one over the single empty row, run in the step block as they otherwise would.
    const bool perRow = runsPerRow(call) && !inputChunks.empty();

    mlir::Block* bodyRoot = stepBlock;
    llvm::SmallVector<mlir::Value, 4> stepChunks(inputChunks.begin(), inputChunks.end());

    if (perRow) {
        const mlir::Value limitHandle = loopLimitHandle(call.getOperation());

        setInsertionInto(stepBlock);
        nl::EachRow eachRow = _builder.create<nl::EachRow>(loc, inputChunks);
        nl::For rowLoop = _builder.create<nl::For>(loc, eachRow.getResult(), limitHandle);
        bodyRoot = rowLoop.getBody();

        stepChunks.clear();
        for (const mlir::BlockArgument rowChunk : bodyRoot->getArguments()) {
            stepChunks.push_back(rowChunk);
        }
    }

    setInsertionInto(bodyRoot);
    nl::OptionalBuffer buffer = _builder.create<nl::OptionalBuffer>(loc, stepChunks);
    const mlir::Value state = buffer.getState();

    // A body carrying the scope reads the tag through its trailing argument and hands it
    // back; the others are tagged here
    mlir::Block& bodyBlock = call.getBody().front();
    if (bodyBlock.getNumArguments() > inputChunks.size()) {
        _valueMap[bodyBlock.getArgument(static_cast<unsigned>(inputChunks.size()))] = buffer.getTag();
    }

    if (runsPerRow(call)) {
        hoistLimitHandles(call.getBody(), bodyRoot, call);
    }

    llvm::SmallVector<mlir::Value, 4> yieldedChunks;
    mlir::Value yieldedTag;
    lowerSubqueryBody(call, bodyRoot, stepChunks, yieldedChunks, yieldedTag);

    // The accumulator appends one buffer per column and the drain reads them back by
    // position, so a constant the body returned is laid out over the rows beside it before
    // any of them is collected.
    rowAlignFactorChunks(yieldedChunks);

    // A body carrying the scope hands the input columns back ahead of its own. The drain
    // rebuilds those out of the step's chunks rather than padding them, so they stay the
    // chunk the accumulator recorded; only the body's own columns need a null to pad with.
    const size_t carriedInputs = call.getCarriesScope() ? stepChunks.size() : 0;

    for (size_t inputIndex = 0; inputIndex < carriedInputs; inputIndex++) {
        if (yieldedChunks[inputIndex].getType() != stepChunks[inputIndex].getType()) {
            throw IRException("db.call_subquery carries an input column back as another chunk type");
        }
    }

    for (size_t chunkIndex = carriedInputs; chunkIndex < yieldedChunks.size(); chunkIndex++) {
        yieldedChunks[chunkIndex] = paddedColumnChunk(yieldedChunks[chunkIndex]);
    }

    // What the collect appends: the inputs as the body left them then its own columns
    // when it carries the scope, and the row's chunks crossed with its columns when it
    // runs per row - one row against N, with the tag in the outer group so it lines up
    llvm::SmallVector<mlir::Value, 4> collected;
    mlir::Value collectedTag = yieldedTag;

    if (perRow) {
        llvm::SmallVector<mlir::Value, 4> outer(stepChunks.begin(), stepChunks.end());
        outer.push_back(buffer.getTag());

        setInsertionInto(deepestOwnerBlock(yieldedChunks, bodyRoot));
        nl::CrossProduct cross = _builder.create<nl::CrossProduct>(loc, outer, yieldedChunks);
        nl::For pairs = _builder.create<nl::For>(loc, cross.getResult(), mlir::Value {});

        const mlir::Block::BlockArgListType pairChunks = pairs.getBody()->getArguments();
        const size_t inputCount = stepChunks.size();

        collected.assign(pairChunks.begin(), pairChunks.begin() + inputCount);
        collectedTag = pairChunks[inputCount];
        collected.append(pairChunks.begin() + inputCount + 1, pairChunks.end());
    } else {
        collected.assign(yieldedChunks.begin(), yieldedChunks.end());
    }

    setInsertionInto(deepestOwnerBlock(collected, bodyRoot));
    _builder.create<nl::OptionalCollect>(loc, state, collectedTag, collected);

    llvm::SmallVector<mlir::Type, 4> chunkTypes;
    for (const mlir::Value chunk : collected) {
        chunkTypes.push_back(chunk.getType());
    }

    // The drain's loop binds the op's results: the inputs then the body's columns, padded
    // where it yielded nothing
    const nl::IteratorType iteratorType = nl::IteratorType::get(_builder.getContext(), chunkTypes);
    setInsertionInto(bodyRoot);
    nl::OptionalDrain drain = _builder.create<nl::OptionalDrain>(loc, iteratorType, state);

    buildLoopForSource(drain.getResult(), call.getOperation());
}

template <typename SubqueryOp, typename YieldOp>
void DBLowering::lowerSubqueryExpression(SubqueryOp subquery) {
    constexpr bool countsRows = std::is_same_v<SubqueryOp, mlir::db::CountSubquery>;
    constexpr bool collectsValues = std::is_same_v<SubqueryOp, mlir::db::CollectSubquery>;

    const mlir::Location loc = _builder.getUnknownLoc();
    mlir::MLIRContext* const context = _builder.getContext();

    llvm::SmallVector<mlir::Value, 4> inputChunks;
    for (const mlir::Value column : subquery.getInputColumns()) {
        inputChunks.push_back(mapValue(column));
    }

    // The step the accumulator covers is the one binding the columns the subquery answers
    // for, so it is emptied once per chunk of them - and the body's nest and the read of
    // the answers both go there, the read after the nest.
    mlir::Block* const stepBlock = deepestOwnerBlock(inputChunks, _rootBlock);
    const mlir::Value cardinality = inputChunks.empty() ? mlir::Value() : inputChunks.front();

    setInsertionInto(stepBlock);

    mlir::Value state;
    mlir::Value tag;

    if constexpr (collectsValues) {
        nl::RowListsBuffer buffer = _builder.create<nl::RowListsBuffer>(loc, cardinality);
        state = buffer.getState();
        tag = buffer.getTag();
    } else if constexpr (countsRows) {
        nl::CountSubqueryBuffer buffer = _builder.create<nl::CountSubqueryBuffer>(loc, inputChunks);
        state = buffer.getState();
        tag = buffer.getTag();
    } else {
        nl::ExistsBuffer buffer = _builder.create<nl::ExistsBuffer>(loc, inputChunks);
        state = buffer.getState();
        tag = buffer.getTag();
    }

    // A body run one row at a time is handed the step's rows a row at a time, each with its
    // tag, so what it holds is answered for under that row and the answers are read over the
    // whole step. A body carrying the scope, and one over the single empty row, run in the
    // step block as they otherwise would.
    const bool bodyRunsPerRow = runsPerRow(subquery);
    const bool perRow = bodyRunsPerRow && !inputChunks.empty();

    mlir::Block* bodyRoot = stepBlock;
    llvm::SmallVector<mlir::Value, 4> bodyChunks(inputChunks.begin(), inputChunks.end());
    mlir::Value rowTag;

    if (perRow) {
        llvm::SmallVector<mlir::Value, 4> rowChunks(inputChunks.begin(), inputChunks.end());
        rowChunks.push_back(tag);

        nl::EachRow eachRow = _builder.create<nl::EachRow>(loc, rowChunks);
        nl::For rowLoop = _builder.create<nl::For>(loc, eachRow.getResult(), mlir::Value());
        bodyRoot = rowLoop.getBody();

        bodyChunks.assign(bodyRoot->args_begin(), bodyRoot->args_begin() + inputChunks.size());
        rowTag = bodyRoot->getArgument(static_cast<unsigned>(inputChunks.size()));
    }

    // The body reads this step's rows through its block arguments, and the row tag through
    // the trailing one; a body run per row, or with nothing to join onto, takes no tag.
    mlir::Region& body = subquery.getBody();
    mlir::Block& bodyBlock = body.front();
    for (size_t inputIndex = 0; inputIndex < bodyChunks.size(); inputIndex++) {
        _valueMap[bodyBlock.getArgument(static_cast<unsigned>(inputIndex))] = bodyChunks[inputIndex];
    }

    if (bodyBlock.getNumArguments() > inputChunks.size()) {
        _valueMap[bodyBlock.getArgument(static_cast<unsigned>(inputChunks.size()))] = tag;
    }

    if (bodyRunsPerRow) {
        hoistLimitHandles(body, bodyRoot, subquery);
    }

    // A dataflow of its own, rooted where the accumulator sits so its loops nest inside
    // this step; the caller's root and innermost loop are restored once it is lowered.
    mlir::Block* const previousRoot = _rootBlock;
    mlir::Block* const previousInnermostLoopBody = _innermostLoopBody;
    const mlir::Value previousInnermostCardinality = _innermostCardinality;
    _rootBlock = bodyRoot;
    _innermostLoopBody = nullptr;
    _innermostCardinality = mlir::Value();

    for (mlir::Operation& operation : bodyBlock.without_terminator()) {
        lowerOperation(operation);
    }

    YieldOp yield = mlir::cast<YieldOp>(bodyBlock.getTerminator());

    llvm::SmallVector<mlir::Value, 4> heldChunks;
    for (const mlir::Value column : yield.getColumns()) {
        heldChunks.push_back(mapValue(column));
    }

    mlir::Value heldTag;
    if (const mlir::Value yieldedTag = yield.getTag()) {
        heldTag = mapValue(yieldedTag);
    }

    // A constant holds one cell standing for every row the body produced, so an untagged
    // body is answered off a chunk that carries those rows, where one does
    mlir::Value heldRows;
    if (!heldTag) {
        heldRows = cardinalityDriver(heldChunks);
        if (!heldRows) {
            heldRows = heldChunks.front();
        }
    }

    // The answer is taken where the body left its rows - its innermost loop body - which is
    // the deepest block the tag or the rows it holds are bound in.
    llvm::SmallVector<mlir::Value, 4> answeredChunks = heldChunks;
    answeredChunks.push_back(heldTag ? heldTag : heldRows);

    mlir::Value collectedValue;
    if constexpr (collectsValues) {
        collectedValue = rowListsValueChunk(mapValue(yield.getValue()), heldTag ? heldTag : heldRows);
        answeredChunks.push_back(collectedValue);
    }

    setInsertionInto(deepestOwnerBlock(answeredChunks, bodyRoot));

    if constexpr (collectsValues) {
        _builder.create<nl::RowListsCollect>(loc, state, heldTag, rowTag, collectedValue);
    } else if constexpr (countsRows) {
        _builder.create<nl::CountSubqueryTally>(loc, state, heldTag, rowTag, heldRows);
    } else {
        llvm::SmallVector<mlir::Value, 1> markedRows;
        if (heldRows) {
            markedRows.push_back(heldRows);
        }

        _builder.create<nl::ExistsMark>(loc, state, heldTag, rowTag, markedRows);
    }

    _rootBlock = previousRoot;
    _innermostLoopBody = previousInnermostLoopBody;
    _innermostCardinality = previousInnermostCardinality;

    setInsertionInto(stepBlock);

    mlir::Value answer;

    if constexpr (collectsValues) {
        const mlir::Type listType = storage::ListType::get(context, listedElementType(context, collectedValue));
        const nl::ChunkType listChunk = nl::ChunkType::get(context, listType);

        nl::RowListsResult result = _builder.create<nl::RowListsResult>(loc, listChunk, state, cardinality);
        answer = result.getResult();
    } else if constexpr (countsRows) {
        nl::CountSubqueryResult result = _builder.create<nl::CountSubqueryResult>(loc, state, cardinality);
        answer = result.getResult();
    } else {
        const nl::ChunkType boolChunk = nl::ChunkType::get(context, mlir::storage::BoolType::get(context));

        nl::ExistsResult result = _builder.create<nl::ExistsResult>(loc, boolChunk, state, cardinality);
        answer = result.getResult();
    }

    _valueMap[subquery.getResult()] = answer;
}

void DBLowering::lowerCrossProduct(mlir::db::CrossProduct product) {
    // The outer factor roots where this op would have - the entry block at top
    // level. The inner factor roots inside the outer factor's innermost loop
    // body, so its loops nest under the outer loop: a nested-loop join where the
    // inner factor re-runs once per outer chunk.
    mlir::Block* const rootBlock = _rootBlock;

    llvm::SmallVector<mlir::Value, 4> outerColumns;
    mlir::Block* const outerBody = lowerFactor(product.getLeftFactor(), rootBlock, outerColumns);

    llvm::SmallVector<mlir::Value, 4> innerColumns;
    mlir::Block* const innerBody = lowerFactor(product.getRightFactor(), outerBody, innerColumns);

    // The cross sits at the deepest point - the inner factor's innermost loop
    // body, where both factors have a chunk bound.
    setInsertionInto(innerBody);

    nl::CrossProduct cross = _builder.create<nl::CrossProduct>(_builder.getUnknownLoc(),
                                                               outerColumns,
                                                               innerColumns);

    // The pairs come out chunk by chunk, so the product drives a loop of its own
    // nested in the inner factor's - the third level of the nest, below the two the
    // factors opened. That binds the db results to the loop variables, carries the
    // limit handle when one governs the product, and leaves the loop body as the
    // innermost one, which is where the consumer (the lowered db.output) goes and
    // where an enclosing factor roots when this product is itself a factor - the
    // three-way MATCH (a), (b), (c).
    buildLoopForSource(cross.getResult(), product.getOperation());
}

void DBLowering::lowerHashJoin(mlir::db::HashJoin join) {
    // Both nests root where this op stands - the entry block at top level, the enclosing
    // factor's innermost loop body inside a product - so they are siblings rather than
    // nested: the built side is read once, not once per chunk of the other.
    mlir::Block* const rootBlock = _rootBlock;

    llvm::SmallVector<mlir::Value, 4> buildColumns;
    mlir::Block* const buildBody = lowerFactor(join.getRightFactor(), rootBlock, buildColumns);
    rowAlignFactorChunks(buildColumns);

    const mlir::Location loc = _builder.getUnknownLoc();

    _builder.setInsertionPointToStart(rootBlock);
    nl::HashJoinBuffer buffer = _builder.create<nl::HashJoinBuffer>(loc,
                                                                    join.getRightKey(),
                                                                    join.getLeftKey());
    const mlir::Value state = buffer.getState();

    setInsertionInto(buildBody);
    _builder.create<nl::HashJoinCollect>(loc, state, buildColumns);

    llvm::SmallVector<mlir::Value, 4> probeColumns;
    mlir::Block* const probeBody = lowerFactor(join.getLeftFactor(), rootBlock, probeColumns);
    rowAlignFactorChunks(probeColumns);

    setInsertionInto(probeBody);

    llvm::SmallVector<mlir::Type, 8> resultTypes;
    for (const mlir::Value column : probeColumns) {
        resultTypes.push_back(column.getType());
    }
    for (const mlir::Value column : buildColumns) {
        resultTypes.push_back(column.getType());
    }

    // The join's chunks are the left factor's yielded columns followed by the right
    // factor's, which is how the probe lays its own out: probed side then built side.
    const nl::IteratorType iteratorType = nl::IteratorType::get(_builder.getContext(), resultTypes);
    nl::HashJoinProbe probe = _builder.create<nl::HashJoinProbe>(loc,
                                                                 iteratorType,
                                                                 state,
                                                                 probeColumns);

    // A key many build rows carry makes more pairs than the probe chunk holds, so the
    // pairs come out chunk by chunk and the probe drives a loop of its own nested in the
    // probe factor's - as a cross product's pairs do. That binds the db results to the
    // loop variables, carries the limit handle when one governs the join, and leaves the
    // loop body as the innermost one, where the consumer goes and where an enclosing
    // factor roots when this join is itself a factor.
    buildLoopForSource(probe.getResult(), join.getOperation());
}

mlir::Block* DBLowering::lowerFactor(mlir::Region& factor,
                                     mlir::Block* rootBlock,
                                     llvm::SmallVectorImpl<mlir::Value>& yieldedChunks) {
    // Root this factor's scans at rootBlock and track its own innermost loop;
    // save and restore the caller's so nested or sibling products are unaffected.
    mlir::Block* const previousRoot = _rootBlock;
    mlir::Block* const previousInnermostLoopBody = _innermostLoopBody;
    const mlir::Value previousInnermostCardinality = _innermostCardinality;
    _rootBlock = rootBlock;
    _innermostLoopBody = nullptr;
    _innermostCardinality = mlir::Value();

    // A factor is one self-contained block ending in a db.yield. Lower each op
    // as at top level; the yield names the columns this factor contributes, so
    // map its operands to the nl chunks they lowered to rather than lowering it.
    for (mlir::Operation& operation : factor.front()) {
        if (mlir::db::Yield yield = mlir::dyn_cast<mlir::db::Yield>(operation)) {
            for (const mlir::Value column : yield.getColumns()) {
                yieldedChunks.push_back(mapValue(column));
            }
        } else {
            lowerOperation(operation);
        }
    }

    // A factor's row count is read from its first yielded column, so a factor
    // that yields none cannot size its side of the product. The db.cross_product
    // verifier rejects this, so reaching it here means unverified IR - a
    // defensive backstop.
    if (yieldedChunks.empty()) {
        throw IRException("cross_product factor yields no column");
    }

    // The block the factor's columns are bound in, which is where the other factor has to
    // root: the innermost loop body of a factor that walks a relation, and the root block
    // itself for one whose columns are a single row bound above every loop - a scalar
    // aggregate, or a constant laid out over the one row it is
    mlir::Block* const factorBody = deepestOwnerBlock(yieldedChunks, rootBlock);

    _rootBlock = previousRoot;
    _innermostLoopBody = previousInnermostLoopBody;
    _innermostCardinality = previousInnermostCardinality;

    return factorBody;
}

mlir::Type DBLowering::columnType(mlir::Value column) {
    return mlir::cast<mlir::db::ColumnType>(column.getType()).getType();
}

void DBLowering::lowerSort(mlir::db::Sort sort) {
    // The nl chunks the sorted columns lowered to; these are what sort_collect
    // appends to the buffers, and the emit loop yields back sorted.
    llvm::SmallVector<mlir::Value, 4> chunks;
    for (const mlir::Value column : sort.getColumns()) {
        chunks.push_back(mapValue(column));
    }

    // Sort::verify rejects an empty db.sort, so reaching it here means unverified
    // IR - a defensive backstop, as in lowerFactor and lowerLimit.
    if (chunks.empty()) {
        throw IRException("db.sort requires at least one column");
    }

    rowAlignBufferedChunks(chunks);

    const mlir::Location loc = _builder.getUnknownLoc();

    // The accumulator and its sort spec are hoisted to the top of the root block,
    // above every loop of this dataflow, so the buffers exist before the producing
    // loop fills them and the handle dominates the collect and the emit loop. A sort
    // fused with a terminal db.limit carries that count as its top-K bound, so the
    // accumulator keeps only the best k rows; an unfused sort keeps every row.
    const auto fusedIt = _sortTopK.find(sort.getOperation());
    mlir::IntegerAttr topKAttr;
    mlir::Value boundTopK;
    if (fusedIt != _sortTopK.end()) {
        mlir::db::Limit fusedLimit = fusedIt->second;
        const mlir::Value boundCount = fusedLimit.getBoundCount();

        topKAttr = fusedLimit.getCountAttr();
        boundTopK = boundCount ? mapValue(boundCount) : mlir::Value();
    }

    setInsertionAtStart(_rootBlock, boundTopK);

    nl::SortBuffer bufferOp = _builder.create<nl::SortBuffer>(loc,
                                                              sort.getKeyColumnsAttr(),
                                                              sort.getKeyAscendingAttr(),
                                                              topKAttr,
                                                              boundTopK);
    const mlir::Value state = bufferOp.getState();

    // The collect appends each step's chunk of every column to the buffers. It
    // sits in the innermost producing loop body, where all sorted columns are
    // bound together (the same block db.output would emit from), so the buffers
    // stay row-aligned.
    const mlir::Value representative = chunks.front();
    setInsertionInto(ownerBlock(representative));
    _builder.create<nl::SortCollect>(loc, state, chunks);

    // The emit phase is an nl.sort source iterator plus its nl.for, placed after
    // the producing loop (before the func.return) so the buffers are full when
    // the loop first steps. The iterator yields one chunk per collected column,
    // so its chunk types are exactly the collected chunk types.
    llvm::SmallVector<mlir::Type, 4> chunkTypes;
    for (const mlir::Value chunk : chunks) {
        chunkTypes.push_back(chunk.getType());
    }

    const nl::IteratorType iteratorType = nl::IteratorType::get(_builder.getContext(), chunkTypes);

    setInsertionInto(_rootBlock);
    nl::Sort sortOp = _builder.create<nl::Sort>(loc, iteratorType, state);

    // The emit loop binds one variable per sorted column. It is the only loop of this
    // sort a limit may bound - assignProducerLoops stops at the sort and hands the
    // handle to it, never to the producing loops, which had to see every row - so it
    // stops re-chunking once a downstream streaming limit is spent. A limit fused into
    // the top-K bounds nothing here: the accumulator already holds at most k rows.
    // buildLoopForSource maps db.sort's results to the loop variables, so the
    // db.output that follows lowers into the emit loop body reading the sorted chunks.
    buildLoopForSource(sortOp.getResult(), sort.getOperation());
}

// Each branch is lowered as a program of its own rooted in the entry block, so its loops
// are appended after the ones the branch before it opened and the sink sees one branch's
// rows and then the next. What a branch binds is unreachable from the next, so the
// innermost-loop record is cleared between them rather than carried over.
void DBLowering::lowerUnion(mlir::db::Union unionOp) {
    if (unionOp.getNumResults() > 0) {
        lowerUnionResults(unionOp);
        return;
    }

    const mlir::MutableArrayRef<mlir::Region> branches = unionOp.getBranches();

    mlir::Block* const previousRoot = _rootBlock;
    mlir::Block* const previousInnermostLoopBody = _innermostLoopBody;
    const mlir::Value previousInnermostCardinality = _innermostCardinality;

    llvm::SmallVector<nl::Output, 4> branchOutputs;

    for (mlir::Region& branch : branches) {
        _rootBlock = _entryBlock;
        _innermostLoopBody = nullptr;
        _innermostCardinality = mlir::Value();

        mlir::db::Output branchOutput = mlir::cast<mlir::db::Output>(branch.front().back());

        for (mlir::Operation& operation : branch.front()) {
            llvm::SmallVector<std::pair<mlir::Value, mlir::Value>, 4> replacedMappings;
            convertUnionResultChunks(operation, branchOutput.getColumns(), replacedMappings);

            if (&operation == branchOutput.getOperation()) {
                branchOutputs.push_back(lowerOutput(branchOutput));
            } else {
                lowerOperation(operation);
            }

            for (const auto& [column, chunk] : replacedMappings) {
                _valueMap[column] = chunk;
            }
        }
    }

    llvm::SmallVector<mlir::MutableOperandRange, 4> branchColumns;
    for (nl::Output branchOutput : branchOutputs) {
        branchColumns.push_back(branchOutput.getColumnsMutable());
    }

    nl::Output firstOutput = branchOutputs.front();
    const std::optional<mlir::ArrayAttr> columnNames = firstOutput.getColumnNames();

    reconcileBranchResultTypes(branchColumns, columnNames.value_or(mlir::ArrayAttr()));

    _rootBlock = previousRoot;
    _innermostLoopBody = previousInnermostLoopBody;
    _innermostCardinality = previousInnermostCardinality;
}

// The branches of a union inside a CALL body feed the rest of the body, which is lowered
// once, so their rows meet in an accumulator: each branch is rooted in the block the body
// runs over, collects what it yields there, and the loop draining it is where the body
// goes on from.
void DBLowering::lowerUnionResults(mlir::db::Union unionOp) {
    const mlir::Location loc = _builder.getUnknownLoc();
    mlir::Block* const root = _rootBlock;

    _builder.setInsertionPointToStart(root);
    const mlir::Value state = _builder.create<nl::RowBuffer>(loc).getState();

    llvm::SmallVector<nl::RowCollect, 4> collects;

    for (mlir::Region& branch : unionOp.getBranches()) {
        lowerCollectedBranch(branch, root, state, collects);
    }

    drainCollectedBranches(collects, state, root, unionOp.getOperation());
}

void DBLowering::lowerConditional(mlir::db::Conditional conditional) {
    const mlir::Location loc = _builder.getUnknownLoc();
    mlir::MLIRContext* const context = _builder.getContext();
    const nl::ChunkType maskType = nl::ChunkType::get(context, storage::BoolType::get(context));

    mlir::Block* const root = _rootBlock;
    mlir::Block* const previousInnermostLoopBody = _innermostLoopBody;
    const mlir::Value previousInnermostCardinality = _innermostCardinality;
    mlir::Block* const previousConstantComputationBlock = _constantComputationBlock;

    const bool returning = conditional.getNumResults() > 0;

    mlir::Value state;
    if (returning) {
        _builder.setInsertionPointToStart(root);
        state = _builder.create<nl::RowBuffer>(loc).getState();
    }

    _constantComputationBlock = root;

    llvm::SmallVector<mlir::Value, 4> readColumns;
    collectRowColumnsReadByBranches(conditional, readColumns);

    llvm::SmallVector<mlir::Value, 8> outerChunks;
    for (const mlir::Value column : readColumns) {
        outerChunks.push_back(mapValue(column));
    }

    // Codegen hands a WHEN its rows through the columns its branches read, so one reading
    // none runs once: the loop open before it may be a set a pass scanned ahead of it
    llvm::SmallVector<mlir::Value, 8> reaching(outerChunks.begin(), outerChunks.end());
    mlir::Value reachingCardinality = reaching.empty() ? mlir::Value() : reaching.front();

    const mlir::MutableArrayRef<mlir::Region> regions = conditional.getBranches();
    const size_t conditionCount = regions.size() / 2;
    const bool hasDefault = regions.size() % 2 == 1;

    llvm::SmallVector<nl::RowCollect, 4> collects;

    for (size_t conditionIndex = 0; conditionIndex < conditionCount; conditionIndex++) {
        for (size_t columnIndex = 0; columnIndex < readColumns.size(); columnIndex++) {
            _valueMap[readColumns[columnIndex]] = reaching[columnIndex];
        }

        const mlir::Value condition = lowerBlockOverRows(regions[2 * conditionIndex].front(), reachingCardinality);

        llvm::SmallVector<mlir::Value, 8> taken;
        filterCaseRows(condition, reaching, taken);
        lowerConditionalBranch(regions[2 * conditionIndex + 1], readColumns, taken, state, collects);

        const bool rowsFallThrough = hasDefault || conditionIndex + 1 < conditionCount;
        if (!rowsFallThrough) {
            break;
        }

        setInsertionForUnaryOp(condition);
        const mlir::Value fallsThrough = _builder.create<nl::IsNotTrue>(loc, maskType, condition).getResult();

        llvm::SmallVector<mlir::Value, 8> rest;
        filterCaseRows(fallsThrough, reaching, rest);

        reachingCardinality = rest.front();
        reaching.assign(rest.begin() + 1, rest.end());
    }

    if (hasDefault) {
        llvm::SmallVector<mlir::Value, 8> taken {reachingCardinality};
        taken.append(reaching.begin(), reaching.end());

        lowerConditionalBranch(regions.back(), readColumns, taken, state, collects);
    }

    for (size_t columnIndex = 0; columnIndex < readColumns.size(); columnIndex++) {
        _valueMap[readColumns[columnIndex]] = outerChunks[columnIndex];
    }

    _constantComputationBlock = previousConstantComputationBlock;

    if (!returning) {
        _innermostLoopBody = previousInnermostLoopBody;
        _innermostCardinality = previousInnermostCardinality;

        setInsertionInto(root);
        return;
    }

    drainCollectedBranches(collects, state, root, conditional.getOperation());
}

void DBLowering::lowerConditionalBranch(mlir::Region& branch,
                                        llvm::ArrayRef<mlir::Value> readColumns,
                                        llvm::ArrayRef<mlir::Value> taken,
                                        mlir::Value state,
                                        llvm::SmallVectorImpl<nl::RowCollect>& collects) {
    const mlir::Location loc = _builder.getUnknownLoc();
    mlir::Block* const root = _rootBlock;

    setInsertionInto(deepestOwnerBlock(taken, root));
    nl::EachRow eachRow = _builder.create<nl::EachRow>(loc, taken);
    nl::For branchLoop = _builder.create<nl::For>(loc, eachRow.getResult(), mlir::Value());
    mlir::Block* const branchRoot = branchLoop.getBody();

    for (size_t columnIndex = 0; columnIndex < readColumns.size(); columnIndex++) {
        const unsigned argumentIndex = static_cast<unsigned>(columnIndex + 1);
        _valueMap[readColumns[columnIndex]] = branchRoot->getArgument(argumentIndex);
    }

    _rootBlock = branchRoot;
    _constantComputationBlock = branchRoot;

    lowerCollectedBranch(branch, branchRoot, state, collects);

    _rootBlock = root;
    _constantComputationBlock = root;
}

void DBLowering::lowerCollectedBranch(mlir::Region& branch,
                                      mlir::Block* root,
                                      mlir::Value state,
                                      llvm::SmallVectorImpl<nl::RowCollect>& collects) {
    const mlir::Location loc = _builder.getUnknownLoc();

    _innermostLoopBody = nullptr;
    _innermostCardinality = mlir::Value();

    mlir::db::Yield branchYield = mlir::cast<mlir::db::Yield>(branch.front().back());

    for (mlir::Operation& operation : branch.front()) {
        llvm::SmallVector<std::pair<mlir::Value, mlir::Value>, 4> replacedMappings;
        convertUnionResultChunks(operation, branchYield.getColumns(), replacedMappings);

        if (&operation != branchYield.getOperation()) {
            lowerOperation(operation);
        } else if (state) {
            llvm::SmallVector<mlir::Value, 4> chunks;
            for (const mlir::Value column : branchYield.getColumns()) {
                chunks.push_back(mapValue(column));
            }

            rowAlignBufferedChunks(chunks);

            // A branch of constants alone lays them out where they are bound, above the
            // root, which the collect must still run once per step of
            setInsertionInto(deepestOwnerBlock(chunks, root));
            collects.push_back(_builder.create<nl::RowCollect>(loc, state, chunks));
        }

        for (const auto& [column, chunk] : replacedMappings) {
            _valueMap[column] = chunk;
        }
    }
}

void DBLowering::drainCollectedBranches(llvm::ArrayRef<nl::RowCollect> collects,
                                        mlir::Value state,
                                        mlir::Block* root,
                                        mlir::Operation* holder) {
    const mlir::Location loc = _builder.getUnknownLoc();

    llvm::SmallVector<mlir::MutableOperandRange, 4> branchColumns;
    for (nl::RowCollect collect : collects) {
        branchColumns.push_back(collect.getColumnsMutable());
    }

    reconcileBranchResultTypes(branchColumns, mlir::ArrayAttr());

    nl::RowCollect firstCollect = collects.front();
    const llvm::SmallVector<mlir::Type, 4> chunkTypes(firstCollect.getColumns().getTypes());
    const nl::IteratorType iteratorType = nl::IteratorType::get(_builder.getContext(), chunkTypes);

    setInsertionInto(root);
    nl::RowDrain drain = _builder.create<nl::RowDrain>(loc, iteratorType, state);

    buildLoopForSource(drain.getResult(), holder);
}

void DBLowering::collectRowColumnsReadByBranches(mlir::db::Conditional conditional,
                                                 llvm::SmallVectorImpl<mlir::Value>& columns) {
    mlir::Operation* const holder = conditional.getOperation();
    llvm::SmallPtrSet<mlir::Value, 8> seen;

    holder->walk([&](mlir::Operation* operation) {
        if (operation == holder) {
            return;
        }

        for (mlir::OpOperand& use : operation->getOpOperands()) {
            mlir::Value operand = use.get();
            const bool readFromAbove = !holder->isAncestor(operand.getParentRegion()->getParentOp());
            const bool isColumn = mlir::isa<mlir::db::ColumnType>(operand.getType());
            if (!readFromAbove || !isColumn || readsAheadOfTheRows(use) || seen.contains(operand)) {
                continue;
            }

            seen.insert(operand);

            if (!yieldsConstantColumn(mapValue(operand), _constantColumns)) {
                columns.push_back(operand);
            }
        }
    });
}

// A dedup keys a row on the bytes of the chunk it is handed, so a branch's result columns
// are converted on their way into it rather than on their way out: a count keyed as a
// plain ui64 and a property keyed as a nullable i64 spell the same number two different
// ways, and the duplicate the union exists to drop survives.
void DBLowering::convertUnionResultChunks(mlir::Operation& operation,
                                          mlir::OperandRange resultColumns,
                                          llvm::SmallVectorImpl<std::pair<mlir::Value, mlir::Value>>& replacedMappings) {
    llvm::SmallVector<mlir::Value, 4> columns;

    if (mlir::isa<mlir::db::Output, mlir::db::Yield>(operation)) {
        columns.assign(resultColumns.begin(), resultColumns.end());
    } else if (mlir::db::RemoveDuplicates dedup = mlir::dyn_cast<mlir::db::RemoveDuplicates>(operation)) {
        const mlir::OperandRange dedupColumns = dedup.getColumns();
        const mlir::ResultRange results = dedup.getResults();

        for (size_t columnIndex = 0; columnIndex < results.size(); columnIndex++) {
            if (llvm::is_contained(resultColumns, results[columnIndex])) {
                columns.push_back(dedupColumns[columnIndex]);
            }
        }
    }

    if (columns.empty()) {
        return;
    }

    // A constant is converted once it holds rows: a union inside a CALL body lays its
    // constants out here rather than in codegen
    llvm::SmallVector<mlir::Value, 4> chunks;
    for (const mlir::Value column : columns) {
        chunks.push_back(mapValue(column));
    }

    rowAlignBufferedChunks(chunks);

    for (size_t columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
        const mlir::Value column = columns[columnIndex];
        replacedMappings.emplace_back(column, mapValue(column));
        _valueMap[column] = unionColumnChunk(chunks[columnIndex]);
    }
}

// A result column carries one type for the whole result, which is only known here: a
// property fetch is typed none until its name is resolved against the schema. A branch not
// holding that type is converted to it before the dedup the branches share, so that every
// branch spells a row's key the one way.
void DBLowering::reconcileBranchResultTypes(llvm::ArrayRef<mlir::MutableOperandRange> branchColumns,
                                            mlir::ArrayAttr columnNames) {
    const size_t columnCount = branchColumns.front().size();

    for (size_t columnIndex = 0; columnIndex < columnCount; columnIndex++) {
        const mlir::Type columnType = unionResultType(branchColumns, columnIndex, columnNames);

        for (const mlir::MutableOperandRange& columns : branchColumns) {
            mlir::OpOperand& operand = columns[static_cast<unsigned>(columnIndex)];
            if (operand.get().getType() != columnType) {
                retypeBranchColumn(operand, columnType);
            }
        }
    }
}

// The type the branches of a result column agree on, when every one holds it and it holds
// a branch's null. Where they disagree, each row keeps the type of its value, as the rows a
// CASE selects do: the column holds the element their values select into. Where every
// branch is the null literal, the first branch's untyped null is the column.
mlir::Type DBLowering::unionResultType(llvm::ArrayRef<mlir::MutableOperandRange> branchColumns,
                                       size_t columnIndex,
                                       mlir::ArrayAttr columnNames) {
    const unsigned operandIndex = static_cast<unsigned>(columnIndex);

    mlir::Type sharedType;
    mlir::Type element;
    bool branchesAgree = true;
    bool holdsANull = false;

    for (const mlir::MutableOperandRange& columns : branchColumns) {
        const mlir::Type branchType = columns[operandIndex].get().getType();
        if (isUntypedNullChunk(branchType)) {
            holdsANull = true;
            continue;
        }

        const mlir::Type branchElement = chunkValueElement(_builder, branchType);
        holdsANull = holdsANull || isNullableChunk(branchType);

        if (!sharedType) {
            sharedType = branchType;
            element = branchElement;
            continue;
        }

        const mlir::Type sharedElement = chunkValueElement(_builder, sharedType);
        const bool holdsCells = holdsAsTaggedCell(branchElement) && holdsAsTaggedCell(sharedElement);
        if (branchElement != sharedElement && !holdsCells) {
            throwOnDisagreeingBranchTypes(branchType, sharedType, columnNames, columnIndex);
        }

        branchesAgree = branchesAgree && branchType == sharedType;

        const mlir::Type selected = selectedValueElement(_builder, element, branchElement);
        element = selected ? selected : storage::AnyType::get(_builder.getContext());
    }

    if (!sharedType) {
        return branchColumns.front()[operandIndex].get().getType();
    } else if (branchesAgree && (!holdsANull || holdsItsNull(sharedType))) {
        return sharedType;
    }

    const bool isContainer = mlir::isa<storage::ListType, storage::MapType>(element);
    if (isContainer && !holdsANull) {
        return nl::ChunkType::get(_builder.getContext(), element);
    }

    return caseResultChunkType(element);
}

// A branch that dedups keys its rows on the chunks it is handed, so its column is converted
// ahead of its filter, and the filter's survivors take the converted type
void DBLowering::retypeBranchColumn(mlir::OpOperand& operand, mlir::Type chunkType) {
    const mlir::Value chunk = operand.get();

    nl::DistinctFilter filter = chunk.getDefiningOp<nl::DistinctFilter>();
    if (!filter) {
        operand.set(branchChunkAs(chunk, chunkType));
        return;
    }

    mlir::OpResult survivors = mlir::cast<mlir::OpResult>(chunk);
    mlir::OpOperand& key = filter.getColumnsMutable()[survivors.getResultNumber()];

    key.set(branchChunkAs(key.get(), chunkType));
    survivors.setType(chunkType);
}

// @param chunk as a column of @param chunkType, one unionResultType answers: a column of
// tagged cells, or the branch's own values in the shape that holds the null of another
mlir::Value DBLowering::branchChunkAs(mlir::Value chunk, mlir::Type chunkType) {
    if (chunk.getType() == chunkType) {
        return chunk;
    } else if (isUntypedNullChunk(chunk.getType())) {
        return typedNullChunk(chunk, chunkType);
    }

    const mlir::Type resultElement = mlir::cast<nl::ChunkType>(chunkType).getElementType();
    if (mlir::isa<storage::AnyType>(resultElement)) {
        return toAnyChunk(chunk, chunkType);
    }

    const auto nullableResult = mlir::dyn_cast<storage::NullableType>(resultElement);
    const mlir::Type resultValue = nullableResult ? nullableResult.getValueType() : resultElement;

    // Two lists of different elements are the list they join to, and two integers the one
    // integer Cypher has
    mlir::Value converted = chunk;
    const bool convertsElement = chunkValueElement(_builder, converted.getType()) != resultValue;

    if (convertsElement && mlir::isa<storage::ListType>(resultValue)) {
        mlir::MLIRContext* const context = _builder.getContext();
        const bool nullableList = isNullableChunk(converted.getType());
        const mlir::Type listElement = nullableList ? mlir::Type(storage::NullableType::get(context, resultValue)) : resultValue;

        converted = toAnyChunk(converted, nl::ChunkType::get(context, listElement));
    } else if (convertsElement) {
        converted = chunkAsElement(converted, resultValue);
    }

    if (!nullableResult) {
        return converted;
    }

    return paddedColumnChunk(converted);
}

void DBLowering::throwOnDisagreeingBranchTypes(mlir::Type branchType,
                                               mlir::Type resultType,
                                               mlir::ArrayAttr columnNames,
                                               size_t columnIndex) {
    std::string branchName;
    describeColumnType(branchType, branchName);

    std::string resultName;
    describeColumnType(resultType, resultName);

    std::string column = "A column";
    if (columnNames) {
        const llvm::StringRef columnName = mlir::cast<mlir::StringAttr>(columnNames[columnIndex]).getValue();
        column = fmt::format("Column '{}'", std::string_view {columnName.data(), columnName.size()});
    }

    throw IRException(fmt::format("{} of a UNION or a WHEN cannot hold both {} and {} values",
                                  column,
                                  branchName,
                                  resultName));
}

// The shared seen-set is hoisted to the top of the root block, above every branch's
// loops, so it is emptied once per step of that block and dominates each branch's filter -
// the same placement lowerRemoveDuplicates gives a dedup's private set.
void DBLowering::lowerDistinctSet(mlir::db::DistinctSet distinctSet) {
    _builder.setInsertionPointToStart(_rootBlock);

    _valueMap[distinctSet.getSet()] = _builder.create<nl::Distinct>(_builder.getUnknownLoc()).getState();
}

void DBLowering::lowerMaterialize(mlir::db::Materialize materialize) {
    const mlir::Location loc = _builder.getUnknownLoc();

    llvm::SmallVector<mlir::Value, 4> chunks;
    for (const mlir::Value column : materialize.getColumns()) {
        chunks.push_back(mapValue(column));
    }

    rowAlignBufferedChunks(chunks);

    _builder.setInsertionPointToStart(_rootBlock);
    const mlir::Value state = _builder.create<nl::RowBuffer>(loc).getState();

    setInsertionInto(deepestOwnerBlock(chunks, _rootBlock));
    _builder.create<nl::RowCollect>(loc, state, chunks);

    _valueMap[materialize.getRows()] = state;
}

void DBLowering::lowerScanMaterialized(mlir::db::ScanMaterialized scan) {
    const mlir::Value state = mapValue(scan.getRows());

    nl::RowCollect collect;
    for (mlir::Operation* user : state.getUsers()) {
        if (nl::RowCollect stateCollect = mlir::dyn_cast<nl::RowCollect>(user)) {
            collect = stateCollect;
            break;
        }
    }

    if (!collect) {
        throw IRException("db.scan_materialized reads rows no db.materialize collected");
    }

    drainCollectedBranches({collect}, state, _rootBlock, scan.getOperation());
}

void DBLowering::lowerRemoveDuplicates(mlir::db::RemoveDuplicates distinct) {
    // The nl chunks the deduped columns lowered to; these are what the filter
    // reads to build each row's key, and gathers the survivors from.
    llvm::SmallVector<mlir::Value, 4> chunks;
    for (const mlir::Value column : distinct.getColumns()) {
        chunks.push_back(mapValue(column));
    }

    // RemoveDuplicates::verify rejects an empty db.remove_duplicates, so reaching
    // it here means unverified IR - a defensive backstop, as in lowerSort.
    if (chunks.empty()) {
        throw IRException("db.remove_duplicates requires at least one column");
    }

    rowAlignBufferedChunks(chunks);

    const mlir::Location loc = _builder.getUnknownLoc();

    // The seen-set handle is hoisted to the top of the root block, above every loop
    // of this dataflow, so it is reset once per step of that block and dominates the
    // filter placed in the producing loop body. A dedup naming a shared set reads that
    // one instead, so every branch of a union records its rows in the same set.
    mlir::Value state;
    if (const mlir::Value sharedSet = distinct.getSet()) {
        state = mapValue(sharedSet);
    } else {
        _builder.setInsertionPointToStart(_rootBlock);
        state = _builder.create<nl::Distinct>(loc).getState();
    }

    // The filter sits in the innermost producing loop body, where all deduped
    // columns are bound together (the same block db.output would emit from), and
    // emits each step's not-yet-seen rows as fresh survivor chunks. It opens no
    // loop of its own: DISTINCT streams, so - unlike db.sort - the rows are
    // filtered in place in the producing loop, not accumulated and re-emitted.
    setInsertionInto(deepestOwnerBlock(chunks, _rootBlock));
    nl::DistinctFilter filter = _builder.create<nl::DistinctFilter>(loc, state, chunks);

    // Map db.remove_duplicates' results to the survivor chunks, so its consumer
    // reads the deduped rows: nl.output when the query ends here, or a downstream
    // traversal when a WITH DISTINCT feeds a further MATCH (the chained case). The
    // survivor chunk is a genuine cut chunk (like nl.limit_truncate's), so that
    // consumer needs no DISTINCT awareness of its own.
    const mlir::ResultRange dbResults = distinct.getResults();
    const mlir::ResultRange filteredChunks = filter.getResults();
    for (size_t resultIndex = 0; resultIndex < dbResults.size(); resultIndex++) {
        _valueMap[dbResults[resultIndex]] = filteredChunks[resultIndex];
    }

    followCardinalityThrough(chunks, filteredChunks);
}

void DBLowering::lowerCount(mlir::db::Count count) {
    // The nl chunk the counted column lowered to; the update reads its per-step
    // non-null row count. A constant column counts the rows it stands for, not the
    // one row it is, so it is laid out over the driving relation first.
    const mlir::Value inputChunk = rowAlignedChunk(mapValue(count.getInput()), rowsInFlight());

    const mlir::Location loc = _builder.getUnknownLoc();

    // The tally is hoisted to the top of the root block, above every loop of this
    // dataflow, so it is reset once per step of that block and dominates the update
    // placed in the producing loop body and the emit that reads it after the loop.
    _builder.setInsertionPointToStart(_rootBlock);
    const mlir::Value state = _builder.create<nl::Count>(loc).getState();

    // count(DISTINCT x) feeds the tally the survivors of a DISTINCT instead of the raw
    // column. A null does survive the filter - all nulls share one key - but is still
    // never charged, since nl.count_update counts only non-null rows, so the tally
    // comes out as the distinct non-null value count Cypher asks for.
    mlir::Value distinctState;
    if (count.getDistinct()) {
        distinctState = _builder.create<nl::Distinct>(loc).getState();
    }

    // The update sits in the innermost producing loop body, where the counted
    // column is bound (the same block db.output would emit from), and charges each
    // step's non-null rows against the tally.
    mlir::Block* const producingBlock = accumulatorUpdateBlock(ownerBlock(inputChunk));
    setInsertionInto(producingBlock);

    mlir::Value countedChunk = inputChunk;
    if (distinctState) {
        nl::DistinctFilter filter = _builder.create<nl::DistinctFilter>(loc, distinctState, mlir::ValueRange {inputChunk});
        countedChunk = filter.getResults().front();
    }

    _builder.create<nl::CountUpdate>(loc, state, countedChunk, count.getRows());

    // COUNT is a pipeline breaker: the tally is final only once every row has been
    // seen. Since it collapses to exactly one row there is nothing to iterate, so -
    // unlike db.sort - it opens no emit loop: nl.count_result materializes the tally
    // chunk in place at function scope, right after the producing loop, and db.output
    // consumes it there. The chunk is the single-row count as an unsigned i64
    // (!nl.chunk<ui64>) - a non-negative tally that is never null, so no nullable
    // wrapper.
    mlir::MLIRContext* const context = _builder.getContext();
    const mlir::Type countElementType = _builder.getIntegerType(64, /*isSigned=*/false);
    const nl::ChunkType countChunkType = nl::ChunkType::get(context, countElementType);

    setInsertionAfterProducingLoop(producingBlock);
    nl::CountResult result = _builder.create<nl::CountResult>(loc, countChunkType, state);

    // db.count's result maps to that chunk, so the db.output that follows lowers
    // into a function-scope nl.output reading it - the block that holds the chunk is
    // the entry block, so lowerOutput places nl.output there.
    _valueMap[count.getResult()] = result.getResult();

    // The count collapses the rows in flight to its one row, so from here on that row is
    // what sizes a projection of constants alone
    _innermostCardinality = result.getResult();
}

void DBLowering::lowerCountScanRows(mlir::db::CountScanRows countScanRows) {
    // The tally reads no column, but it is the rows in flight, so it goes at the top of the
    // root and not of the entry block: in a body run once per row, the ops over its row run
    // once per run.
    _builder.setInsertionPointToStart(_rootBlock);

    nl::CountScanRows rows = _builder.create<nl::CountScanRows>(_builder.getUnknownLoc(),
                                                                countScanRows.getLabelsAttr(),
                                                                countScanRows.getPropertyAttr(),
                                                                countScanRows.getPropertyScanAttr());

    _valueMap[countScanRows.getResult()] = rows.getResult();
    _innermostCardinality = rows.getResult();
}

void DBLowering::lowerAggregate(mlir::Value input, mlir::Value result, storage::AggregateKind kind, bool distinct) {
    // The nl chunk the aggregated column lowered to; the update folds its per-step
    // non-null values into the accumulator. A constant column is reduced over the
    // rows it stands for, so it is laid out over the driving relation first.
    const mlir::Value alignedChunk = rowAlignedChunk(mapValue(input), rowsInFlight());

    const mlir::Value inputChunk = reducedValueChunk(alignedChunk);

    mlir::MLIRContext* const context = _builder.getContext();
    const mlir::Location loc = _builder.getUnknownLoc();

    // A type-erased column is read as the tagged cell it holds; every other input is a
    // nullable value chunk, and the reduction is resolved from the value type it wraps.
    const mlir::Type inputChunkElement = mlir::cast<nl::ChunkType>(inputChunk.getType()).getElementType();
    const bool taggedCells = mlir::isa<storage::AnyType>(inputChunkElement);
    const mlir::Type inputElement = taggedCells
        ? inputChunkElement
        : mlir::cast<storage::NullableType>(inputChunkElement).getValueType();

    // The accumulator (and result) element type: avg widens numbers to f64, the rest
    // keep the input type. This also validates the reduction against the value type.
    const mlir::Type resultElement = aggregateResultElementType(_builder, kind, inputElement);

    // The accumulator is hoisted to the top of the root block, above every loop of
    // this dataflow, so it is reset once per step of that block and dominates the
    // update in the producing loop body and the emit that reads it after the loop.
    _builder.setInsertionPointToStart(_rootBlock);
    const nl::AggregateStateType stateType = nl::AggregateStateType::get(context, resultElement);
    const mlir::Value state = _builder.create<nl::Aggregate>(loc, stateType, kind).getState();

    // sum(DISTINCT x) folds the survivors of a DISTINCT instead of the raw column, so a
    // value repeated across rows is charged once. A null does survive the filter - all
    // nulls share one key - but the update skips nulls as it always does.
    mlir::Value distinctState;
    if (distinct) {
        distinctState = _builder.create<nl::Distinct>(loc).getState();
    }

    // The update sits in the innermost producing loop body, where the aggregated
    // column is bound (the same block db.output would emit from), and folds each
    // step's non-null values into the accumulator.
    mlir::Block* const producingBlock = accumulatorUpdateBlock(ownerBlock(inputChunk));
    setInsertionInto(producingBlock);

    mlir::Value reducedChunk = inputChunk;
    if (distinctState) {
        nl::DistinctFilter filter = _builder.create<nl::DistinctFilter>(loc, distinctState, mlir::ValueRange {inputChunk});
        reducedChunk = filter.getResults().front();
    }

    _builder.create<nl::AggregateUpdate>(loc, state, reducedChunk, kind);

    // Like db.count, an aggregate is a pipeline breaker that collapses to one row,
    // so it opens no emit loop: nl.aggregate_result materializes the reduced value
    // in place at function scope, right after the producing loop. The result is a
    // single-row nullable value chunk - an aggregate can be null (min/max/avg of no
    // non-null row), and sum rides the same representation.
    const nl::ChunkType resultChunkType = nl::ChunkType::get(context, nullableElementType(resultElement));

    setInsertionAfterProducingLoop(producingBlock);
    nl::AggregateResult aggregateResult = _builder.create<nl::AggregateResult>(loc, resultChunkType, state, kind);

    // The db aggregate's result maps to that chunk, so the db.output that follows
    // lowers into a function-scope nl.output reading it, exactly as db.count does.
    _valueMap[result] = aggregateResult.getResult();
    _innermostCardinality = aggregateResult.getResult();
}

void DBLowering::lowerGroupAggregate(mlir::db::GroupAggregate groupAggregate) {
    const mlir::OperandRange columns = groupAggregate.getColumns();
    const uint64_t keyCount = groupAggregate.getKeyCount();
    const llvm::ArrayRef<int64_t> kinds = groupAggregate.getKinds();

    // The nl chunks the columns lowered to: the grouping keys first, then the
    // aggregate inputs. nl.group_aggregate_update appends these to the per-group
    // state, and the emit loop yields the group rows back.
    llvm::SmallVector<mlir::Value, 4> chunks;
    for (const mlir::Value column : columns) {
        chunks.push_back(mapValue(column));
    }

    // GroupAggregate::verify guarantees keyCount >= 1, kinds.size() >= 1 and
    // columns.size() == keyCount + kinds.size(), so reaching an empty column set
    // here means unverified IR - a defensive backstop, as in lowerSort.
    if (chunks.empty()) {
        throw IRException("db.group_aggregate requires at least one column");
    }

    // The group assignment reads one row of every key per row of the step, which a constant
    // key - one value standing for every row - has none of. The keys are laid out first, so
    // that the aggregate inputs behind them are laid out over a key chunk holding rows.
    rowAlignBufferedChunks(llvm::MutableArrayRef<mlir::Value>(chunks).take_front(keyCount));

    // A constant aggregate input is reduced over the rows of the group it falls in,
    // not over the single row it is, so it is laid out over the chunk the grouping
    // keys are read from - the same rows the group assignment is computed for. A
    // reduction then reads its input as a nullable value chunk, so a column carrying
    // no null is laid out as one; a count reads the chunk it is anchored on as it comes.
    for (size_t inputIndex = keyCount; inputIndex < chunks.size(); inputIndex++) {
        chunks[inputIndex] = rowAlignedChunk(chunks[inputIndex], chunks.front());
    }

    for (size_t aggregateIndex = 0; aggregateIndex < kinds.size(); aggregateIndex++) {
        const auto kind = static_cast<storage::GroupAggregateKind>(kinds[aggregateIndex]);
        const size_t chunkIndex = keyCount + aggregateIndex;

        if (reducesValues(kind)) {
            chunks[chunkIndex] = reducedValueChunk(chunks[chunkIndex]);
        }
    }

    mlir::MLIRContext* const context = _builder.getContext();
    const mlir::Location loc = _builder.getUnknownLoc();

    // The accumulator - with its keyCount / aggregate-kind spec - is hoisted to the
    // top of the root block, above every loop of this dataflow, so the group table
    // exists before the producing loop fills it and the handle dominates the update
    // and the emit loop.
    // The grouped sibling of lowerSort's nl.sort_buffer.
    _builder.setInsertionPointToStart(_rootBlock);
    nl::GroupAggregateBuffer bufferOp = _builder.create<nl::GroupAggregateBuffer>(loc,
                                                                                 keyCount,
                                                                                 groupAggregate.getKindsAttr());
    const mlir::Value state = bufferOp.getState();

    // The update folds each step's chunk of every column into the per-group state.
    // It sits in the innermost producing loop body, where all columns are bound
    // together (the same block db.output would emit from), so the group assignment
    // and the per-group folds stay row-aligned.
    const mlir::Value representative = chunks.front();
    setInsertionInto(ownerBlock(representative));
    _builder.create<nl::GroupAggregateUpdate>(loc, state, chunks);

    // The emit iterator yields one chunk per output column: the grouping-key columns
    // (same chunk types as the key inputs) followed by the aggregate results. A count
    // result is a single non-null unsigned i64 per group; sum/min/max keep the
    // input's value type and avg widens numbers to f64 - all resolved here from each
    // kind and its input chunk.
    llvm::SmallVector<mlir::Type, 4> chunkTypes;
    for (size_t keyIndex = 0; keyIndex < keyCount; keyIndex++) {
        chunkTypes.push_back(chunks[keyIndex].getType());
    }

    const mlir::Type ui64Element = _builder.getIntegerType(64, /*isSigned=*/false);
    const nl::ChunkType countChunkType = nl::ChunkType::get(context, ui64Element);

    for (size_t aggregateIndex = 0; aggregateIndex < kinds.size(); aggregateIndex++) {
        const auto kind = static_cast<storage::GroupAggregateKind>(kinds[aggregateIndex]);
        const mlir::Value inputChunk = chunks[keyCount + aggregateIndex];

        chunkTypes.push_back(groupAggregateResultChunkType(_builder, kind, inputChunk, countChunkType));
    }

    const nl::IteratorType iteratorType = nl::IteratorType::get(context, chunkTypes);

    // The emit phase is an nl.group_aggregate source iterator plus its nl.for, placed
    // after the producing loop (before the func.return) so every row has been folded
    // when the loop first steps. buildLoopForSource binds one loop variable per
    // output column and maps db.group_aggregate's results to them, so the db.output
    // that follows lowers into the emit loop body reading the group rows. As for a
    // sort, this emit loop is the only loop of this aggregation a limit may bound -
    // assignProducerLoops stops at the breaker and hands the handle to it, never to
    // the producing loops, which had to fold every row.
    setInsertionInto(_rootBlock);
    nl::GroupAggregate groupOp = _builder.create<nl::GroupAggregate>(loc, iteratorType, state);
    buildLoopForSource(groupOp.getResult(), groupAggregate.getOperation());
}

void DBLowering::lowerCollect(mlir::db::Collect collect) {
    const mlir::OperandRange columns = collect.getColumns();
    const uint64_t keyCount = collect.getKeyCount();
    const llvm::ArrayRef<int64_t> kinds = collect.getKinds().value_or(llvm::ArrayRef<int64_t> {});

    // The nl chunks the columns lowered to: the grouping keys first, then the collected
    // value columns, then the aggregate inputs. nl.collect_update appends these to the
    // per-group lists.
    llvm::SmallVector<mlir::Value, 4> chunks;
    for (const mlir::Value column : columns) {
        chunks.push_back(mapValue(column));
    }

    // Collect::verify guarantees at least one value column after the keys, so an empty
    // column set here means unverified IR - a defensive backstop, as in
    // lowerGroupAggregate.
    if (chunks.empty()) {
        throw IRException("db.collect requires at least one column");
    }

    const size_t valueCount = chunks.size() - keyCount - kinds.size();

    // The grouping keys are laid out first, as lowerGroupAggregate lays its own: the group
    // assignment reads a row of every key per row of the step, which a constant key has none of.
    rowAlignBufferedChunks(llvm::MutableArrayRef<mlir::Value>(chunks).take_front(keyCount));

    // A constant collected column is folded over the rows of the group it falls in, not
    // over the single row it is, so it is laid out over the chunk the grouping keys are
    // read from - the driving relation when the collect has no keys. The aggregate
    // inputs beside it are laid out the same way, as lowerGroupAggregate lays its own.
    const mlir::Value cardinality = keyCount > 0 ? chunks.front() : rowsInFlight();
    for (size_t inputIndex = keyCount; inputIndex < chunks.size(); inputIndex++) {
        chunks[inputIndex] = rowAlignedChunk(chunks[inputIndex], cardinality);
    }

    for (size_t valueIndex = 0; valueIndex < valueCount; valueIndex++) {
        const size_t chunkIndex = keyCount + valueIndex;
        chunks[chunkIndex] = listItemChunk(chunks[chunkIndex]);
    }

    // A reduction beside the lists reads its input as it does under a grouped
    // aggregation; a count tallies rows and takes the chunk as it is.
    for (size_t aggregateIndex = 0; aggregateIndex < kinds.size(); aggregateIndex++) {
        const auto kind = static_cast<storage::GroupAggregateKind>(kinds[aggregateIndex]);
        const size_t chunkIndex = keyCount + valueCount + aggregateIndex;

        if (reducesValues(kind)) {
            chunks[chunkIndex] = reducedValueChunk(chunks[chunkIndex]);
        }
    }

    const mlir::Location loc = _builder.getUnknownLoc();

    // The accumulator is hoisted to the top of the root block, above every loop of
    // this dataflow, so the group table exists before the producing loop fills it and
    // the handle dominates the update. The collect sibling of lowerGroupAggregate's
    // buffer.
    _builder.setInsertionPointToStart(_rootBlock);
    nl::CollectBuffer bufferOp = _builder.create<nl::CollectBuffer>(loc,
                                                                    keyCount,
                                                                    collect.getKindsAttr(),
                                                                    collect.getDistinctValuesAttr());
    const mlir::Value state = bufferOp.getState();

    // The update folds each step's chunk of every column into the per-group lists. It
    // sits in the innermost producing loop body, where all columns are bound together
    // (the same block db.output would emit from), so the group assignment and the
    // per-group appends stay row-aligned.
    const mlir::Value representative = chunks.front();
    setInsertionInto(accumulatorUpdateBlock(ownerBlock(representative)));
    _builder.create<nl::CollectUpdate>(loc, state, chunks);

    // The emit phase: an nl.collect source iterator yielding one row per group - the
    // key columns then one per-group list cell per collected column - drained by an
    // nl.for after the producing loop. Each list chunk's element is the resolved value
    // type (unwrapped from the collected column's nullable) wrapped in a storage list;
    // the key chunks keep their input types.
    mlir::MLIRContext* const context = _builder.getContext();

    llvm::SmallVector<mlir::Type, 4> chunkTypes;
    for (uint64_t keyIndex = 0; keyIndex < keyCount; keyIndex++) {
        chunkTypes.push_back(chunks[keyIndex].getType());
    }

    for (size_t valueIndex = 0; valueIndex < valueCount; valueIndex++) {
        const mlir::Type valueElement = mlir::cast<nl::ChunkType>(chunks[keyCount + valueIndex].getType()).getElementType();

        mlir::Type listElement = valueElement;
        if (const auto nullable = mlir::dyn_cast<storage::NullableType>(valueElement)) {
            listElement = nullable.getValueType();
        }

        chunkTypes.push_back(nl::ChunkType::get(context, storage::ListType::get(context, listElement)));
    }

    const mlir::Type ui64Element = _builder.getIntegerType(64, /*isSigned=*/false);
    const nl::ChunkType countChunkType = nl::ChunkType::get(context, ui64Element);

    for (size_t aggregateIndex = 0; aggregateIndex < kinds.size(); aggregateIndex++) {
        const storage::GroupAggregateKind kind = static_cast<storage::GroupAggregateKind>(kinds[aggregateIndex]);
        const mlir::Value inputChunk = chunks[keyCount + valueCount + aggregateIndex];

        chunkTypes.push_back(groupAggregateResultChunkType(_builder, kind, inputChunk, countChunkType));
    }

    const nl::IteratorType iteratorType = nl::IteratorType::get(context, chunkTypes);

    setInsertionInto(_rootBlock);
    nl::Collect collectOp = _builder.create<nl::Collect>(loc, iteratorType, state);
    buildLoopForSource(collectOp.getResult(), collect.getOperation());
}

void DBLowering::lowerUnwindCollect(mlir::db::UnwindCollect unwindCollect) {
    const mlir::OperandRange columns = unwindCollect.getColumns();
    const uint64_t keyCount = unwindCollect.getKeyCount();

    // The nl chunks the columns lowered to: the grouping keys first, then the single
    // collected value column.
    llvm::SmallVector<mlir::Value, 4> chunks;
    for (const mlir::Value column : columns) {
        chunks.push_back(mapValue(column));
    }

    // UnwindCollect::verify guarantees columns.size() == keyCount + 1, so an empty
    // column set here means unverified IR - a defensive backstop.
    if (chunks.empty()) {
        throw IRException("db.unwind_collect requires at least one column");
    }

    mlir::MLIRContext* const context = _builder.getContext();
    const mlir::Location loc = _builder.getUnknownLoc();

    // The accumulate phase is identical to lowerCollect: a hoisted nl.collect_buffer
    // and an nl.collect_update in the producing loop body.
    _builder.setInsertionPointToStart(_rootBlock);
    nl::CollectBuffer bufferOp = _builder.create<nl::CollectBuffer>(loc,
                                                                    keyCount,
                                                                    mlir::DenseI64ArrayAttr {},
                                                                    mlir::DenseI64ArrayAttr {});
    const mlir::Value state = bufferOp.getState();

    const mlir::Value representative = chunks.front();
    setInsertionInto(accumulatorUpdateBlock(ownerBlock(representative)));
    _builder.create<nl::CollectUpdate>(loc, state, chunks);

    // The emit phase: an nl.unwind_collect source iterator yielding one row per element - the
    // key columns then the unwound value - drained by an nl.for. The value chunk keeps
    // the collected column's type (a nullable value chunk); the keys keep theirs.
    llvm::SmallVector<mlir::Type, 4> chunkTypes;
    for (uint64_t keyIndex = 0; keyIndex < keyCount; keyIndex++) {
        chunkTypes.push_back(chunks[keyIndex].getType());
    }

    chunkTypes.push_back(chunks[keyCount].getType());

    const nl::IteratorType iteratorType = nl::IteratorType::get(context, chunkTypes);

    setInsertionInto(_rootBlock);
    nl::UnwindCollect unwindCollectOp = _builder.create<nl::UnwindCollect>(loc, iteratorType, state);
    buildLoopForSource(unwindCollectOp.getResult(), unwindCollect.getOperation());
}

void DBLowering::foldBroadcastsIntoOutputs(mlir::func::FuncOp nlFunction) {
    llvm::SmallVector<nl::Output, 4> foldable;

    nlFunction.walk([&foldable](nl::Output output) {
        const mlir::OperandRange columns = output.getColumns();
        if (output.getCardinality() || columns.empty()) {
            return;
        }

        mlir::Value sharedCardinality;
        for (const mlir::Value column : columns) {
            nl::BroadcastConstant broadcast = column.getDefiningOp<nl::BroadcastConstant>();
            if (!broadcast || !column.hasOneUse()) {
                return;
            }

            const mlir::Value cardinality = broadcast.getCardinality();
            if (!cardinality) {
                return;
            } else if (!sharedCardinality) {
                sharedCardinality = cardinality;
            } else if (cardinality != sharedCardinality) {
                return;
            }
        }

        foldable.push_back(output);
    });

    for (nl::Output output : foldable) {
        llvm::SmallVector<mlir::Value, 4> constants;
        llvm::SmallVector<nl::BroadcastConstant, 4> broadcasts;
        for (const mlir::Value column : output.getColumns()) {
            nl::BroadcastConstant broadcast = column.getDefiningOp<nl::BroadcastConstant>();
            constants.push_back(broadcast.getValue());
            broadcasts.push_back(broadcast);
        }

        _builder.setInsertionPoint(output);
        _builder.create<nl::Output>(output.getLoc(),
                                    constants,
                                    output.getLimit(),
                                    output.getSkip(),
                                    broadcasts.front().getCardinality(),
                                    output.getColumnNamesAttr());

        eraseOp(output);
        for (nl::BroadcastConstant broadcast : broadcasts) {
            eraseOp(broadcast);
        }
    }
}

mlir::Value DBLowering::propertyWriteChunk(llvm::StringRef propertyName, mlir::Value chunk) {
    if (!_view || !_view->isValid()) {
        return chunk;
    }

    // if the property type is a double in the db, but an integer was provided, convert
    // that int to double
    const GraphMetadata& metadata = _view->metadata();
    const PropertyTypeMap& propTypes = metadata.propTypes();
    const std::optional<PropertyType> propertyType = propTypes.get(propertyName);
    if (!propertyType || propertyType->_valueType != ValueType::Double) {
        return chunk;
    }

    const mlir::Type element = chunkValueElement(_builder, chunk.getType());
    const auto integerElement = mlir::dyn_cast<mlir::IntegerType>(element);
    if (!integerElement || integerElement.getWidth() != 64) {
        return chunk;
    }

    return chunkAsElement(chunk, _builder.getF64Type());
}

void DBLowering::coercePropertyWriteChunks(mlir::ArrayAttr names,
                                           llvm::SmallVectorImpl<mlir::Value>& chunks) {
    for (size_t index = 0; index < chunks.size(); index++) {
        const llvm::StringRef name = mlir::cast<mlir::StringAttr>(names[index]).getValue();

        chunks[index] = propertyWriteChunk(name, chunks[index]);
    }
}

void DBLowering::coerceGroupedPropertyWriteChunks(mlir::ArrayAttr groupedNames,
                                                  llvm::SmallVectorImpl<mlir::Value>& chunks) {
    size_t index = 0;

    for (const mlir::Attribute group : groupedNames) {
        for (const mlir::Attribute name : mlir::cast<mlir::ArrayAttr>(group)) {
            chunks[index] = propertyWriteChunk(mlir::cast<mlir::StringAttr>(name).getValue(), chunks[index]);
            index++;
        }
    }
}

void DBLowering::lowerCreateNode(mlir::db::CreateNode createNode) {
    const mlir::Location loc = _builder.getUnknownLoc();

    llvm::SmallVector<mlir::Value, 4> propChunks;
    for (const mlir::Value propValue : createNode.getPropValues()) {
        propChunks.push_back(mapValue(propValue));
    }

    coercePropertyWriteChunks(createNode.getPropNames(), propChunks);

    // A constant stands for the rows in flight, which is how many nodes the op writes
    mlir::Value cardinalityChunk;
    if (createNode.getCardinality()) {
        cardinalityChunk = mapValue(createNode.getCardinality());

        const mlir::Value rows = rowsInFlight();
        if (rows) {
            cardinalityChunk = rowAlignedChunk(cardinalityChunk, rows);
        }
    }

    if (cardinalityChunk) {
        setInsertionInto(ownerBlock(cardinalityChunk));
    } else {
        mlir::Block* targetBlock = _rootBlock;
        for (const mlir::OpOperand& use : createNode.getResult().getUses()) {
            auto createEdge = mlir::dyn_cast<mlir::db::CreateEdge>(use.getOwner());
            if (!createEdge) {
                continue;
            }
            for (const mlir::Value candidate : {createEdge.getSrcIds(), createEdge.getTgtIds()}) {
                if (candidate == createNode.getResult()) {
                    continue;
                }
                const auto it = _valueMap.find(candidate);
                if (it == _valueMap.end()) {
                    continue;
                }
                mlir::Block* const candidateBlock = ownerBlock(it->second);
                if (candidateBlock != _rootBlock) {
                    targetBlock = candidateBlock;
                    break;
                }
            }
            if (targetBlock != _rootBlock) {
                break;
            }
        }

        setInsertionInto(targetBlock);
    }

    nl::CreateNode create = _builder.create<nl::CreateNode>(
        loc,
        createNode.getLabelsAttr(),
        createNode.getPropNamesAttr(),
        propChunks,
        cardinalityChunk);
    _valueMap[createNode.getResult()] = create.getResult();
}

void DBLowering::lowerCreateEdge(mlir::db::CreateEdge createEdge) {
    const mlir::Location loc = _builder.getUnknownLoc();
    const mlir::Value srcChunk = mapValue(createEdge.getSrcIds());
    const mlir::Value tgtChunk = mapValue(createEdge.getTgtIds());

    llvm::SmallVector<mlir::Value, 4> propChunks;
    for (const mlir::Value propValue : createEdge.getPropValues()) {
        propChunks.push_back(mapValue(propValue));
    }

    coercePropertyWriteChunks(createEdge.getPropNames(), propChunks);

    llvm::SmallVector<mlir::Value, 8> operandChunks {srcChunk, tgtChunk};
    operandChunks.append(propChunks.begin(), propChunks.end());

    const mlir::Value reference = deepestBoundChunk(operandChunks);
    setInsertionInto(ownerBlock(reference));

    nl::CreateEdge create = _builder.create<nl::CreateEdge>(
        loc,
        srcChunk,
        tgtChunk,
        createEdge.getEdgeTypeAttr(),
        createEdge.getPropNamesAttr(),
        propChunks,
        mapOptionalMask(createEdge.getSrcPending()),
        mapOptionalMask(createEdge.getTgtPending()),
        createEdge.getSrcAllPending(),
        createEdge.getTgtAllPending());
    _valueMap[createEdge.getResult()] = create.getResult();
}

mlir::Value DBLowering::mapOptionalMask(mlir::Value mask) {
    if (!mask) {
        return mlir::Value();
    }

    return pendingMaskChunk(mapValue(mask));
}

// An OPTIONAL CALL pads the mask of a merged entity it returns with null, and the row it
// pads names no entity, so none this change wrote
mlir::Value DBLowering::pendingMaskChunk(mlir::Value chunk) {
    if (!isNullableChunk(chunk.getType())) {
        return chunk;
    }

    const mlir::Location loc = _builder.getUnknownLoc();
    mlir::MLIRContext* const context = _builder.getContext();
    const nl::ChunkType maskType = nl::ChunkType::get(context, storage::BoolType::get(context));

    const mlir::Value notPending = _builder.create<nl::IsNotTrue>(loc, maskType, chunk).getResult();
    return _builder.create<nl::Not>(loc, maskType, notPending).getResult();
}

void DBLowering::mapColumns(mlir::OperandRange columns, llvm::SmallVectorImpl<mlir::Value>& chunks) {
    chunks.clear();
    for (const mlir::Value column : columns) {
        chunks.push_back(mapValue(column));
    }
}

void DBLowering::lowerMerge(mlir::db::Merge merge) {
    llvm::SmallVector<mlir::Value, 8> boundNodes;
    llvm::SmallVector<mlir::Value, 8> boundPending;
    llvm::SmallVector<mlir::Value, 8> nodePropValues;
    llvm::SmallVector<mlir::Value, 8> edgePropValues;
    llvm::SmallVector<mlir::Value, 8> carriedColumns;

    mapColumns(merge.getBoundNodes(), boundNodes);
    mapColumns(merge.getBoundPending(), boundPending);

    for (mlir::Value& pending : boundPending) {
        pending = rowAlignedChunk(pending, boundNodes.front());
    }
    mapColumns(merge.getNodePropValues(), nodePropValues);
    mapColumns(merge.getEdgePropValues(), edgePropValues);
    mapColumns(merge.getCarriedColumns(), carriedColumns);

    coerceGroupedPropertyWriteChunks(merge.getNodePropNames(), nodePropValues);
    coerceGroupedPropertyWriteChunks(merge.getEdgePropNames(), edgePropValues);

    llvm::SmallVector<mlir::Value, 8> operandChunks(boundNodes);
    operandChunks.append(boundPending.begin(), boundPending.end());
    operandChunks.append(nodePropValues.begin(), nodePropValues.end());
    operandChunks.append(edgePropValues.begin(), edgePropValues.end());
    operandChunks.append(carriedColumns.begin(), carriedColumns.end());

    // Inserted into the deepest block, where all the operands are defined. A merge whose
    // operands are all bound above the root - literals alone - opens in the root, as a
    // CREATE does, so a subquery branch that is not taken merges nothing.
    mlir::Block* targetBlock = _rootBlock;
    if (const mlir::Value insertionReference = deepestBoundChunk(operandChunks)) {
        mlir::Block* const referenceBlock = ownerBlock(insertionReference);
        if (enclosesBlock(_rootBlock, referenceBlock)) {
            targetBlock = referenceBlock;
        }
    }

    setInsertionInto(targetBlock);

    for (mlir::Value& pending : boundPending) {
        pending = pendingMaskChunk(pending);
    }

    mlir::MLIRContext* const context = _builder.getContext();
    const mlir::Type nodeChunkType = nl::ChunkType::get(context, storage::NodeIDType::get(context));
    const mlir::Type edgeChunkType = nl::ChunkType::get(context, storage::EdgeIDType::get(context));
    const mlir::Type maskChunkType = nl::ChunkType::get(context, storage::BoolType::get(context));

    const mlir::ArrayAttr nodeLabels = merge.getNodeLabels();
    const size_t hopCount = nodeLabels.size() - 1;
    const size_t matchedNodeCount = mlir::mergeMatchedNodeCount(nodeLabels);

    llvm::SmallVector<mlir::Type, 8> resultTypes;
    for (size_t nodeIndex = 0; nodeIndex < matchedNodeCount; nodeIndex++) {
        resultTypes.push_back(nodeChunkType);
        resultTypes.push_back(maskChunkType);
    }

    for (size_t hopIndex = 0; hopIndex < hopCount; hopIndex++) {
        resultTypes.push_back(edgeChunkType);
        resultTypes.push_back(maskChunkType);
    }

    resultTypes.push_back(maskChunkType);

    for (const mlir::Value carriedChunk : carriedColumns) {
        resultTypes.push_back(carriedChunk.getType());
    }

    nl::Merge nlMerge = _builder.create<nl::Merge>(_builder.getUnknownLoc(),
                                                   resultTypes,
                                                   merge.getNodeLabelsAttr(),
                                                   merge.getNodePropNamesAttr(),
                                                   merge.getEdgeTypesAttr(),
                                                   merge.getEdgePropNamesAttr(),
                                                   merge.getEdgeDirectionsAttr(),
                                                   merge.getPendingNodesAttr(),
                                                   boundNodes,
                                                   boundPending,
                                                   nodePropValues,
                                                   edgePropValues,
                                                   carriedColumns);

    const mlir::ResultRange dbResults = merge.getResults();
    const mlir::ResultRange nlResults = nlMerge.getResults();
    for (size_t index = 0; index < dbResults.size(); index++) {
        _valueMap[dbResults[index]] = nlResults[index];
    }

    // The merge grows or shrinks the rows in flight, so from here on what sizes a
    // projection of constants alone is its own node chunk
    _innermostCardinality = nlResults.front();
}

void DBLowering::lowerSetNodeProperty(mlir::db::SetNodeProperty setNodeProperty) {
    const mlir::Location loc = _builder.getUnknownLoc();
    const mlir::Value inputChunk = mapValue(setNodeProperty.getInputNodes());
    const mlir::Value valueChunk = propertyWriteChunk(setNodeProperty.getProperty(),
                                                      mapValue(setNodeProperty.getValue()));

    setWriteInsertion(inputChunk, valueChunk);

    _builder.create<nl::SetNodeProperty>(
        loc,
        inputChunk,
        setNodeProperty.getPropertyAttr(),
        valueChunk,
        mapOptionalMask(setNodeProperty.getPending()),
        mapOptionalMask(setNodeProperty.getRows()),
        setNodeProperty.getAllPending());
}

void DBLowering::lowerSetEdgeProperty(mlir::db::SetEdgeProperty setEdgeProperty) {
    const mlir::Location loc = _builder.getUnknownLoc();
    const mlir::Value inputChunk = mapValue(setEdgeProperty.getInputEdges());
    const mlir::Value valueChunk = propertyWriteChunk(setEdgeProperty.getProperty(),
                                                      mapValue(setEdgeProperty.getValue()));

    setWriteInsertion(inputChunk, valueChunk);

    _builder.create<nl::SetEdgeProperty>(
        loc,
        inputChunk,
        setEdgeProperty.getPropertyAttr(),
        valueChunk,
        mapOptionalMask(setEdgeProperty.getPending()),
        mapOptionalMask(setEdgeProperty.getRows()),
        setEdgeProperty.getAllPending());
}

template <typename NLSetProperties, typename DBSetProperties>
void DBLowering::lowerSetProperties(DBSetProperties setProperties, mlir::Value input) {
    const mlir::Location loc = _builder.getUnknownLoc();
    const mlir::Value inputChunk = mapValue(input);
    const mlir::Value valueChunk = mapValue(setProperties.getValue());

    setWriteInsertion(inputChunk, valueChunk);

    _builder.create<NLSetProperties>(loc,
                                     inputChunk,
                                     valueChunk,
                                     mapOptionalMask(setProperties.getPending()),
                                     mapOptionalMask(setProperties.getRows()),
                                     setProperties.getAllPending(),
                                     setProperties.getReplaces(),
                                     setProperties.getCreatedTypesAttr());
}

void DBLowering::lowerDeleteNode(mlir::db::DeleteNode deleteNode) {
    const mlir::Location loc = _builder.getUnknownLoc();
    const mlir::Value inputChunk = mapValue(deleteNode.getInputNodes());

    // The delete runs where its node chunk is live - the block that owns it.
    setInsertionInto(ownerBlock(inputChunk));

    _builder.create<nl::DeleteNode>(loc,
                                    inputChunk,
                                    deleteNode.getDetach(),
                                    mapOptionalMask(deleteNode.getPending()),
                                    deleteNode.getAllPending());
}

void DBLowering::lowerDeleteEdge(mlir::db::DeleteEdge deleteEdge) {
    const mlir::Location loc = _builder.getUnknownLoc();
    const mlir::Value inputChunk = mapValue(deleteEdge.getInputEdges());

    setInsertionInto(ownerBlock(inputChunk));

    _builder.create<nl::DeleteEdge>(loc,
                                    inputChunk,
                                    mapOptionalMask(deleteEdge.getPending()),
                                    deleteEdge.getAllPending());
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

void DBLowering::lowerCurrentDateTime(mlir::db::CurrentDateTime currentDateTime) {
    setInsertionAfterHoisted();

    const nl::ChunkType resultType
        = nl::ChunkType::get(_builder.getContext(), storage::DateTimeType::get(_builder.getContext()));

    nl::CurrentDateTime lowered
        = _builder.create<nl::CurrentDateTime>(_builder.getUnknownLoc(), resultType);

    _valueMap[currentDateTime.getResult()] = lowered.getResult();
    _lastHoisted = lowered.getOperation();
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

template <typename NLOp>
void DBLowering::lowerBinaryOp(mlir::Operation& op, BinaryResultKind kind) {
    mlir::Value lhsChunk = mapValue(op.getOperand(0));
    mlir::Value rhsChunk = mapValue(op.getOperand(1));

    const mlir::Type lhsChunkType = lhsChunk.getType();
    const mlir::Type rhsChunkType = rhsChunk.getType();

    const bool readsScalarOperands = kind != BinaryResultKind::Index
                                  && kind != BinaryResultKind::Membership
                                  && kind != BinaryResultKind::Position;

    const bool lhsReadsItsOwnNull = mayHoldANull(lhsChunkType);
    const bool rhsReadsItsOwnNull = mayHoldANull(rhsChunkType);

    const bool nullAgainstRhs = readsScalarOperands
                             && isUntypedNullChunk(rhsChunkType)
                             && !lhsReadsItsOwnNull;

    const bool nullAgainstLhs = readsScalarOperands
                             && isUntypedNullChunk(lhsChunkType)
                             && !rhsReadsItsOwnNull;

    // The null literal meets kernels reading a nullable column on its other side, as a
    // logical operator takes it as an unknown truth value
    if (nullAgainstRhs) {
        lhsChunk = nullableValueChunk(lhsChunk);
    } else if (nullAgainstLhs) {
        rhsChunk = nullableValueChunk(rhsChunk);
    }

    // AND, OR and XOR read a cell's truth value against a nullable boolean
    constexpr bool CONNECTS_TRUTH_VALUES = std::is_same_v<NLOp, nl::And>
                                      || std::is_same_v<NLOp, nl::Or>
                                      || std::is_same_v<NLOp, nl::Xor>;

    if constexpr (CONNECTS_TRUTH_VALUES) {
        const bool lhsHoldsCells = isTaggedCellChunk(lhsChunkType);
        const bool rhsHoldsCells = isTaggedCellChunk(rhsChunkType);

        if (lhsHoldsCells && !rhsHoldsCells && !isUntypedNullChunk(rhsChunkType)) {
            rhsChunk = nullableValueChunk(rhsChunk);
        } else if (rhsHoldsCells && !lhsHoldsCells && !isUntypedNullChunk(lhsChunkType)) {
            lhsChunk = nullableValueChunk(lhsChunk);
        }
    }

    if constexpr (CONNECTS_TRUTH_VALUES) {
        lhsChunk = laidOutNullAs(lhsChunk, _builder.getI1Type());
        rhsChunk = laidOutNullAs(rhsChunk, _builder.getI1Type());
    } else if (readsScalarOperands) {
        const mlir::Type lhsElement = chunkValueElement(_builder, lhsChunk.getType());
        const mlir::Type rhsElement = chunkValueElement(_builder, rhsChunk.getType());

        lhsChunk = laidOutNullAs(lhsChunk, rhsElement);
        rhsChunk = laidOutNullAs(rhsChunk, lhsElement);
    }

    const mlir::Type resultElement = binaryResultElement(kind, lhsChunk.getType(), rhsChunk.getType());
    const nl::ChunkType resultType = nl::ChunkType::get(_builder.getContext(), resultElement);

    setInsertionForBinaryOp(lhsChunk, rhsChunk);

    NLOp nlOp = _builder.create<NLOp>(_builder.getUnknownLoc(), resultType, lhsChunk, rhsChunk);
    _valueMap[op.getResult(0)] = nlOp.getResult();
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

template <typename NLOp>
void DBLowering::lowerNullTest(mlir::Value operand, mlir::Value result) {
    const mlir::Value operandChunk = mapValue(operand);

    mlir::MLIRContext* const context = _builder.getContext();
    const nl::ChunkType resultType = nl::ChunkType::get(context, storage::BoolType::get(context));

    setInsertionForUnaryOp(operandChunk);

    NLOp test = _builder.create<NLOp>(_builder.getUnknownLoc(), resultType, operandChunk);
    _valueMap[result] = test.getResult();
}

void DBLowering::lowerUnaryFunction(mlir::Operation* op) {
    const UnaryFunctionLowering* spec = lookupUnaryFunctionLowering(*op);
    bioassert(spec, "lowerUnaryFunction called on a non-function op");

    const mlir::Value inputChunk = mapValue(op->getOperand(0));

    const mlir::Type inputElement = mlir::cast<nl::ChunkType>(inputChunk.getType()).getElementType();
    const auto inputNullableElement = mlir::dyn_cast<storage::NullableType>(inputElement);
    const mlir::Type inputValueElement = inputNullableElement ? inputNullableElement.getValueType() : inputElement;

    const mlir::Type baseElement = unaryFunctionElement(op, inputValueElement);
    if (!baseElement) {
        const mlir::OperationName name = op->getName();
        throw IRException(fmt::format("{} reads no column of this element type", name.getStringRef().str()));
    }

    const bool inputCanBeNull = mayHoldANull(inputChunk.getType());

    const bool alwaysNull = spec->nullability == ResultNullability::AlwaysNullable;
    const bool specNull = spec->nullability == ResultNullability::FollowsInput && inputCanBeNull;
    const bool resultNullable = alwaysNull || specNull;

    mlir::Type resultElement = baseElement;
    if (resultNullable) {
        resultElement = nullableElementType(baseElement);
    }

    const nl::ChunkType resultType = nl::ChunkType::get(_builder.getContext(), resultElement);

    setInsertionForUnaryOp(inputChunk);

    _valueMap[op->getResult(0)] = spec->emit(_builder, _builder.getUnknownLoc(), resultType, inputChunk, op);
}

void DBLowering::lowerHeadOrLast(mlir::Operation& operation) {
    const mlir::Value listChunk = mapValue(operation.getOperand(0));
    const mlir::Type listChunkType = listChunk.getType();

    const bool listsTypedElements = isListChunk(listChunkType)
                                 && !mlir::isa<storage::AnyType, mlir::NoneType>(indexedListElementType(listChunkType));
    if (!listsTypedElements) {
        lowerUnaryFunction(&operation);
        return;
    }

    const int64_t position = mlir::isa<mlir::db::Head>(operation) ? 0 : -1;

    setInsertionAfterHoisted();
    nl::Constant positionConstant = _builder.create<nl::Constant>(_builder.getUnknownLoc(), _builder.getI64IntegerAttr(position));
    _lastHoisted = positionConstant.getOperation();

    const mlir::Value positionChunk = positionConstant.getResult();
    const mlir::Type resultElement = binaryResultElement(BinaryResultKind::Index, listChunkType, positionChunk.getType());
    const nl::ChunkType resultType = nl::ChunkType::get(_builder.getContext(), resultElement);

    setInsertionForBinaryOp(listChunk, positionChunk);

    nl::ListIndex element = _builder.create<nl::ListIndex>(_builder.getUnknownLoc(), resultType, listChunk, positionChunk);
    _valueMap[operation.getResult(0)] = element.getResult();
}

void DBLowering::lowerBinaryFunction(mlir::Operation* op) {
    const BinaryFunctionLowering* spec = lookupBinaryFunctionLowering(*op);
    bioassert(spec, "lowerBinaryFunction called on a non-function op");

    const mlir::Value lhsChunk = mapValue(op->getOperand(0));
    const mlir::Value rhsChunk = mapValue(op->getOperand(1));

    const mlir::Type lhsElement = mlir::cast<nl::ChunkType>(lhsChunk.getType()).getElementType();
    const mlir::Type baseElement = spec->element(_builder, lhsElement);

    mlir::Type resultElement = baseElement;
    if (mayHoldANull(lhsChunk.getType()) || mayHoldANull(rhsChunk.getType())) {
        resultElement = nullableElementType(baseElement);
    }

    const nl::ChunkType resultType = nl::ChunkType::get(_builder.getContext(), resultElement);

    setInsertionForBinaryOp(lhsChunk, rhsChunk);

    _valueMap[op->getResult(0)] = spec->emit(_builder, _builder.getUnknownLoc(), resultType, lhsChunk, rhsChunk);
}

void DBLowering::lowerFilter(mlir::db::FilterOp filter) {
    llvm::SmallVector<mlir::Value, 4> columnChunks;
    llvm::SmallVector<mlir::Type, 4> resultTypes;
    for (const mlir::Value column : filter.getColumnsToFilter()) {
        const mlir::Value columnChunk = mapValue(column);
        const mlir::Type chunkType = columnChunk.getType();

        columnChunks.push_back(columnChunk);
        resultTypes.push_back(chunkType);
    }

    // A predicate over constants alone holds one value standing for every row, and the cut
    // reads a mask row by row: it is laid out over the rows of the columns it cuts, so the
    // mask has exactly as many rows as they do - the driving relation's when a column of
    // it is what the cut carries, and the single row a projection of constants is when
    // nothing drives it.
    const mlir::Value maskDriver = cardinalityDriver(columnChunks);
    const mlir::Value maskChunk = rowAlignedChunk(mapValue(filter.getMask()), maskDriver);

    // Inserted into the deepest block, where all operands are defined
    mlir::Value insertionReference = maskChunk;
    for (const mlir::Value columnChunk : columnChunks) {
        mlir::Block* const block = deeperBlock(insertionReference, columnChunk);
        if (ownerBlock(columnChunk) == block) {
            insertionReference = columnChunk;
        }
    }

    setInsertionInto(ownerBlock(insertionReference));

    const mlir::Location uloc = _builder.getUnknownLoc();
    nl::Filter nlFilter = _builder.create<nl::Filter>(uloc, resultTypes, maskChunk, columnChunks);

    const mlir::ResultRange filteredColumns = filter.getFilteredColumns();
    for (size_t columnIndex = 0; columnIndex < filteredColumns.size(); columnIndex++) {
        const mlir::Value filteredCol = filteredColumns[columnIndex];
        const mlir::Value outputChunk = nlFilter.getResult(columnIndex);
        _valueMap[filteredCol] = outputChunk;
    }

    followCardinalityThrough(columnChunks, nlFilter.getResults());
}

void DBLowering::lowerEntityIDOf(mlir::db::EntityIDOf entityIDOf) {
    const mlir::Value inputChunk = mapValue(entityIDOf.getInput());
    const mlir::db::ColumnType resultColumn = mlir::cast<mlir::db::ColumnType>(entityIDOf.getType());
    const nl::ChunkType resultType = nl::ChunkType::get(_builder.getContext(), resultColumn.getType());

    setInsertionForUnaryOp(inputChunk);

    nl::EntityIDOf nlEntityIDOf = _builder.create<nl::EntityIDOf>(_builder.getUnknownLoc(), resultType, inputChunk);
    _valueMap[entityIDOf.getResult()] = nlEntityIDOf.getResult();
}

nl::Output DBLowering::lowerOutput(mlir::db::Output output) {
    llvm::SmallVector<mlir::Value, 4> columns;
    for (const mlir::Value column : output.getColumns()) {
        columns.push_back(mapValue(column));
    }

    if (columns.empty()) {
        throw IRException("db.output requires at least one column");
    }

    // nl.output is emitted limit-oblivious: when a db.limit governs these columns,
    // the columns mapped here are the truncated chunks (lowerLimit remapped
    // db.limit's results to the truncate's), so the chunk's own row count is the
    // budget-capped count. foldTruncatesIntoOutputs later rewrites the terminal
    // case - where the truncate feeds only this output - into nl.output ... limit,
    // dropping the copy.

    // If there is a column which is produced by a block which is not the entry block
    // (e.g. a loop block), then set the anchor to be that block. Otherwise no relation
    // drives the projection, as in RETURN 1, and the output is the single row it holds.
    // Getting the owner block of the returned column is sufficient because in nested
    // loops, each Cypher variable is redefined each op due to the carry set implictly
    // filtering. Otherwise we would need to check for the *deepest* block of all
    // returned values.
    mlir::Block* anchorBlock = _entryBlock;
    for (const mlir::Value column : columns) {
        mlir::Block* const columnBlock = ownerBlock(column);
        if (columnBlock != _entryBlock) {
            anchorBlock = columnBlock;
            break;
        }
    }

    setInsertionInto(anchorBlock);

    return _builder.create<nl::Output>(_builder.getUnknownLoc(),
                                       columns,
                                       mlir::Value(),
                                       mlir::Value(),
                                       mlir::Value(),
                                       output.getColumnNamesAttr());
}

mlir::Value DBLowering::mapValue(mlir::Value dbValue) const {
    const auto slotIt = _valueMap.find(dbValue);
    if (slotIt == _valueMap.end()) {
        throw IRException("db column used before the operation that produces it");
    }

    return slotIt->second;
}

// A type-erased column of tagged cells - what a list mixing types, holding a null or
// holding nothing unwinds into - is read as it stands, every cell carrying its own tag. A
// list or a map, which min and max order as a whole, rides a nullable column of its own type.
mlir::Value DBLowering::reducedValueChunk(mlir::Value chunk) {
    const mlir::Type element = mlir::cast<nl::ChunkType>(chunk.getType()).getElementType();

    if (mlir::isa<storage::AnyType>(element)) {
        return chunk;
    } else if (mlir::isa<storage::ListType, storage::MapType>(element)) {
        return toNullableChunk(chunk, element);
    } else {
        return nullableValueChunk(chunk);
    }
}

// The one column a union result carries, whichever branch filled it. A count is an
// unsigned tally that is never null and a property a nullable signed value, and the two
// are one Cypher INTEGER, so a scalar reaches the column as the nullable signed form. An
// entity, a path, a list and an embedding have one spelling already and are left alone.
mlir::Value DBLowering::unionColumnChunk(mlir::Value chunk) {
    const mlir::Type element = chunkValueElement(_builder, chunk.getType());

    const bool isString = mlir::isa<storage::StringType>(element);
    const bool isDouble = mlir::isa<mlir::Float64Type>(element);
    const mlir::IntegerType integerType = mlir::dyn_cast<mlir::IntegerType>(element);

    if (!isString && !isDouble && !integerType) {
        return chunk;
    }

    // Wrapped before it is converted, and not instead: the kernel a conversion selects
    // reads the column it was handed, so converting a plain chunk would allocate a plain
    // column under a type naming a nullable one
    const mlir::Value nullableChunk = nullableValueChunk(chunk);

    const bool isUnsignedInteger = integerType && integerType.isUnsigned() && integerType.getWidth() == 64;
    if (!isUnsignedInteger) {
        return nullableChunk;
    }

    mlir::MLIRContext* const context = _builder.getContext();
    const storage::NullableType signedElement = storage::NullableType::get(context, _builder.getIntegerType(64));
    const nl::ChunkType signedChunk = nl::ChunkType::get(context, signedElement);

    mlir::OpBuilder::InsertionGuard guard(_builder);
    setInsertionForUnaryOp(nullableChunk);

    return _builder.create<nl::ToInteger>(_builder.getUnknownLoc(), signedChunk, nullableChunk).getResult();
}
