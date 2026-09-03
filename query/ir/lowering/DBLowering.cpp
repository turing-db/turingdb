#include "DBLowering.h"

#include <algorithm>
#include <mlir/IR/Location.h>
#include <optional>
#include <string_view>
#include <unordered_map>

#include "mlir/IR/Block.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/Types.h"
#include "mlir/IR/Verifier.h"

#include "NLOps.h"

#include "IRConstantColumn.h"
#include "IRRowAlignment.h"
#include "Procedure.h"
#include "ProcedureManager.h"
#include "ProcedureTypeVector.h"

#include "views/GraphView.h"
#include "metadata/GraphMetadata.h"
#include "metadata/LabelSet.h"
#include "metadata/LabelSetHandle.h"
#include "metadata/LabelSetMap.h"
#include "metadata/LabelMap.h"
#include "metadata/PropertyType.h"

#include "IRException.h"
#include "llvm/Support/Casting.h"

using namespace db;

namespace nl = mlir::nl;
namespace storage = mlir::storage;

namespace {

using NLUnaryFunctionEmitter = mlir::Value (*)(mlir::OpBuilder& builder,
                                               mlir::Location loc,
                                               nl::ChunkType resultType,
                                               mlir::Value input);
using UnaryFunctionElement = mlir::Type (*)(mlir::OpBuilder& builder);

template <typename NLOp>
mlir::Value emitNLUnaryFunction(mlir::OpBuilder& builder,
                                mlir::Location loc,
                                nl::ChunkType resultType,
                                mlir::Value input) {
    return builder.create<NLOp>(loc, resultType, input).getResult();
}

mlir::Type ownedStringFunctionElement(mlir::OpBuilder& builder) {
    return storage::OwnedStringType::get(builder.getContext());
}

mlir::Type integerFunctionElement(mlir::OpBuilder& builder) {
    return builder.getI64Type();
}

mlir::Type floatFunctionElement(mlir::OpBuilder& builder) {
    return builder.getF64Type();
}

mlir::Type booleanFunctionElement(mlir::OpBuilder& builder) {
    return builder.getI1Type();
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
};

enum class ResultNullability {
    FollowsInput,
    AlwaysNullable,
};

struct UnaryFunctionLowering {
    NLUnaryFunctionEmitter emit {nullptr};
    UnaryFunctionElement element {nullptr};
    ResultNullability nullability {ResultNullability::FollowsInput};
};

const std::unordered_map<std::string_view, UnaryFunctionLowering> unaryFunctionLowerings = {
    {"db.labels",     {&emitNLUnaryFunction<nl::Labels>,    &ownedStringFunctionElement, ResultNullability::FollowsInput}},
    {"db.edge_type",  {&emitNLUnaryFunction<nl::EdgeType>,  &ownedStringFunctionElement, ResultNullability::FollowsInput}},
    {"db.to_integer", {&emitNLUnaryFunction<nl::ToInteger>, &integerFunctionElement,     ResultNullability::AlwaysNullable}},
    {"db.to_float",   {&emitNLUnaryFunction<nl::ToFloat>,   &floatFunctionElement,       ResultNullability::AlwaysNullable}},
    {"db.to_boolean", {&emitNLUnaryFunction<nl::ToBoolean>, &booleanFunctionElement,     ResultNullability::AlwaysNullable}},
};

const UnaryFunctionLowering* lookupUnaryFunctionLowering(mlir::Operation& operation) {
    const llvm::StringRef name = operation.getName().getStringRef();
    const auto it = unaryFunctionLowerings.find(std::string_view(name.data(), name.size()));
    return it == unaryFunctionLowerings.end() ? nullptr : &it->second;
}

using NLBinaryFunctionEmitter = mlir::Value (*)(mlir::OpBuilder& builder,
                                                mlir::Location loc,
                                                nl::ChunkType resultType,
                                                mlir::Value lhs,
                                                mlir::Value rhs);

template <typename NLOp>
mlir::Value emitNLBinaryFunction(mlir::OpBuilder& builder,
                                 mlir::Location loc,
                                 nl::ChunkType resultType,
                                 mlir::Value lhs,
                                 mlir::Value rhs) {
    return builder.create<NLOp>(loc, resultType, lhs, rhs).getResult();
}

struct BinaryFunctionLowering {
    NLBinaryFunctionEmitter emit {nullptr};
    UnaryFunctionElement element {nullptr};
};

const std::unordered_map<std::string_view, BinaryFunctionLowering> binaryFunctionLowerings = {
    {"db.cosine_similarity",  {&emitNLBinaryFunction<nl::CosineSimilarity>,  &floatFunctionElement}},
    {"db.euclidean_distance", {&emitNLBinaryFunction<nl::EuclideanDistance>, &floatFunctionElement}},
};

const BinaryFunctionLowering* lookupBinaryFunctionLowering(mlir::Operation& operation) {
    const llvm::StringRef name = operation.getName().getStringRef();
    const auto it = binaryFunctionLowerings.find(std::string_view(name.data(), name.size()));
    return it == binaryFunctionLowerings.end() ? nullptr : &it->second;
}

// Map a stored property value type to the MLIR element type baked into the
// nullable value chunk. The element only has to round-trip back to this value
// type during translation, so each kind takes a distinct builtin.
mlir::Type valueTypeToElementType(mlir::OpBuilder& builder, ValueType valueType) {
    switch (valueType) {
        case ValueType::Int64:
            return builder.getIntegerType(64);
        break;

        case ValueType::UInt64:
            return builder.getIntegerType(64, /*isSigned=*/false);
        break;

        case ValueType::Double:
            return builder.getF64Type();
        break;

        case ValueType::Bool:
            return builder.getI1Type();
        break;

        case ValueType::String:
            return storage::StringType::get(builder.getContext());
        break;

        case ValueType::Embedding:
            return storage::EmbeddingType::get(builder.getContext());
        break;

        case ValueType::Invalid:
        case ValueType::_SIZE:
            throw IRException("Invalid property value type");
        break;
    }

    throw IRException("Unhandled property value type");
}

// The chunk a procedure's return value of this type is read as. The element type
// names the concrete column the procedure writes into, so an nl chunk of a
// procedure result is as fully typed as a property chunk: an ID column for the
// entity types, the storage placeholder for the ones with no MLIR builtin (a value
// type code, an owned string, a list) and the matching builtin for the numbers.
nl::ChunkType procedureChunkType(mlir::OpBuilder& builder, ProcedureType procedureType) {
    mlir::MLIRContext* const context = builder.getContext();

    switch (procedureType) {
        case ProcedureType::NODE:
            return nl::ChunkType::get(context, storage::NodeIDType::get(context));
        break;

        case ProcedureType::EDGE:
            return nl::ChunkType::get(context, storage::EdgeIDType::get(context));
        break;

        case ProcedureType::LABEL_ID:
            return nl::ChunkType::get(context, storage::LabelIDType::get(context));
        break;

        case ProcedureType::EDGE_TYPE_ID:
            return nl::ChunkType::get(context, storage::EdgeTypeIDType::get(context));
        break;

        case ProcedureType::PROPERTY_TYPE_ID:
            return nl::ChunkType::get(context, storage::PropertyTypeIDType::get(context));
        break;

        case ProcedureType::VALUE_TYPE:
            return nl::ChunkType::get(context, storage::ValueTypeType::get(context));
        break;

        case ProcedureType::UINT_64:
            return nl::ChunkType::get(context, builder.getIntegerType(64, /*isSigned=*/false));
        break;

        case ProcedureType::INT64:
            return nl::ChunkType::get(context, builder.getIntegerType(64));
        break;

        case ProcedureType::DOUBLE:
            return nl::ChunkType::get(context, builder.getF64Type());
        break;

        case ProcedureType::BOOL:
            return nl::ChunkType::get(context, storage::BoolType::get(context));
        break;

        case ProcedureType::STRING_VIEW:
            return nl::ChunkType::get(context, storage::StringType::get(context));
        break;

        case ProcedureType::STRING:
            return nl::ChunkType::get(context, storage::OwnedStringType::get(context));
        break;

        case ProcedureType::LIST:
            return nl::ChunkType::get(context, storage::ListType::get(context, mlir::NoneType::get(context)));
        break;

        case ProcedureType::INVALID:
        case ProcedureType::_SIZE:
            throw IRException("Invalid procedure value type");
        break;
    }

    throw IRException("Unhandled procedure value type");
}

// The accumulator (and result) element type of an aggregate over a column whose
// nullable value chunk wraps inputElement - or over a type-erased column, whose
// inputElement is the tagged cell itself. avg always reduces to an f64; sum,
// min and max keep the input's own type. Throws for a value type the reduction
// cannot handle: sum/avg need a numeric column, min/max an orderable one (so a
// string sum, a bool sum or an embedding min is rejected, matching Cypher). A tagged
// cell is numeric only once read, so sum and avg accept one - both landing on the f64
// mixed numeric tags reduce to - where min/max would have to hand the winning cell back
// under its own type, which no single result type names.
mlir::Type aggregateResultElementType(mlir::OpBuilder& builder,
                                      storage::AggregateKind kind,
                                      mlir::Type inputElement) {
    const bool isFloat = mlir::isa<mlir::Float64Type>(inputElement);
    const auto integerType = mlir::dyn_cast<mlir::IntegerType>(inputElement);
    const bool isBool = integerType && integerType.getWidth() == 1;
    const bool isInteger = integerType && !isBool;
    const bool isNumeric = isFloat || isInteger;
    const bool isString = mlir::isa<storage::StringType>(inputElement);
    const bool isTaggedCell = mlir::isa<storage::ListElementType>(inputElement);

    switch (kind) {
        case storage::AggregateKind::Sum: {
            if (!isNumeric && !isTaggedCell) {
                throw IRException("db.sum requires a numeric column");
            }
            return isTaggedCell ? builder.getF64Type() : inputElement;
        }
        break;

        case storage::AggregateKind::Avg: {
            if (!isNumeric && !isTaggedCell) {
                throw IRException("db.avg requires a numeric column");
            }
            return builder.getF64Type();
        }
        break;

        case storage::AggregateKind::Min:
        case storage::AggregateKind::Max: {
            // min/max order the values, so anything with a natural order is fine -
            // numbers, strings and bools - but an embedding has none.
            if (!isNumeric && !isString && !isBool) {
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

    const bool isFloat = mlir::isa<mlir::Float64Type>(element);
    const auto integerType = mlir::dyn_cast<mlir::IntegerType>(element);
    const bool isInt64 = integerType && integerType.getWidth() == 64;
    if (!isFloat && !isInt64) {
        throw IRException("db.<op> requires numeric operands");
    }

    return {.numeric = element, .nullable = nullable};
}

bool isNullableChunk(mlir::Type chunkType) {
    const nl::ChunkType chunk = mlir::dyn_cast<nl::ChunkType>(chunkType);
    if (!chunk) {
        throw IRException("Tried to check nullity of non-chunk.");
    }

    return mlir::isa<storage::NullableType>(chunk.getElementType());
}

// The null literal's chunk: nullable with no value type of its own.
bool isUntypedNullChunk(mlir::Type chunkType) {
    const nl::ChunkType chunk = mlir::dyn_cast<nl::ChunkType>(chunkType);
    if (!chunk) {
        return false;
    }

    const auto nullableType = mlir::dyn_cast<storage::NullableType>(chunk.getElementType());
    return nullableType && mlir::isa<mlir::NoneType>(nullableType.getValueType());
}

mlir::Type promoteNumeric(mlir::OpBuilder& builder, mlir::Type lhs, mlir::Type rhs) {
    const bool anyFloat = mlir::isa<mlir::Float64Type>(lhs) || mlir::isa<mlir::Float64Type>(rhs);
    if (anyFloat) {
        return builder.getF64Type();
    }

    // Cypher has one integer type and it is signed. A tally is carried unsigned - it can
    // never be negative - but arithmetic over it can be, so a count entering an expression
    // is promoted like any other integer instead of wrapping around zero
    return builder.getIntegerType(64);
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

        case storage::GroupAggregateKind::Count:
        case storage::GroupAggregateKind::CountDistinct:
        case storage::GroupAggregateKind::CountRows:
            throw IRException("count has no value-reduction kind");
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
// the value type, avg widens to f64.
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
        case storage::GroupAggregateKind::AvgDistinct: {
            const nl::ChunkType inputChunkType = mlir::cast<nl::ChunkType>(inputChunk.getType());
            const mlir::Type inputElement = inputChunkType.getElementType();
            const auto inputNullable = mlir::dyn_cast<storage::NullableType>(inputElement);
            const bool taggedCells = mlir::isa<storage::ListElementType>(inputElement);
            if (!inputNullable && !taggedCells) {
                throw IRException("db.group_aggregate sum/min/max/avg requires a property value column");
            }

            const mlir::Type resultElement = aggregateResultElementType(builder,
                                                                        groupKindToAggregateKind(kind),
                                                                        taggedCells ? inputElement : inputNullable.getValueType());
            const storage::NullableType resultNullable = storage::NullableType::get(context, resultElement);

            return nl::ChunkType::get(context, resultNullable);
        }
        break;
    }

    throw IRException("Unhandled group aggregate kind");
}

// The single nl.output that solely consumes every result in the range, or a null
// op if any result has more than one use, a non-nl.output user, or a different
// output than its siblings. Read the direction as "one shared output user => the
// truncate's copy can be dropped": a terminal truncate folds into its output
// exactly when this is non-null, and then erasing the truncate leaves nothing
// dangling.
nl::Output soleOutputConsumer(mlir::ResultRange results) {
    // A result's one-and-only nl.output user, or a null op for any other shape
    // (more than one use, or a lone use that is not an nl.output).
    const auto soleOutputUser = [](const mlir::Value result) -> nl::Output {
        if (!result.hasOneUse()) {
            return nl::Output();
        }
        return mlir::dyn_cast<nl::Output>(*result.user_begin());
    };

    if (results.empty()) {
        return nl::Output();
    }

    const nl::Output output = soleOutputUser(results.front());
    const bool sharedByAllResults = output && std::all_of(results.begin(), results.end(), [&](const mlir::Value result) {
        return soleOutputUser(result) == output;
    });

    return sharedByAllResults ? output : nl::Output();
}

bool opensSourceLoop(mlir::Operation* operation) {
    return mlir::isa<mlir::db::ScanNodes,
                     mlir::db::ConstScanNodes,
                     mlir::db::UnwindConst,
                     mlir::db::LoadCSV,
                     mlir::db::VectorSearch,
                     mlir::db::Unwind,
                     mlir::db::ScanNodesByLabel,
                     mlir::db::ScanEdges,
                     mlir::db::ScanEdgesByType,
                     mlir::db::GetOutEdges,
                     mlir::db::GetInEdges,
                     mlir::db::GetEdges,
                     mlir::db::GetOutEdgesByType,
                     mlir::db::GetInEdgesByType,
                     mlir::db::CallProcedure>(operation);
}

// The db ops whose rows a projection is emitted over: a source, the nest a cross product
// builds, and the emit loop a pipeline breaker opens over what it accumulated
bool opensRowLoop(mlir::Operation* operation) {
    return opensSourceLoop(operation)
        || mlir::isa<mlir::db::CrossProduct, mlir::db::Sort, mlir::db::GroupAggregate>(operation);
}

// A reduction emits its one row at function scope, so what follows it walks no rows of the
// relation it read - and that relation had to be read in full to reduce it
bool reducesToOneRow(mlir::Operation* operation) {
    return mlir::isa<mlir::db::Count,
                     mlir::db::Sum,
                     mlir::db::Min,
                     mlir::db::Max,
                     mlir::db::Avg>(operation);
}

// Passes some of its rows on and keeps the rest back, so the rows reaching a cut below it
// are fewer than the rows a producer above it made
bool dropsRows(mlir::Operation* operation) {
    return mlir::isa<mlir::db::FilterOp,
                     mlir::db::Skip,
                     mlir::db::Limit,
                     mlir::db::RemoveDuplicates>(operation);
}

// The list element types an unwind can drain into a column of that very type: the entity
// IDs, the value types a nullable value chunk is laid out for, and a nested list, which
// drains into a list column one level shallower. An unresolved element, an embedding, or
// the list_element a heterogeneous list holds drains as tagged scalars instead - none of
// them names a column shape the drain could fill.
bool drainsToItsOwnElementType(mlir::Type listElement) {
    if (mlir::isa<storage::NodeIDType, storage::EdgeIDType, storage::StringType, storage::ListType>(listElement)) {
        return true;
    } else if (mlir::isa<mlir::Float64Type>(listElement)) {
        return true;
    }

    const auto intType = mlir::dyn_cast<mlir::IntegerType>(listElement);

    return intType && (intType.getWidth() == 1 || intType.getWidth() == 64);
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
    _builder.setInsertionPointToEnd(module.getBody());
    const auto functionType = mlir::FunctionType::get(context, {}, {});
    auto nlFunction = _builder.create<mlir::func::FuncOp>(loc, dbFunction.getSymName(), functionType);
    _entryBlock = nlFunction.addEntryBlock();

    // Create the ReturnOp of the target function right away
    _builder.setInsertionPointToStart(_entryBlock);
    _builder.create<mlir::func::ReturnOp>(loc);

    // Lower each operation of the db function. Top-level scans root their loop
    // in the entry block; a cross product retargets the root per factor.
    _valueMap.clear();
    _propertyTypes.clear();
    _edgeTypes.clear();
    _rootBlock = _entryBlock;
    _innermostLoopBody = nullptr;
    _innermostCardinality = mlir::Value();
    _limitHandles.clear();
    _loopLimitHandle.clear();
    _sortTopK.clear();
    _fusedLimits.clear();

    // Find ORDER BY ... LIMIT k: a db.limit capping a db.sort's result fuses into
    // a bounded top-K, so the limit gets no streaming handle and the sort carries
    // the bound. Detect before the limit pre-scan so the fused ones are skipped.
    detectTopKFusion(dbFunction);

    // Pre-scan for db.limits before any loop is built: nl.for's limit operand is
    // fixed at build time, so each handle must exist first to be threaded in, and
    // which loops a handle attaches to must be known up front. A limit fused into
    // a sort's top-K carries no streaming handle, so it is left out here.
    llvm::SmallVector<mlir::db::Limit, 2> limits;
    dbFunction.walk([&](mlir::db::Limit limit) {
        if (!_fusedLimits.count(limit.getOperation())) {
            limits.push_back(limit);
        }
    });

    // Hoist one nl.limit handle per db.limit to the top of the entry block, where
    // each dominates the loops, the update and the truncate that read it. The
    // reset scope is function scope (uncorrelated); a correlated limit would hoist
    // into its enclosing loop body instead - future work.
    if (!limits.empty()) {
        _builder.setInsertionPointToStart(_entryBlock);
        for (mlir::db::Limit limit : limits) {
            nl::Limit limitOp = _builder.create<nl::Limit>(loc, limit.getCount());
            _limitHandles[limit.getOperation()] = limitOp.getState();
        }
    }

    // Assign each limit's handle to the loops that produce its columns and their
    // enclosing nest, so only those loops early-exit (consumer loops downstream of
    // the truncate fan out freely). The first limit, in program order, to claim a
    // shared producer wins, so a loop never needs to carry two handles.
    for (mlir::db::Limit limit : limits) {
        const mlir::Value handle = _limitHandles[limit.getOperation()];

        bool producedByALoop = false;
        for (const mlir::Value column : limit.getColumns()) {
            producedByALoop |= assignProducerLoops(column, handle, /*rowsDroppedBeforeTheCut=*/false);
        }

        // A cut over constants alone walks back to no loop at all - the constants are
        // bound above the nest - so the handle goes to the relation driving the
        // projection instead. Without it the nest runs to its end and the budget only
        // stops the output, producing every row to throw all but k away.
        if (!producedByALoop) {
            assignCardinalityDriverLoop(limit, handle);
        }
    }

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

    // Run MLIR verifier on the nlFunction
    if (mlir::failed(mlir::verify(nlFunction))) {
        throw IRException("DBLowering produced an invalid nl function");
    }

    return nlFunction;
}

void DBLowering::lowerOperation(mlir::Operation& operation) {
    if (mlir::db::ScanNodes scanNodes = mlir::dyn_cast<mlir::db::ScanNodes>(operation)) {
        lowerScanNodes(scanNodes);
    } else if (mlir::db::ScanNodesByLabel scanNodesByLabel = mlir::dyn_cast<mlir::db::ScanNodesByLabel>(operation)) {
        lowerScanNodesByLabel(scanNodesByLabel);
    } else if (mlir::db::ConstScanNodes constScanNodes = mlir::dyn_cast<mlir::db::ConstScanNodes>(operation)) {
        lowerConstScanNodes(constScanNodes);
    } else if (mlir::db::UnwindConst unwindConst = mlir::dyn_cast<mlir::db::UnwindConst>(operation)) {
        lowerUnwindConst(unwindConst);
    } else if (mlir::db::LoadCSV loadCSV = mlir::dyn_cast<mlir::db::LoadCSV>(operation)) {
        lowerLoadCSV(loadCSV);
    } else if (mlir::db::VectorSearch vectorSearch = mlir::dyn_cast<mlir::db::VectorSearch>(operation)) {
        lowerVectorSearch(vectorSearch);
    } else if (mlir::db::Unwind unwind = mlir::dyn_cast<mlir::db::Unwind>(operation)) {
        lowerUnwind(unwind);
    } else if (mlir::db::ScanEdges scanEdges = mlir::dyn_cast<mlir::db::ScanEdges>(operation)) {
        lowerScanEdges(scanEdges);
    } else if (mlir::db::ScanEdgesByType scanEdgesByType = mlir::dyn_cast<mlir::db::ScanEdgesByType>(operation)) {
        lowerScanEdgesByType(scanEdgesByType);
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
    } else if (mlir::db::GetNodeProperties getNodeProperties = mlir::dyn_cast<mlir::db::GetNodeProperties>(operation)) {
        lowerGetNodeProperties(getNodeProperties);
    } else if (mlir::db::GetEdgeProperties getEdgeProperties = mlir::dyn_cast<mlir::db::GetEdgeProperties>(operation)) {
        lowerGetEdgeProperties(getEdgeProperties);
    } else if (mlir::db::GetNodeLabelSet getNodeLabelSet = mlir::dyn_cast<mlir::db::GetNodeLabelSet>(operation)) {
        lowerGetNodeLabelSet(getNodeLabelSet);
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
    } else if (mlir::db::DeleteNode deleteNode = mlir::dyn_cast<mlir::db::DeleteNode>(operation)) {
        lowerDeleteNode(deleteNode);
    } else if (mlir::db::DeleteEdge deleteEdge = mlir::dyn_cast<mlir::db::DeleteEdge>(operation)) {
        lowerDeleteEdge(deleteEdge);
    } else if (mlir::db::CrossProduct crossProduct = mlir::dyn_cast<mlir::db::CrossProduct>(operation)) {
        lowerCrossProduct(crossProduct);
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
    } else if (mlir::db::Sum sum = mlir::dyn_cast<mlir::db::Sum>(operation)) {
        lowerAggregate(sum.getInput(), sum.getResult(), storage::AggregateKind::Sum, sum.getDistinct());
    } else if (mlir::db::Min min = mlir::dyn_cast<mlir::db::Min>(operation)) {
        lowerAggregate(min.getInput(), min.getResult(), storage::AggregateKind::Min, min.getDistinct());
    } else if (mlir::db::Max max = mlir::dyn_cast<mlir::db::Max>(operation)) {
        lowerAggregate(max.getInput(), max.getResult(), storage::AggregateKind::Max, max.getDistinct());
    } else if (mlir::db::Avg avg = mlir::dyn_cast<mlir::db::Avg>(operation)) {
        lowerAggregate(avg.getInput(), avg.getResult(), storage::AggregateKind::Avg, avg.getDistinct());
    } else if (mlir::db::ConstantOp constant = mlir::dyn_cast<mlir::db::ConstantOp>(operation)) {
        lowerConstant(constant);
    } else if (mlir::db::BroadcastConstant broadcast = mlir::dyn_cast<mlir::db::BroadcastConstant>(operation)) {
        lowerBroadcastConstant(broadcast);
    } else if (mlir::isa<mlir::db::AddOp>(operation)) {
        lowerBinaryOp<nl::Add>(operation, BinaryResultKind::Numeric);
    } else if (mlir::isa<mlir::db::ConcatOp>(operation)) {
        lowerBinaryOp<nl::Concat>(operation, BinaryResultKind::String);
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
        lowerBinaryOp<nl::Eq>(operation, BinaryResultKind::Boolean);
    } else if (mlir::isa<mlir::db::NeqOp>(operation)) {
        lowerBinaryOp<nl::Neq>(operation, BinaryResultKind::Boolean);
    } else if (mlir::isa<mlir::db::GtOp>(operation)) {
        lowerBinaryOp<nl::Gt>(operation, BinaryResultKind::Boolean);
    } else if (mlir::isa<mlir::db::LtOp>(operation)) {
        lowerBinaryOp<nl::Lt>(operation, BinaryResultKind::Boolean);
    } else if (mlir::isa<mlir::db::GteOp>(operation)) {
        lowerBinaryOp<nl::Gte>(operation, BinaryResultKind::Boolean);
    } else if (mlir::isa<mlir::db::LteOp>(operation)) {
        lowerBinaryOp<nl::Lte>(operation, BinaryResultKind::Boolean);
    } else if (mlir::isa<mlir::db::StartsWithOp>(operation)) {
        lowerBinaryOp<nl::StartsWith>(operation, BinaryResultKind::Boolean);
    } else if (mlir::isa<mlir::db::EndsWithOp>(operation)) {
        lowerBinaryOp<nl::EndsWith>(operation, BinaryResultKind::Boolean);
    } else if (mlir::isa<mlir::db::ContainsOp>(operation)) {
        lowerBinaryOp<nl::Contains>(operation, BinaryResultKind::Boolean);
    } else if (mlir::isa<mlir::db::AndOp>(operation)) {
        lowerBinaryOp<nl::And>(operation, BinaryResultKind::Boolean);
    } else if (mlir::isa<mlir::db::OrOp>(operation)) {
        lowerBinaryOp<nl::Or>(operation, BinaryResultKind::Boolean);
    } else if (mlir::isa<mlir::db::XorOp>(operation)) {
        lowerBinaryOp<nl::Xor>(operation, BinaryResultKind::Boolean);
    } else if (mlir::db::NotOp notOp = mlir::dyn_cast<mlir::db::NotOp>(operation)) {
        lowerNot(notOp);
    } else if (mlir::db::FilterOp filter = mlir::dyn_cast<mlir::db::FilterOp>(operation)) {
        lowerFilter(filter);
    } else if (mlir::db::GroupAggregate groupAggregate = mlir::dyn_cast<mlir::db::GroupAggregate>(operation)) {
        lowerGroupAggregate(groupAggregate);
    } else if (mlir::db::Collect collect = mlir::dyn_cast<mlir::db::Collect>(operation)) {
        lowerCollect(collect);
    } else if (mlir::db::UnwindCollect unwindCollect = mlir::dyn_cast<mlir::db::UnwindCollect>(operation)) {
        lowerUnwindCollect(unwindCollect);
    } else if (mlir::db::CallProcedure call = mlir::dyn_cast<mlir::db::CallProcedure>(operation)) {
        lowerCallProcedure(call);
    } else if (mlir::db::Output output = mlir::dyn_cast<mlir::db::Output>(operation)) {
        lowerOutput(output);
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

void DBLowering::lowerScanNodes(mlir::db::ScanNodes scanNodes) {
    // A scan reads no column, so its loop sits at the top of the current root
    // block: the function entry at top level, or - inside a cross product - the
    // outer factor's innermost loop body, so the inner factor nests under it.
    setInsertionInto(_rootBlock);

    nl::ScanNodes nodes = _builder.create<nl::ScanNodes>(_builder.getUnknownLoc());
    buildLoopForSource(nodes.getResult(), scanNodes.getOperation());
}

void DBLowering::lowerScanNodesByLabel(mlir::db::ScanNodesByLabel scanNodesByLabel) {
    // The label sibling of lowerScanNodes: a scan reads no column, so its loop
    // sits at the top of the current root block. The label list is a filter on
    // the rows, not a column input, so it is forwarded as-is to the nl op -
    // translation resolves the names against the schema, the same way the
    // property name on nl.get_property_type is resolved by its consumer.
    setInsertionInto(_rootBlock);

    nl::ScanNodesByLabel nodes = _builder.create<nl::ScanNodesByLabel>(_builder.getUnknownLoc(),
                                                                       scanNodesByLabel.getLabelsAttr());
    buildLoopForSource(nodes.getResult(), scanNodesByLabel.getOperation());
}

void DBLowering::lowerConstScanNodes(mlir::db::ConstScanNodes constScanNodes) {
    // The constant-set sibling of lowerScanNodes: a scan reads no column, so its
    // loop sits at the top of the current root block. The node ID list is the set
    // of rows to emit, not a column input, so it is forwarded as-is to the nl op -
    // translation resolves each entry to a storage NodeID.
    setInsertionInto(_rootBlock);

    nl::ConstScanNodes nodes = _builder.create<nl::ConstScanNodes>(_builder.getUnknownLoc(),
                                                                   constScanNodes.getNodeIDsAttr());
    buildLoopForSource(nodes.getResult(), constScanNodes.getOperation());
}

void DBLowering::lowerUnwindConst(mlir::db::UnwindConst unwindConst) {
    // The literal-list sibling of lowerConstScanNodes: a source reads no column, so
    // its loop sits at the top of the current root block. The literals are the rows to
    // emit, forwarded as-is; translation materializes them into a ListView. Unlike a
    // node-ID scan the chunk element type varies with the list (a homogeneous value
    // type, or list_element for a heterogeneous one), so - like nl.collect - the
    // iterator type is spelled here from the db column's element type rather than
    // inferred.
    setInsertionInto(_rootBlock);

    mlir::MLIRContext* const context = _builder.getContext();
    const mlir::db::ColumnType column = mlir::cast<mlir::db::ColumnType>(unwindConst.getResult().getType());
    const mlir::Type elementType = column.getType();

    // A homogeneous list rides a nullable value chunk, as a property fetch and the
    // unwind_collect drain do: the elements are never null, but every value-chunk
    // consumer (a cross product broadcast, a filter, a skip, an aggregate) dispatches
    // on nullable<T>, so the uniform shape is what makes the unwound column composable.
    // A heterogeneous list keeps its type-erased list_element chunk, which only
    // pass-through consumers accept.
    const bool heterogeneous = mlir::isa<storage::ListElementType>(elementType);
    const mlir::Type chunkElementType = heterogeneous ? elementType
                                                      : storage::NullableType::get(context, elementType);

    const nl::ChunkType chunk = nl::ChunkType::get(context, chunkElementType);
    const nl::IteratorType iteratorType = nl::IteratorType::get(context, {chunk});

    nl::UnwindConst rows = _builder.create<nl::UnwindConst>(_builder.getUnknownLoc(),
                                                            iteratorType,
                                                            unwindConst.getElementsAttr());
    buildLoopForSource(rows.getResult(), unwindConst.getOperation());
}

void DBLowering::lowerLoadCSV(mlir::db::LoadCSV loadCSV) {
    // The file sibling of lowerUnwindConst: a source reads no column, so its loop sits at
    // the top of the current root block. The path, the field list and the flags are
    // forwarded as-is; the path and the header names are resolved when the loop runs. One
    // owning string chunk per field, so - like nl.unwind_const - the iterator type is
    // spelled here rather than inferred.
    setInsertionInto(_rootBlock);

    mlir::MLIRContext* const context = _builder.getContext();
    const nl::ChunkType chunk = nl::ChunkType::get(context, storage::OwnedStringType::get(context));

    const llvm::SmallVector<mlir::Type> chunks(loadCSV.getResults().size(), chunk);
    const nl::IteratorType iteratorType = nl::IteratorType::get(context, chunks);

    nl::LoadCSV records = _builder.create<nl::LoadCSV>(_builder.getUnknownLoc(),
                                                       iteratorType,
                                                       loadCSV.getPathAttr(),
                                                       loadCSV.getFieldsAttr(),
                                                       loadCSV.getWithHeadersAttr(),
                                                       loadCSV.getSkipOnErrorAttr());
    buildLoopForSource(records.getResult(), loadCSV.getOperation());
}

void DBLowering::lowerVectorSearch(mlir::db::VectorSearch vectorSearch) {
    // The neighbour sibling of lowerUnwindConst: a source reads no column, so its loop
    // sits at the top of the current root block. The index, the neighbour count and the
    // query vector are forwarded as-is; the two chunk types are fixed, so the iterator
    // type is inferred rather than spelled.
    setInsertionInto(_rootBlock);

    nl::VectorSearch neighbours = _builder.create<nl::VectorSearch>(_builder.getUnknownLoc(),
                                                                    vectorSearch.getIndexNameAttr(),
                                                                    vectorSearch.getKAttr(),
                                                                    vectorSearch.getQueryVectorAttr());
    buildLoopForSource(neighbours.getResult(), vectorSearch.getOperation());
}

mlir::Type DBLowering::unwoundElementType(mlir::MLIRContext* context, mlir::Type sourceElement) {
    // Any source but a list keeps the column it already rides - its cells are the
    // elements, and a tagged cell holding a list gives up tagged scalars again.
    const auto listType = mlir::dyn_cast<storage::ListType>(sourceElement);
    if (!listType) {
        return sourceElement;
    }

    // The elements of a list whose type is known are that type, so the unwind hands the
    // rest of the query a column it can read as one - a node stays a node, an integer an
    // integer. Only a list whose elements share no such type drains into the type-erased
    // column of tagged scalars.
    const mlir::Type listElement = listType.getElementType();
    if (!drainsToItsOwnElementType(listElement)) {
        return storage::ListElementType::get(context);
    }

    // An entity ID and a nested list are present in every row they are drained from, so
    // they ride a plain chunk; a value rides the nullable one every value-chunk consumer
    // dispatches on, as lowerUnwindConst's homogeneous list does.
    if (mlir::isa<storage::NodeIDType, storage::EdgeIDType, storage::ListType>(listElement)) {
        return listElement;
    }

    return storage::NullableType::get(context, listElement);
}

void DBLowering::lowerUnwind(mlir::db::Unwind unwind) {
    llvm::SmallVector<mlir::Value, 4> carriedChunks;
    for (const mlir::Value carriedColumn : unwind.getColumnsToFilter()) {
        carriedChunks.push_back(mapValue(carriedColumn));
    }

    // A constant source holds one cell standing for every row rather than one per row, so
    // it is laid out over the rows it expands - the carried ones, or the single row a
    // scope of constants alone is.
    const mlir::Value cardinality = cardinalityDriver(carriedChunks);
    const mlir::Value sourceChunk = rowAlignedChunk(mapValue(unwind.getSource()), cardinality);

    // Inserted into the deepest block, where every operand is bound - as lowerFilter's is
    mlir::Value insertionReference = sourceChunk;
    for (const mlir::Value carriedChunk : carriedChunks) {
        mlir::Block* const block = deeperBlock(insertionReference, carriedChunk);
        if (ownerBlock(carriedChunk) == block) {
            insertionReference = carriedChunk;
        }
    }

    setInsertionInto(ownerBlock(insertionReference));

    mlir::MLIRContext* const context = _builder.getContext();
    const mlir::Type sourceElement = mlir::cast<nl::ChunkType>(sourceChunk.getType()).getElementType();

    llvm::SmallVector<mlir::Type, 4> chunkTypes;
    chunkTypes.push_back(nl::ChunkType::get(context, unwoundElementType(context, sourceElement)));

    for (const mlir::Value carriedChunk : carriedChunks) {
        chunkTypes.push_back(carriedChunk.getType());
    }

    const nl::IteratorType iteratorType = nl::IteratorType::get(context, chunkTypes);

    nl::Unwind rows = _builder.create<nl::Unwind>(_builder.getUnknownLoc(),
                                                  iteratorType,
                                                  sourceChunk,
                                                  carriedChunks);
    buildLoopForSource(rows.getResult(), unwind.getOperation());
}

void DBLowering::lowerScanEdges(mlir::db::ScanEdges scanEdges) {
    // The edge sibling of lowerScanNodes: a scan reads no column, so its loop
    // sits at the top of the current root block. The nl.scan_edges iterator
    // produces the four fixed edge chunks, which buildLoopForSource binds to the
    // op's four results in order.
    setInsertionInto(_rootBlock);

    nl::ScanEdges edges = _builder.create<nl::ScanEdges>(_builder.getUnknownLoc());
    buildLoopForSource(edges.getResult(), scanEdges.getOperation());
}

void DBLowering::lowerScanEdgesByType(mlir::db::ScanEdgesByType scanEdgesByType) {
    // The by-type sibling of lowerScanEdges: same placement at the top of the root
    // block, with the type name hoisted into the nl.get_edge_type handle the
    // by-type hops already share.
    const mlir::Value edgeTypeHandle = getOrCreateEdgeTypeHandle(scanEdgesByType.getEdgeType());

    setInsertionInto(_rootBlock);

    nl::ScanEdgesByType edges = _builder.create<nl::ScanEdgesByType>(_builder.getUnknownLoc(), edgeTypeHandle);
    buildLoopForSource(edges.getResult(), scanEdgesByType.getOperation());
}

void DBLowering::lowerGetOutEdges(mlir::db::GetOutEdges getOutEdges) {
    // Map the input node column and the carry set to the nl chunks they lowered
    // to. The fetch nests in the loop that binds its input chunk.
    const mlir::Value inputChunk = mapValue(getOutEdges.getInputNodes());

    llvm::SmallVector<mlir::Value, 4> carriedChunks;
    for (const mlir::Value carriedColumn : getOutEdges.getColumnsToFilter()) {
        carriedChunks.push_back(mapValue(carriedColumn));
    }

    setInsertionInto(ownerBlock(inputChunk));

    // The result iterator type - the four fixed edge chunks plus one per
    // carried chunk - is inferred from the operands.
    nl::GetOutEdges edges = _builder.create<nl::GetOutEdges>(_builder.getUnknownLoc(), inputChunk, carriedChunks);
    buildLoopForSource(edges.getResult(), getOutEdges.getOperation());
}

void DBLowering::lowerGetInEdges(mlir::db::GetInEdges getInEdges) {
    // The predecessor counterpart of lowerGetOutEdges: same shape, reverse
    // direction. Map the input node column and the carry set to the nl chunks
    // they lowered to. The fetch nests in the loop that binds its input chunk.
    const mlir::Value inputChunk = mapValue(getInEdges.getInputNodes());

    llvm::SmallVector<mlir::Value, 4> carriedChunks;
    for (const mlir::Value carriedColumn : getInEdges.getColumnsToFilter()) {
        carriedChunks.push_back(mapValue(carriedColumn));
    }

    setInsertionInto(ownerBlock(inputChunk));

    // The result iterator type - the four fixed edge chunks plus one per
    // carried chunk - is inferred from the operands.
    nl::GetInEdges edges = _builder.create<nl::GetInEdges>(_builder.getUnknownLoc(), inputChunk, carriedChunks);
    buildLoopForSource(edges.getResult(), getInEdges.getOperation());
}

void DBLowering::lowerGetEdges(mlir::db::GetEdges getEdges) {
    const mlir::Value inputChunk = mapValue(getEdges.getInputNodes());

    llvm::SmallVector<mlir::Value, 4> carriedChunks;
    for (const mlir::Value carriedColumn : getEdges.getColumnsToFilter()) {
        carriedChunks.push_back(mapValue(carriedColumn));
    }

    setInsertionInto(ownerBlock(inputChunk));


    const mlir::Location uloc = _builder.getUnknownLoc();
    nl::GetEdges edges = _builder.create<nl::GetEdges>(uloc, inputChunk, carriedChunks);
    buildLoopForSource(edges.getResult(), getEdges.getOperation());
}

void DBLowering::lowerGetOutEdgesByType(mlir::db::GetOutEdgesByType getOutEdgesByType) {
    const mlir::Value inputChunk = mapValue(getOutEdgesByType.getInputNodes());
    const mlir::Value edgeTypeHandle = getOrCreateEdgeTypeHandle(getOutEdgesByType.getEdgeType());

    llvm::SmallVector<mlir::Value, 4> carriedChunks;
    for (const mlir::Value carriedColumn : getOutEdgesByType.getColumnsToFilter()) {
        carriedChunks.push_back(mapValue(carriedColumn));
    }

    setInsertionInto(ownerBlock(inputChunk));

    nl::GetOutEdgesByType edges = _builder.create<nl::GetOutEdgesByType>(_builder.getUnknownLoc(),
                                                                         inputChunk,
                                                                         edgeTypeHandle,
                                                                         carriedChunks);
    buildLoopForSource(edges.getResult(), getOutEdgesByType.getOperation());
}

void DBLowering::lowerGetInEdgesByType(mlir::db::GetInEdgesByType getInEdgesByType) {
    // The predecessor counterpart of lowerGetOutEdgesByType: same shape, reverse
    // direction, edge type hoisted into the same nl.get_edge_type handle.
    const mlir::Value inputChunk = mapValue(getInEdgesByType.getInputNodes());
    const mlir::Value edgeTypeHandle = getOrCreateEdgeTypeHandle(getInEdgesByType.getEdgeType());

    llvm::SmallVector<mlir::Value, 4> carriedChunks;
    for (const mlir::Value carriedColumn : getInEdgesByType.getColumnsToFilter()) {
        carriedChunks.push_back(mapValue(carriedColumn));
    }

    setInsertionInto(ownerBlock(inputChunk));

    // The result iterator type - the four fixed edge chunks plus one per carried
    // chunk - is inferred from the operands, exactly as for get_in_edges.
    nl::GetInEdgesByType edges = _builder.create<nl::GetInEdgesByType>(_builder.getUnknownLoc(),
                                                                       inputChunk,
                                                                       edgeTypeHandle,
                                                                       carriedChunks);
    buildLoopForSource(edges.getResult(), getInEdgesByType.getOperation());
}

void DBLowering::lowerGetNodeProperties(mlir::db::GetNodeProperties getNodeProperties) {
    const mlir::Value inputChunk = mapValue(getNodeProperties.getInputNodes());
    const llvm::StringRef property = getNodeProperties.getProperty();

    // Resolve the name once, hoisted above the loops, and bake the value type.
    const mlir::Value handle = getOrCreatePropertyTypeHandle(property);
    const mlir::Type valueChunkType = propertyValueChunkType(property);

    // A property read maps the input chunk in place, one value per node, so the
    // fetch nests in the loop that binds that chunk - it opens no loop of its own.
    setInsertionInto(ownerBlock(inputChunk));

    nl::GetNodeProperties fetch = _builder.create<nl::GetNodeProperties>(_builder.getUnknownLoc(),
                                                                         valueChunkType,
                                                                         inputChunk,
                                                                         handle,
                                                                         mapOptionalMask(getNodeProperties.getPending()));
    _valueMap[getNodeProperties.getResult()] = fetch.getValues();
}

void DBLowering::lowerGetEdgeProperties(mlir::db::GetEdgeProperties getEdgeProperties) {
    const mlir::Value inputChunk = mapValue(getEdgeProperties.getInputEdges());
    const llvm::StringRef property = getEdgeProperties.getProperty();

    const mlir::Value handle = getOrCreatePropertyTypeHandle(property);
    const mlir::Type valueChunkType = propertyValueChunkType(property);

    setInsertionInto(ownerBlock(inputChunk));

    nl::GetEdgeProperties fetch = _builder.create<nl::GetEdgeProperties>(_builder.getUnknownLoc(),
                                                                         valueChunkType,
                                                                         inputChunk,
                                                                         handle,
                                                                         mapOptionalMask(getEdgeProperties.getPending()));
    _valueMap[getEdgeProperties.getResult()] = fetch.getValues();
}

void DBLowering::lowerGetNodeLabelSet(mlir::db::GetNodeLabelSet getNodeLabelSet) {
    const mlir::Value inputChunk = mapValue(getNodeLabelSet.getInputNodes());

    setInsertionInto(ownerBlock(inputChunk));

    const mlir::Type labelSetIDChunkType = nl::ChunkType::get(
        _builder.getContext(),
        storage::LabelSetIDType::get(_builder.getContext()));

    nl::GetNodeLabelSet fetch = _builder.create<nl::GetNodeLabelSet>(
        _builder.getUnknownLoc(),
        labelSetIDChunkType,
        inputChunk);

    _valueMap[getNodeLabelSet.getResult()] = fetch.getLabelSetIds();
}

void DBLowering::lowerCheckLabelConstraint(mlir::db::CheckLabelConstraint checkLabelConstraint) {
    const LabelMap& labelMap = _view->metadata().labels();

    LabelSet constraintLabelSet;
    for (const mlir::Attribute labelAttr : checkLabelConstraint.getLabels()) {
        const llvm::StringRef labelName = mlir::cast<mlir::StringAttr>(labelAttr).getValue();
        const std::optional<LabelID> labelID = labelMap.get(
            std::string_view(labelName.data(), labelName.size()));

        bioassert(labelID.has_value(), "Invalid label passed analyzer.");

        constraintLabelSet.set(*labelID);
    }

    llvm::SmallVector<int64_t> matchingIDs;
    const LabelSetHandle constraintHandle(constraintLabelSet);
    for (const LabelSetMap::Pair& pair : _view->metadata().labelsets()) {
        const LabelSetHandle candidate(*pair._value);
        if (candidate.hasAtLeastLabels(constraintHandle)) {
            matchingIDs.push_back(static_cast<int64_t>(pair._id.getValue()));
        }
    }

    const mlir::Value inputChunk = mapValue(checkLabelConstraint.getLabelsetIds());

    setInsertionInto(ownerBlock(inputChunk));

    const mlir::Type boolChunkType = nl::ChunkType::get(
        _builder.getContext(),
        storage::BoolType::get(_builder.getContext()));

    nl::CheckLabelConstraint check = _builder.create<nl::CheckLabelConstraint>(
        _builder.getUnknownLoc(),
        boolChunkType,
        inputChunk,
        _builder.getDenseI64ArrayAttr(matchingIDs));

    _valueMap[checkLabelConstraint.getResult()] = check.getResult();
}

void DBLowering::lowerCheckEdgeTypeConstraint(mlir::db::CheckEdgeTypeConstraint checkEdgeTypeConstraint) {
    const EdgeTypeMap& edgeTypeMap = _view->metadata().edgeTypes();

    llvm::SmallVector<int64_t> matchingIDs;
    for (const mlir::Attribute typeAttr : checkEdgeTypeConstraint.getEdgeTypes()) {
        const llvm::StringRef typeName = mlir::cast<mlir::StringAttr>(typeAttr).getValue();
        const std::optional<EdgeTypeID> edgeTypeID = edgeTypeMap.get(
            std::string_view(typeName.data(), typeName.size()));

        bioassert(edgeTypeID.has_value(), "Invalid edge type passed analyzer.");

        matchingIDs.push_back(static_cast<int64_t>(edgeTypeID->getValue()));
    }

    const mlir::Value inputChunk = mapValue(checkEdgeTypeConstraint.getEdgeTypeIds());

    setInsertionInto(ownerBlock(inputChunk));

    const mlir::Type boolChunkType = nl::ChunkType::get(
        _builder.getContext(),
        storage::BoolType::get(_builder.getContext()));

    nl::CheckEdgeTypeConstraint check = _builder.create<nl::CheckEdgeTypeConstraint>(
        _builder.getUnknownLoc(),
        boolChunkType,
        inputChunk,
        _builder.getDenseI64ArrayAttr(matchingIDs));

    _valueMap[checkEdgeTypeConstraint.getResult()] = check.getResult();
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
    // body, where both factors have a chunk bound - just before whatever
    // consumes the product (the lowered db.output).
    setInsertionInto(innerBody);

    // Null when no limit governs this product (built in full); otherwise the
    // handle whose budget caps the build, so the cross lays out only the prefix
    // the limit can emit this step.
    const mlir::Value limitHandle = _loopLimitHandle.lookup(product.getOperation());
    nl::CrossProduct cross = _builder.create<nl::CrossProduct>(_builder.getUnknownLoc(),
                                                               outerColumns,
                                                               innerColumns,
                                                               limitHandle);

    // The product's results are the outer factor's yielded columns followed by
    // the inner's, the same order nl.cross_product lays out its results.
    const mlir::ResultRange dbResults = product.getResults();
    const mlir::ResultRange crossResults = cross.getResults();
    for (size_t resultIndex = 0; resultIndex < dbResults.size(); resultIndex++) {
        _valueMap[dbResults[resultIndex]] = crossResults[resultIndex];
    }

    // The crossed columns live in innerBody, so when this product is itself
    // nested in a factor - e.g. the three-way MATCH (a), (b), (c), where a
    // cross_product's factor is another cross_product - it stands in for that
    // factor's innermost loop: the enclosing factor roots its next op (or a
    // deeper product) there. Record it the way buildLoopForSource records a real
    // loop, so lowerFactor sees a factor whose innermost "loop" is this product.
    // At top level nothing reads _innermostLoopBody, so this is a no-op there.
    _innermostLoopBody = innerBody;

    // Cross prod result defines cardinality
    _innermostCardinality = crossResults.front();
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
    mlir::Block* factorBody = rootBlock;
    size_t deepestDepth = blockNestingDepth(rootBlock);
    for (const mlir::Value chunk : yieldedChunks) {
        mlir::Block* const owner = ownerBlock(chunk);
        const size_t ownerDepth = blockNestingDepth(owner);

        if (ownerDepth > deepestDepth) {
            factorBody = owner;
            deepestDepth = ownerDepth;
        }
    }

    _rootBlock = previousRoot;
    _innermostLoopBody = previousInnermostLoopBody;
    _innermostCardinality = previousInnermostCardinality;

    return factorBody;
}

mlir::Value DBLowering::getOrCreatePropertyTypeHandle(llvm::StringRef propertyName) {
    const auto existing = _propertyTypes.find(propertyName);
    if (existing != _propertyTypes.end()) {
        return existing->second;
    }

    // The handle reads no chunk, so it sits at the very top of the entry block,
    // above every loop, where it dominates all the fetches that use it.
    _builder.setInsertionPointToStart(_entryBlock);

    nl::GetPropertyType handleOp = _builder.create<nl::GetPropertyType>(_builder.getUnknownLoc(),
                                                                        _builder.getStringAttr(propertyName));
    const mlir::Value handle = handleOp.getResult();
    _propertyTypes[propertyName] = handle;

    return handle;
}

mlir::Value DBLowering::getOrCreateEdgeTypeHandle(llvm::StringRef edgeTypeName) {
    // The edge sibling of getOrCreatePropertyTypeHandle: dedup per name and hoist
    // the handle to the top of the entry block, above every loop, so a by-type hop
    // nested in a loop reuses one resolved handle rather than re-carrying the name.
    const auto existing = _edgeTypes.find(edgeTypeName);
    if (existing != _edgeTypes.end()) {
        return existing->second;
    }

    _builder.setInsertionPointToStart(_entryBlock);

    nl::GetEdgeType handleOp = _builder.create<nl::GetEdgeType>(_builder.getUnknownLoc(),
                                                               _builder.getStringAttr(edgeTypeName));
    const mlir::Value handle = handleOp.getResult();
    _edgeTypes[edgeTypeName] = handle;

    return handle;
}

mlir::Type DBLowering::propertyValueChunkType(llvm::StringRef propertyName) {
    if (!_view) {
        throw IRException("Lowering a property fetch needs a graph to resolve the type of '" + propertyName.str() + "'");
    }

    const std::optional<PropertyType> propertyType = _view->metadata().propTypes().get(propertyName);
    if (!propertyType) {
        throw IRException("Unknown property '" + propertyName.str() + "'");
    }

    const mlir::Type elementType = valueTypeToElementType(_builder, propertyType->_valueType);
    storage::NullableType nullableType = storage::NullableType::get(_builder.getContext(), elementType);

    return nl::ChunkType::get(_builder.getContext(), nullableType);
}

void DBLowering::lowerLimit(mlir::db::Limit limit) {
    // A limit fused into a sort's top-K does no work of its own: the sort already
    // emits at most k sorted rows, so the limit forwards each input chunk straight
    // to its matching result. The db.output that follows then reads the sort's
    // emit-loop variables, exactly as if the limit were not there.
    if (_fusedLimits.count(limit.getOperation())) {
        const mlir::ResultRange results = limit.getResults();
        const mlir::OperandRange columns = limit.getColumns();
        for (size_t columnIndex = 0; columnIndex < results.size(); columnIndex++) {
            _valueMap[results[columnIndex]] = mapValue(columns[columnIndex]);
        }

        return;
    }

    // The nl chunks the limited columns lowered to; these are what the truncate
    // copies, and the consumer reads the cut copies.
    llvm::SmallVector<mlir::Value, 4> chunks;
    for (const mlir::Value column : limit.getColumns()) {
        chunks.push_back(mapValue(column));
    }

    // Limit::verify rejects an empty db.limit, so reaching it here means
    // unverified IR - a defensive backstop, as in lowerFactor for cross products.
    if (chunks.empty()) {
        throw IRException("db.limit requires at least one column");
    }

    rowAlignCutChunks(chunks);

    const mlir::Location loc = _builder.getUnknownLoc();
    const mlir::Value handle = _limitHandles.lookup(limit.getOperation());

    // The representative is the first limited column, in the innermost producing
    // loop body (post-cross-product if there is one), so its row count is what
    // this step charges and the truncate copies.
    const mlir::Value representative = chunks.front();
    setInsertionInto(ownerBlock(representative));

    // Charge this step's rows, then copy the first emitThisStep rows of every
    // limited column into fresh chunks, just before the consumer (nl.output when
    // unchained, the inner sub-pipeline when chained).
    _builder.create<nl::LimitUpdate>(loc, handle, representative);
    nl::LimitTruncate truncate = _builder.create<nl::LimitTruncate>(loc, handle, chunks);

    // Map db.limit's results to the truncated chunks, so its consumer reads the
    // cut copies rather than the full producer chunks.
    const mlir::ResultRange dbResults = limit.getResults();
    const mlir::ResultRange truncatedChunks = truncate.getResults();
    for (size_t resultIndex = 0; resultIndex < dbResults.size(); resultIndex++) {
        _valueMap[dbResults[resultIndex]] = truncatedChunks[resultIndex];
    }

    followCardinalityThrough(chunks, truncatedChunks);
}

void DBLowering::lowerSkip(mlir::db::Skip skip) {
    // The nl chunks the skipped columns lowered to; these are what the truncate
    // copies, and the consumer reads the cut copies.
    llvm::SmallVector<mlir::Value, 4> chunks;
    for (const mlir::Value column : skip.getColumns()) {
        chunks.push_back(mapValue(column));
    }

    // Skip::verify rejects an empty db.skip, so reaching it here means unverified
    // IR - a defensive backstop, as in lowerLimit.
    if (chunks.empty()) {
        throw IRException("db.skip requires at least one column");
    }

    rowAlignCutChunks(chunks);

    const mlir::Location loc = _builder.getUnknownLoc();

    // Hoist the skip handle to the top of the entry block, above every loop, so it
    // dominates the update and the truncate placed in the producing loop body.
    // Unlike a limit, a skip threads no operand onto the loops (it cannot
    // early-exit - every row past the dropped prefix must still be produced), so it
    // needs no up-front pre-scan: the handle is created here, in program order,
    // once the producing loops already exist.
    _builder.setInsertionPointToStart(_entryBlock);
    const mlir::Value handle = _builder.create<nl::Skip>(loc, skip.getCount()).getState();

    // The representative is the first skipped column, in the innermost producing
    // loop body (post-cross-product if there is one), so its row count is what this
    // step charges and the truncate's suffix is cut from.
    const mlir::Value representative = chunks.front();
    setInsertionInto(ownerBlock(representative));

    // Charge this step's rows, then lift the surviving suffix of every skipped
    // column into fresh chunks, just before the consumer (nl.output when unchained,
    // the inner sub-pipeline when chained). The unchained case is folded away by
    // foldSkipTruncatesIntoOutputs - nl.output emits the suffix in place at an
    // offset - so this copy survives only when the suffix feeds an inner
    // sub-pipeline that reads from row zero.
    _builder.create<nl::SkipUpdate>(loc, handle, representative);
    nl::SkipTruncate truncate = _builder.create<nl::SkipTruncate>(loc, handle, chunks);

    // Map db.skip's results to the truncated chunks, so its consumer reads the cut
    // copies rather than the full producer chunks.
    const mlir::ResultRange dbResults = skip.getResults();
    const mlir::ResultRange truncatedChunks = truncate.getResults();
    for (size_t resultIndex = 0; resultIndex < dbResults.size(); resultIndex++) {
        _valueMap[dbResults[resultIndex]] = truncatedChunks[resultIndex];
    }

    followCardinalityThrough(chunks, truncatedChunks);
}

void DBLowering::detectTopKFusion(mlir::func::FuncOp dbFunction) {
    dbFunction.walk([&](mlir::db::Limit limit) {
        const mlir::OperandRange columns = limit.getColumns();
        if (columns.empty()) {
            return;
        }

        // Every column the limit caps must come from one db.sort - otherwise the
        // limit is not a terminal ORDER BY ... LIMIT and the streaming limit path
        // handles it.
        mlir::db::Sort sort;
        for (const mlir::Value column : columns) {
            mlir::db::Sort definingSort = column.getDefiningOp<mlir::db::Sort>();
            if (!definingSort || (sort && sort != definingSort)) {
                return;
            }

            sort = definingSort;
        }

        // Capping the sort to top-K must not starve another consumer, so the limit
        // must be the sole user of every result the sort produces.
        for (const mlir::Value result : sort.getResults()) {
            for (mlir::Operation* const user : result.getUsers()) {
                if (user != limit.getOperation()) {
                    return;
                }
            }
        }

        // The sole-user check above guarantees this limit is the only consumer of
        // the sort, so no other limit can claim it; record the fusion.
        _sortTopK[sort.getOperation()] = limit.getCount();
        _fusedLimits.insert(limit.getOperation());
    });
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

    const mlir::Location loc = _builder.getUnknownLoc();

    // The accumulator and its sort spec are hoisted to the top of the entry
    // block, above every loop, so the buffers exist before the producing loop
    // fills them and the handle dominates the collect and the emit loop. A sort
    // fused with a terminal db.limit carries that count as its top-K bound, so the
    // accumulator keeps only the best k rows; an unfused sort keeps every row.
    _builder.setInsertionPointToStart(_entryBlock);

    const auto topK = _sortTopK.find(sort.getOperation());
    mlir::IntegerAttr topKAttr;
    if (topK != _sortTopK.end()) {
        topKAttr = _builder.getIntegerAttr(_builder.getIntegerType(64, /*isSigned=*/false), topK->second);
    }

    nl::SortBuffer bufferOp = _builder.create<nl::SortBuffer>(loc,
                                                              sort.getKeyColumnsAttr(),
                                                              sort.getKeyAscendingAttr(),
                                                              topKAttr);
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

    setInsertionInto(_entryBlock);
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

    const mlir::Location loc = _builder.getUnknownLoc();

    // The seen-set handle is hoisted to the top of the entry block, above every
    // loop, so it is reset once at function scope and dominates the filter placed
    // in the producing loop body. A correlated DISTINCT (reset per enclosing step)
    // would hoist into its enclosing loop body instead - future work, as for the
    // streaming limit.
    _builder.setInsertionPointToStart(_entryBlock);
    const mlir::Value state = _builder.create<nl::Distinct>(loc).getState();

    // The filter sits in the innermost producing loop body, where all deduped
    // columns are bound together (the same block db.output would emit from), and
    // emits each step's not-yet-seen rows as fresh survivor chunks. It opens no
    // loop of its own: DISTINCT streams, so - unlike db.sort - the rows are
    // filtered in place in the producing loop, not accumulated and re-emitted.
    const mlir::Value representative = chunks.front();
    setInsertionInto(ownerBlock(representative));
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
}

void DBLowering::lowerCount(mlir::db::Count count) {
    // The nl chunk the counted column lowered to; the update reads its per-step
    // non-null row count. A constant column counts the rows it stands for, not the
    // one row it is, so it is laid out over the driving relation first.
    const mlir::Value inputChunk = rowAlignedChunk(mapValue(count.getInput()), _innermostCardinality);

    const mlir::Location loc = _builder.getUnknownLoc();

    // The tally is hoisted to the top of the entry block, above every loop, so it
    // is reset once at function scope and dominates the update placed in the
    // producing loop body and the emit that reads it after the loop. A correlated
    // COUNT (reset per enclosing step) would hoist into its enclosing loop body
    // instead - future work, as for the streaming limit.
    _builder.setInsertionPointToStart(_entryBlock);
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
    mlir::Block* const producingBlock = ownerBlock(inputChunk);
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
}

void DBLowering::lowerAggregate(mlir::Value input, mlir::Value result, storage::AggregateKind kind, bool distinct) {
    // The nl chunk the aggregated column lowered to; the update folds its per-step
    // non-null values into the accumulator. A constant column is reduced over the
    // rows it stands for, so it is laid out over the driving relation first.
    const mlir::Value alignedChunk = rowAlignedChunk(mapValue(input), _innermostCardinality);

    // A type-erased column of tagged cells - what a list mixing types, holding a null
    // or holding nothing unwinds into - is folded as it stands: every cell carries its
    // own tag, so there is no one value type to read the column as.
    const mlir::Type alignedElement = mlir::cast<nl::ChunkType>(alignedChunk.getType()).getElementType();
    const bool taggedCells = mlir::isa<storage::ListElementType>(alignedElement);

    const mlir::Value inputChunk = taggedCells ? alignedChunk : nullableValueChunk(alignedChunk);

    mlir::MLIRContext* const context = _builder.getContext();
    const mlir::Location loc = _builder.getUnknownLoc();

    // A type-erased column is read as the tagged cell it holds; every other input is a
    // nullable value chunk, and the reduction is resolved from the value type it wraps.
    const mlir::Type inputChunkElement = mlir::cast<nl::ChunkType>(inputChunk.getType()).getElementType();
    const mlir::Type inputElement = taggedCells
        ? inputChunkElement
        : mlir::cast<storage::NullableType>(inputChunkElement).getValueType();

    // The accumulator (and result) element type: avg widens to f64, the rest keep
    // the input type. This also validates the reduction against the value type.
    const mlir::Type resultElement = aggregateResultElementType(_builder, kind, inputElement);

    // The accumulator is hoisted to the top of the entry block, above every loop,
    // so it is reset once at function scope and dominates the update in the
    // producing loop body and the emit that reads it after the loop. The count
    // sibling; a correlated aggregate (reset per enclosing step) would hoist into
    // its enclosing loop body instead - future work, as for the streaming limit.
    _builder.setInsertionPointToStart(_entryBlock);
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
    mlir::Block* const producingBlock = ownerBlock(inputChunk);
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
    const storage::NullableType resultNullable = storage::NullableType::get(context, resultElement);
    const nl::ChunkType resultChunkType = nl::ChunkType::get(context, resultNullable);

    setInsertionAfterProducingLoop(producingBlock);
    nl::AggregateResult aggregateResult = _builder.create<nl::AggregateResult>(loc, resultChunkType, state, kind);

    // The db aggregate's result maps to that chunk, so the db.output that follows
    // lowers into a function-scope nl.output reading it, exactly as db.count does.
    _valueMap[result] = aggregateResult.getResult();
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

        // A type-erased column of tagged cells is folded as it stands, like a count's
        // input: every cell carries its own tag, so there is no one value type to read
        // the column as.
        const mlir::Value aggregateChunk = chunks[chunkIndex];
        const mlir::Type aggregateElement = mlir::cast<nl::ChunkType>(aggregateChunk.getType()).getElementType();
        const bool taggedCells = mlir::isa<storage::ListElementType>(aggregateElement);

        if (reducesValues(kind) && !taggedCells) {
            chunks[chunkIndex] = nullableValueChunk(aggregateChunk);
        }
    }

    mlir::MLIRContext* const context = _builder.getContext();
    const mlir::Location loc = _builder.getUnknownLoc();

    // The accumulator - with its keyCount / aggregate-kind spec - is hoisted to the
    // top of the entry block, above every loop, so the group table exists before the
    // producing loop fills it and the handle dominates the update and the emit loop.
    // The grouped sibling of lowerSort's nl.sort_buffer.
    _builder.setInsertionPointToStart(_entryBlock);
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
    // input's value type and avg widens to f64 - all resolved here from each kind and
    // its input chunk.
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
    setInsertionInto(_entryBlock);
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

    // A constant collected column is folded over the rows of the group it falls in, not
    // over the single row it is, so it is laid out over the chunk the grouping keys are
    // read from - the driving relation when the collect has no keys. The aggregate
    // inputs beside it are laid out the same way, as lowerGroupAggregate lays its own.
    const mlir::Value cardinality = keyCount > 0 ? chunks.front() : _innermostCardinality;
    for (size_t inputIndex = keyCount; inputIndex < chunks.size(); inputIndex++) {
        chunks[inputIndex] = rowAlignedChunk(chunks[inputIndex], cardinality);
    }

    // An entity column collects as the IDs it carries, a list column as the cells it
    // holds, and a type-erased one as the tagged cells - all present in every row, the
    // tagged null among them, which the fold drops rather than the column. Only a scalar
    // value column is read as nullable, the way a property fetch is.
    for (size_t valueIndex = 0; valueIndex < valueCount; valueIndex++) {
        const size_t chunkIndex = keyCount + valueIndex;
        const mlir::Type collectedElement = mlir::cast<nl::ChunkType>(chunks[chunkIndex].getType()).getElementType();
        const bool collectsCellsPresentInEveryRow = mlir::isa<storage::NodeIDType,
                                                              storage::EdgeIDType,
                                                              storage::ListType,
                                                              storage::ListElementType>(collectedElement);

        if (!collectsCellsPresentInEveryRow) {
            chunks[chunkIndex] = nullableValueChunk(chunks[chunkIndex]);
        }
    }

    // A reduction beside the lists reads its input as a nullable value chunk, as it does
    // under a grouped aggregation; a count tallies rows and takes the chunk as it is, and
    // so does a type-erased column of tagged cells, which has no one value type to be read
    // as.
    for (size_t aggregateIndex = 0; aggregateIndex < kinds.size(); aggregateIndex++) {
        const auto kind = static_cast<storage::GroupAggregateKind>(kinds[aggregateIndex]);
        const size_t chunkIndex = keyCount + valueCount + aggregateIndex;

        const mlir::Type aggregateElement = mlir::cast<nl::ChunkType>(chunks[chunkIndex].getType()).getElementType();
        const bool taggedCells = mlir::isa<storage::ListElementType>(aggregateElement);

        if (reducesValues(kind) && !taggedCells) {
            chunks[chunkIndex] = nullableValueChunk(chunks[chunkIndex]);
        }
    }

    const mlir::Location loc = _builder.getUnknownLoc();

    // The accumulator is hoisted to the top of the entry block, above every loop, so
    // the group table exists before the producing loop fills it and the handle
    // dominates the update. The collect sibling of lowerGroupAggregate's buffer.
    _builder.setInsertionPointToStart(_entryBlock);
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
    setInsertionInto(ownerBlock(representative));
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

    setInsertionInto(_entryBlock);
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
    _builder.setInsertionPointToStart(_entryBlock);
    nl::CollectBuffer bufferOp = _builder.create<nl::CollectBuffer>(loc,
                                                                    keyCount,
                                                                    mlir::DenseI64ArrayAttr {},
                                                                    mlir::DenseI64ArrayAttr {});
    const mlir::Value state = bufferOp.getState();

    const mlir::Value representative = chunks.front();
    setInsertionInto(ownerBlock(representative));
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

    setInsertionInto(_entryBlock);
    nl::UnwindCollect unwindCollectOp = _builder.create<nl::UnwindCollect>(loc, iteratorType, state);
    buildLoopForSource(unwindCollectOp.getResult(), unwindCollect.getOperation());
}

void DBLowering::lowerCallProcedure(mlir::db::CallProcedure call) {
    const Procedure* procedure = procedureFor(call.getProcedure());

    // One column per written argument, in declaration order, so operand i is argument
    // i - the call cannot bind an argument by name. The optional arguments come last
    // and a call may stop short of them, leaving the procedure to read their slots as
    // unbound, so the required count is the floor rather than the count itself.
    const mlir::OperandRange inputs = call.getInputs();
    const ProcedureTypeVector& argumentTypes = procedure->argumentTypes();
    const size_t requiredCount = procedure->getRequiredArgumentCount();
    const bool tooFewArguments = inputs.size() < requiredCount;
    const bool tooManyArguments = inputs.size() > argumentTypes.size();
    if (tooFewArguments || tooManyArguments) {
        throw IRException("db.call_procedure of '" + call.getProcedure().str() + "' passes "
                          + std::to_string(inputs.size()) + " arguments, but the procedure declares "
                          + std::to_string(argumentTypes.size()) + ", "
                          + std::to_string(requiredCount) + " of them required");
    }

    // CallProcedure::verify guarantees one result per yielded name plus one per carried
    // column, with at least one yield, so the ranges below line up. Each yielded name
    // resolves to one of the procedure's declared return values, whose type is the
    // chunk the result is read as; an unknown name throws here.
    const mlir::ArrayAttr yields = call.getYields();
    llvm::SmallVector<mlir::Type, 4> chunkTypes;
    for (const mlir::Attribute yield : yields) {
        const llvm::StringRef name = mlir::cast<mlir::StringAttr>(yield).getValue();
        const std::string_view yieldName(name.data(), name.size());
        const size_t returnIndex = procedure->getReturnValueIndex(yieldName);
        const ProcedureType returnType = procedure->getReturnValueType(returnIndex);

        chunkTypes.push_back(procedureChunkType(_builder, returnType));
    }

    // Each carried column comes back with its own chunk type - the call replicates its
    // rows, it never retypes them - so the carried chunks follow the yields in both the
    // result types and the result mapping.
    const mlir::OperandRange carriedColumns = call.getCarriedColumns();

    // A carried row is replicated once per row the procedure emitted for it, which only
    // the procedure can say - it reports the input row behind each row it emits. One
    // that does not declare that report cannot be carried past at all, and this is where
    // that is settled: at plan time, rather than mid-execution once the rows fail to
    // line up.
    if (!carriedColumns.empty() && !procedure->hasIndices()) {
        throw IRException("db.call_procedure of '" + call.getProcedure().str()
                          + "' carries columns past it, but the procedure does not report the input"
                            " row of the rows it emits, so they could not be aligned with its"
                            " result");
    }

    llvm::SmallVector<mlir::Value, 4> carriedChunks;
    for (const mlir::Value carried : carriedColumns) {
        const mlir::Value carriedChunk = mapValue(carried);

        carriedChunks.push_back(carriedChunk);
        chunkTypes.push_back(carriedChunk.getType());
    }

    mlir::MLIRContext* const context = _builder.getContext();
    const mlir::Location loc = _builder.getUnknownLoc();

    // The call handle is hoisted to the top of the entry block, above every loop, so
    // the procedure is prepared once - its data allocated and its result columns
    // bound - before the loops that drive it, and the handle dominates every op that
    // names it. A correlated CALL (re-prepared per enclosing step) would hoist into
    // its enclosing loop body instead - future work, as for the streaming limit.
    _builder.setInsertionPointToStart(_entryBlock);
    const mlir::Value state = _builder.create<nl::Procedure>(loc, call.getProcedureAttr(), yields).getState();

    llvm::SmallVector<mlir::Value, 4> inputChunks;
    for (const mlir::Value input : inputs) {
        inputChunks.push_back(mapValue(input));
    }

    // A procedure is driven by a loop of its own, the way a scan or a hop is, and each
    // step of that nl.for runs the procedure once until it has answered that chunk of
    // arguments in full - so one chunk of arguments may yield many chunks of rows.
    //
    // The loop opens where the arguments are bound: the innermost producing loop body. A
    // call whose arguments are all loop-invariant - constants, or none at all - has no
    // producing loop to sit in, so it opens its loop where the current dataflow is
    // rooted, exactly as a scan does: the entry block at top level, or the outer factor's
    // innermost loop body inside a db.cross_product, so the factor nests under it.
    // Rooting such a call at the entry block instead would leave it outside the product's
    // nest, referring to chunks that do not dominate it.
    llvm::SmallVector<mlir::Value, 8> operandChunks(inputChunks.begin(), inputChunks.end());
    operandChunks.append(carriedChunks.begin(), carriedChunks.end());

    // Anchored on the deepest-bound operand, so a loop-bound argument or carried column
    // is found wherever it sits in the operand list, behind any hoisted constants.
    mlir::Value anchorChunk;
    for (const mlir::Value operandChunk : operandChunks) {
        if (!anchorChunk) {
            anchorChunk = operandChunk;
            continue;
        }

        mlir::Block* const block = deeperBlock(anchorChunk, operandChunk);
        if (ownerBlock(operandChunk) == block) {
            anchorChunk = operandChunk;
        }
    }

    mlir::Block* insertionBlock = _rootBlock;
    if (anchorChunk) {
        mlir::Block* const argumentBlock = ownerBlock(anchorChunk);

        // A factor shares no SSA value with its sibling, so an argument bound anywhere
        // but function scope is bound inside this factor's own nest.
        if (argumentBlock != _entryBlock) {
            insertionBlock = argumentBlock;
        }
    }

    setInsertionInto(insertionBlock);

    const nl::IteratorType iteratorType = nl::IteratorType::get(context, chunkTypes);
    nl::ProcedureInit init = _builder.create<nl::ProcedureInit>(loc,
                                                               iteratorType,
                                                               state,
                                                               inputChunks,
                                                               carriedChunks);

    // buildLoopForSource binds one loop variable per yielded return value and then per
    // carried column, and maps db.call_procedure's results to them, so the db.output that
    // follows lowers into the drive loop's body reading that step's rows.
    buildLoopForSource(init.getResult(), call.getOperation());
}

const Procedure* DBLowering::procedureFor(llvm::StringRef name) const {
    if (!_procedures) {
        throw IRException("db.call_procedure requires a procedure registry, but the lowering was "
                          "created without one");
    }

    const Procedure* procedure = _procedures->getProcedure(std::string_view(name.data(), name.size()));
    if (!procedure) {
        throw IRException("Procedure '" + name.str() + "' does not exist");
    }

    return procedure;
}

bool DBLowering::assignProducerLoops(mlir::Value column,
                                     mlir::Value handle,
                                     bool rowsDroppedBeforeTheCut) {
    mlir::Operation* const definingOp = column.getDefiningOp();
    if (!definingOp) {
        // A cross-product factor's loop variable is a block argument with no
        // defining op; its producing loop is reached through the factor's yield
        // in the cross-product branch below, not from here.
        return false;
    }

    const bool opensLoop = opensSourceLoop(definingOp);
    const bool isCrossProduct = mlir::isa<mlir::db::CrossProduct>(definingOp);

    // A pipeline breaker accumulates every row before emitting any, so the walk stops
    // here; the limit budgets its emit loop instead, when it opens one.
    const bool emitsThroughLoop = mlir::isa<mlir::db::Sort, mlir::db::GroupAggregate>(definingOp);
    const bool breaksPipeline = emitsThroughLoop || reducesToOneRow(definingOp);

    bool reachedALoop = opensLoop || isCrossProduct || emitsThroughLoop;

    // A loop's budget only stops it from taking another step, so bounding one never trims
    // the step it is in. A cross product's budget cuts the product itself, which stands
    // only while every row it makes reaches the output: an op below the cut that drops rows
    // - a filter, a skip, a dedup - would leave it discarding rows that would have
    // survived, and the cut short of its count. So the product keeps the handle only on a
    // path that drops nothing; its factor loops take it either way.
    // Declining the handle is not the same as reaching no loop: a cross product is still a
    // producer the walk found, so reachedALoop stands and the cut keeps its nest.
    const bool boundsCrossProduct = isCrossProduct && !rowsDroppedBeforeTheCut;
    const bool takesTheHandle = opensLoop || boundsCrossProduct || emitsThroughLoop;

    // The first limit, in program order, to claim a producer wins, so a loop
    // shared by two limits' nests carries the outer one and never two handles.
    if (takesTheHandle && !_loopLimitHandle.count(definingOp)) {
        _loopLimitHandle[definingOp] = handle;
    }

    if (breaksPipeline) {
        return reachedALoop;
    }

    const bool rowsDropped = rowsDroppedBeforeTheCut || dropsRows(definingOp);

    if (isCrossProduct) {
        // A cross product takes no column operands - its factors are regions - so
        // recurse through each factor's db.yield operands to reach the factor
        // scans/edge loops that produce the crossed columns.
        mlir::db::CrossProduct cross = mlir::cast<mlir::db::CrossProduct>(definingOp);
        mlir::Region* const factors[] = {&cross.getLeftFactor(), &cross.getRightFactor()};
        for (mlir::Region* const factor : factors) {
            mlir::Operation* const yield = factor->front().getTerminator();
            for (const mlir::Value yielded : yield->getOperands()) {
                reachedALoop |= assignProducerLoops(yielded, handle, rowsDropped);
            }
        }
    } else {
        // A non-loop producer (a property fetch) is traversed but not assigned -
        // it opens no loop - so its input chunk's loop is still reached.
        for (const mlir::Value operand : definingOp->getOperands()) {
            reachedALoop |= assignProducerLoops(operand, handle, rowsDropped);
        }
    }

    return reachedALoop;
}

void DBLowering::assignCardinalityDriverLoop(mlir::db::Limit limit, mlir::Value handle) {
    mlir::Operation* const limitOp = limit.getOperation();

    // The relation driving the projection is the loop opened last before the cut: its rows
    // are the ones nl.output emits the constants over, so they are the rows this budget
    // cuts. A reduction in between leaves no such loop - it emits its one row at function
    // scope, and the relation behind it had to be read in full to reduce it.
    // A row-dropping op only matters once the driver is behind it: what it drops are the
    // driver's rows, which is what bars a cross-product driver from the budget.
    mlir::Operation* driver = nullptr;
    bool rowsDropped = false;
    for (mlir::Operation& operation : *limitOp->getBlock()) {
        if (&operation == limitOp) {
            break;
        }

        if (opensRowLoop(&operation)) {
            driver = &operation;
            rowsDropped = false;
        } else if (reducesToOneRow(&operation)) {
            driver = nullptr;
        } else if (dropsRows(&operation)) {
            rowsDropped = true;
        }
    }

    if (!driver) {
        return;
    }

    assignProducerLoops(driver->getResult(0), handle, rowsDropped);
}

void DBLowering::foldTruncatesIntoOutputs(mlir::func::FuncOp nlFunction) {
    // Collect the foldable truncates first; erasing ops mid-walk is unsafe.
    llvm::SmallVector<nl::LimitTruncate, 4> foldable;

    nlFunction.walk([&](nl::LimitTruncate truncate) {
        const mlir::ResultRange results = truncate.getResults();

        // Foldable only if a single nl.output solely consumes every truncated
        // column; soleOutputConsumer returns that shared output (a null op if not).
        // Not const: the nl.output accessors below are non-const, as MLIR generates.
        nl::Output output = soleOutputConsumer(results);
        if (!output || output.getLimit()) {
            return;
        }

        // The output must consume exactly the truncate's results - all of them, in
        // the same order, and nothing else. The fold rebuilds the output over the
        // truncate's *input* columns in the truncate's order, so that swap only
        // preserves what the output emits when the two lists line up one-for-one:
        //   truncate (%a,%b)->(%ta,%tb) ; output(%ta,%tb)  folds to  output(%a,%b) limit %h
        //   output(%ta, %unrelated)  does not fold: %unrelated is no truncate result, it would be dropped
        //   output(%tb, %ta)         does not fold: rebuilt as output(%a,%b), it would swap the projection
        const mlir::OperandRange outputColumns = output.getColumns();
        if (outputColumns.size() != results.size()) {
            return;
        }

        for (size_t columnIndex = 0; columnIndex < results.size(); columnIndex++) {
            if (outputColumns[columnIndex] != results[columnIndex]) {
                return;
            }
        }

        foldable.push_back(truncate);
    });

    for (nl::LimitTruncate truncate : foldable) {
        nl::Output output = mlir::cast<nl::Output>(*truncate.getResult(0).user_begin());

        // Re-emit the output over the untruncated inputs, carrying the handle, so
        // it streams the emitThisStep prefix off the counter instead of a copy.
        // The preceding nl.limit_update still sets that count. Drop the old output
        // and the now-unused truncate.
        _builder.setInsertionPoint(output);
        _builder.create<nl::Output>(output.getLoc(),
                                    truncate.getColumns(),
                                    truncate.getState(),
                                    mlir::Value(),
                                    output.getCardinality(),
                                    output.getColumnNamesAttr());

        output.erase();
        truncate.erase();
    }
}

void DBLowering::foldSkipTruncatesIntoOutputs(mlir::func::FuncOp nlFunction) {
    // The skip sibling of foldTruncatesIntoOutputs: a terminal SKIP whose
    // nl.skip_truncate feeds only an nl.output folds into a skip-bearing output
    // that emits the surviving suffix in place (offset getSkipThisStep()) instead
    // of copying it to the front. Collect first; erasing ops mid-walk is unsafe.
    llvm::SmallVector<nl::SkipTruncate, 4> foldable;

    nlFunction.walk([&](nl::SkipTruncate truncate) {
        const mlir::ResultRange results = truncate.getResults();

        // Foldable only if a single nl.output solely consumes every truncated
        // column; soleOutputConsumer returns that shared output (a null op if not).
        // The single shared user also self-excludes the SKIP+LIMIT case: there an
        // nl.limit sits between this skip and the output, so the truncate's result
        // feeds nl.limit_update (and the limit's own consumer), never one nl.output
        // - the shared-user test fails and the skip stays a copy, bounded by the
        // limit's loop early-exit.
        // Not const: the nl.output accessors below are non-const, as MLIR generates.
        nl::Output output = soleOutputConsumer(results);

        // Bail if the output already carries a handle: a folded output carries at
        // most one of limit/skip, and the rebuild below would drop a pre-existing
        // one.
        if (!output || output.getLimit() || output.getSkip()) {
            return;
        }

        // The output must consume exactly the truncate's results - all of them, in
        // the same order, and nothing else - so rebuilding it over the truncate's
        // input columns preserves the projection. Same precondition as the limit
        // fold.
        const mlir::OperandRange outputColumns = output.getColumns();
        if (outputColumns.size() != results.size()) {
            return;
        }

        for (size_t columnIndex = 0; columnIndex < results.size(); columnIndex++) {
            if (outputColumns[columnIndex] != results[columnIndex]) {
                return;
            }
        }

        foldable.push_back(truncate);
    });

    for (nl::SkipTruncate truncate : foldable) {
        nl::Output output = mlir::cast<nl::Output>(*truncate.getResult(0).user_begin());

        // Re-emit the output over the untruncated inputs, carrying the skip handle
        // in the third operand, so it streams the surviving suffix off the counter
        // (offset getSkipThisStep(), getEmitThisStep() rows) instead of a copy. The
        // preceding nl.skip_update still sets that offset and count. Drop the old
        // output and the now-unused truncate.
        _builder.setInsertionPoint(output);
        _builder.create<nl::Output>(output.getLoc(),
                                    truncate.getColumns(),
                                    mlir::Value(),
                                    truncate.getState(),
                                    output.getCardinality(),
                                    output.getColumnNamesAttr());

        output.erase();
        truncate.erase();
    }
}

void DBLowering::lowerCreateNode(mlir::db::CreateNode createNode) {
    const mlir::Location loc = _builder.getUnknownLoc();

    llvm::SmallVector<mlir::Value, 4> propChunks;
    for (const mlir::Value propValue : createNode.getPropValues()) {
        propChunks.push_back(mapValue(propValue));
    }

    mlir::Value cardinalityChunk;
    if (createNode.getCardinality()) {
        cardinalityChunk = mapValue(createNode.getCardinality());
    }

    if (cardinalityChunk) {
        setInsertionInto(ownerBlock(cardinalityChunk));
    } else {
        mlir::Block* targetBlock = _entryBlock;
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
                if (candidateBlock != _entryBlock) {
                    targetBlock = candidateBlock;
                    break;
                }
            }
            if (targetBlock != _entryBlock) {
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

    mlir::Value reference = srcChunk;
    {
        mlir::Block* const block = deeperBlock(reference, tgtChunk);
        if (ownerBlock(tgtChunk) == block) {
            reference = tgtChunk;
        }
    }
    for (const mlir::Value propChunk : propChunks) {
        mlir::Block* const block = deeperBlock(reference, propChunk);
        if (ownerBlock(propChunk) == block) {
            reference = propChunk;
        }
    }
    setInsertionInto(ownerBlock(reference));

    nl::CreateEdge create = _builder.create<nl::CreateEdge>(
        loc,
        srcChunk,
        tgtChunk,
        createEdge.getEdgeTypeAttr(),
        createEdge.getPropNamesAttr(),
        propChunks,
        mapOptionalMask(createEdge.getSrcPending()),
        mapOptionalMask(createEdge.getTgtPending()));
    _valueMap[createEdge.getResult()] = create.getResult();
}

mlir::Value DBLowering::mapOptionalMask(mlir::Value mask) {
    if (!mask) {
        return mlir::Value();
    }

    return mapValue(mask);
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
    mapColumns(merge.getNodePropValues(), nodePropValues);
    mapColumns(merge.getEdgePropValues(), edgePropValues);
    mapColumns(merge.getCarriedColumns(), carriedColumns);

    // Inserted into the deepest block, where all the operands are defined. A merge over
    // literals alone reads no chunk, and so opens where the program does.
    mlir::Block* targetBlock = _entryBlock;
    mlir::Value insertionReference;
    for (const mlir::Value operand : merge->getOperands()) {
        const mlir::Value operandChunk = mapValue(operand);
        if (!insertionReference) {
            insertionReference = operandChunk;
            continue;
        }

        mlir::Block* const block = deeperBlock(insertionReference, operandChunk);
        if (ownerBlock(operandChunk) == block) {
            insertionReference = operandChunk;
        }
    }

    if (insertionReference) {
        targetBlock = ownerBlock(insertionReference);
    }

    setInsertionInto(targetBlock);

    mlir::MLIRContext* const context = _builder.getContext();
    const mlir::Type nodeChunkType = nl::ChunkType::get(context, storage::NodeIDType::get(context));
    const mlir::Type edgeChunkType = nl::ChunkType::get(context, storage::EdgeIDType::get(context));
    const mlir::Type maskChunkType = nl::ChunkType::get(context, storage::BoolType::get(context));

    const size_t nodeCount = merge.getNodeLabels().size();
    const size_t hopCount = nodeCount - 1;

    llvm::SmallVector<mlir::Type, 8> resultTypes;
    for (size_t nodeIndex = 0; nodeIndex < nodeCount; nodeIndex++) {
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
    const mlir::Value valueChunk = mapValue(setNodeProperty.getValue());

    mlir::Value reference = inputChunk;
    mlir::Block* const block = deeperBlock(reference, valueChunk);
    if (ownerBlock(valueChunk) == block) {
        reference = valueChunk;
    }
    setInsertionInto(ownerBlock(reference));

    _builder.create<nl::SetNodeProperty>(
        loc,
        inputChunk,
        setNodeProperty.getPropertyAttr(),
        valueChunk,
        mapOptionalMask(setNodeProperty.getPending()),
        mapOptionalMask(setNodeProperty.getRows()));
}

void DBLowering::lowerSetEdgeProperty(mlir::db::SetEdgeProperty setEdgeProperty) {
    const mlir::Location loc = _builder.getUnknownLoc();
    const mlir::Value inputChunk = mapValue(setEdgeProperty.getInputEdges());
    const mlir::Value valueChunk = mapValue(setEdgeProperty.getValue());

    mlir::Value reference = inputChunk;
    mlir::Block* const block = deeperBlock(reference, valueChunk);
    if (ownerBlock(valueChunk) == block) {
        reference = valueChunk;
    }
    setInsertionInto(ownerBlock(reference));

    _builder.create<nl::SetEdgeProperty>(
        loc,
        inputChunk,
        setEdgeProperty.getPropertyAttr(),
        valueChunk,
        mapOptionalMask(setEdgeProperty.getPending()),
        mapOptionalMask(setEdgeProperty.getRows()));
}

void DBLowering::lowerDeleteNode(mlir::db::DeleteNode deleteNode) {
    const mlir::Location loc = _builder.getUnknownLoc();
    const mlir::Value inputChunk = mapValue(deleteNode.getInputNodes());

    // The delete runs where its node chunk is live - the block that owns it.
    setInsertionInto(ownerBlock(inputChunk));

    _builder.create<nl::DeleteNode>(loc, inputChunk, deleteNode.getDetach());
}

void DBLowering::lowerDeleteEdge(mlir::db::DeleteEdge deleteEdge) {
    const mlir::Location loc = _builder.getUnknownLoc();
    const mlir::Value inputChunk = mapValue(deleteEdge.getInputEdges());

    setInsertionInto(ownerBlock(inputChunk));

    _builder.create<nl::DeleteEdge>(loc, inputChunk);
}

void DBLowering::lowerConstant(mlir::db::ConstantOp constant) {
    // Constants are loop-invariant so hoist
    _builder.setInsertionPointToStart(_entryBlock);

    nl::Constant nlConstant = _builder.create<nl::Constant>(_builder.getUnknownLoc(), constant.getValue());
    _valueMap[constant.getResult()] = nlConstant.getResult();
}

void DBLowering::lowerBroadcastConstant(mlir::db::BroadcastConstant broadcast) {
    const mlir::Value driver = broadcast.getDriver();
    const mlir::Value driverChunk = driver ? mapValue(driver) : mlir::Value();

    _valueMap[broadcast.getResult()] = rowAlignedChunk(mapValue(broadcast.getValue()), driverChunk);
}

mlir::Type DBLowering::binaryResultElement(BinaryResultKind kind,
                                           mlir::Type lhsType,
                                           mlir::Type rhsType) {
    mlir::MLIRContext* const ctx = _builder.getContext();
    const bool operandNullable = isNullableChunk(lhsType) || isNullableChunk(rhsType);

    switch (kind) {
        case BinaryResultKind::Boolean: {
            const mlir::Type boolElement = _builder.getI1Type();
            return operandNullable ? storage::NullableType::get(ctx, boolElement) : boolElement;
        }
        break;

        case BinaryResultKind::Numeric: {
            const NumericOperand lhs = numericOperand(lhsType);
            const NumericOperand rhs = numericOperand(rhsType);
            const mlir::Type promoted = promoteNumeric(_builder, lhs.numeric, rhs.numeric);
            return operandNullable ? storage::NullableType::get(ctx, promoted) : promoted;
        }
        break;

        case BinaryResultKind::Double: {
            numericOperand(lhsType);
            numericOperand(rhsType);

            const mlir::Type doubleElement = _builder.getF64Type();
            return operandNullable ? storage::NullableType::get(ctx, doubleElement) : doubleElement;
        }
        break;

        case BinaryResultKind::String: {
            const mlir::Type stringElement = storage::StringType::get(ctx);
            return operandNullable ? storage::NullableType::get(ctx, stringElement) : stringElement;
        }
        break;
    }

    bioassert(false, "Unhandled binary result kind");
}

template <typename NLOp>
void DBLowering::lowerBinaryOp(mlir::Operation& op, BinaryResultKind kind) {
    mlir::Value lhsChunk = mapValue(op.getOperand(0));
    mlir::Value rhsChunk = mapValue(op.getOperand(1));

    // x IS NULL over a plain scalar column meets kernels reading a nullable value column
    if (isUntypedNullChunk(rhsChunk.getType()) && !isNullableChunk(lhsChunk.getType())) {
        lhsChunk = nullableValueChunk(lhsChunk);
    } else if (isUntypedNullChunk(lhsChunk.getType()) && !isNullableChunk(rhsChunk.getType())) {
        rhsChunk = nullableValueChunk(rhsChunk);
    }

    const mlir::Type resultElement = binaryResultElement(kind, lhsChunk.getType(), rhsChunk.getType());
    const nl::ChunkType resultType = nl::ChunkType::get(_builder.getContext(), resultElement);

    setInsertionForBinaryOp(lhsChunk, rhsChunk);

    NLOp nlOp = _builder.create<NLOp>(_builder.getUnknownLoc(), resultType, lhsChunk, rhsChunk);
    _valueMap[op.getResult(0)] = nlOp.getResult();
}

void DBLowering::lowerNot(mlir::db::NotOp notOp) {
    const mlir::Value operandChunk = mapValue(notOp.getOperand());

    const mlir::Type operandType = operandChunk.getType();
    const bool resultNull = isNullableChunk(operandType);

    mlir::MLIRContext* bldCtxt = _builder.getContext();

    // A mask stays a mask through the negation - what nl.merge and the constraint checks
    // produce - while a predicate over values keeps the i1 element they carry
    const bool operandIsMask = isa<storage::BoolType>(mlir::cast<nl::ChunkType>(operandType).getElementType());
    const mlir::Type boolElement = operandIsMask ? storage::BoolType::get(bldCtxt)
                                                 : mlir::Type(_builder.getI1Type());

    mlir::Type resultElement = boolElement;
    if (resultNull) {
        resultElement = storage::NullableType::get(bldCtxt, boolElement);
    }

    const nl::ChunkType resultType = nl::ChunkType::get(bldCtxt, resultElement);

    mlir::Block* const insertBlock = ownerBlock(operandChunk);
    if (insertBlock != _entryBlock) {
        setInsertionInto(insertBlock);
    } else {
        mlir::Operation* const operandDef = operandChunk.getDefiningOp();
        if (operandDef) {
            _builder.setInsertionPointAfter(operandDef);
        } else {
            _builder.setInsertionPointToStart(_entryBlock);
        }
    }

    nl::Not nlNotOp = _builder.create<nl::Not>(_builder.getUnknownLoc(), resultType, operandChunk);
    _valueMap[notOp.getResult()] = nlNotOp.getResult();
}

void DBLowering::setInsertionForUnaryOp(mlir::Value operandChunk) {
    mlir::Block* const insertBlock = ownerBlock(operandChunk);
    if (insertBlock != _entryBlock) {
        setInsertionInto(insertBlock);
    } else {
        mlir::Operation* const operandDef = operandChunk.getDefiningOp();
        if (operandDef) {
            _builder.setInsertionPointAfter(operandDef);
        } else {
            _builder.setInsertionPointToStart(_entryBlock);
        }
    }
}

void DBLowering::lowerUnaryFunction(mlir::Operation* op) {
    const UnaryFunctionLowering* spec = lookupUnaryFunctionLowering(*op);
    bioassert(spec, "lowerUnaryFunction called on a non-function op");

    const mlir::Value inputChunk = mapValue(op->getOperand(0));

    const mlir::Type baseElement = spec->element(_builder);

    const bool inputNullable = isNullableChunk(inputChunk.getType());
    const bool alwaysNull = spec->nullability == ResultNullability::AlwaysNullable;
    const bool specNull = spec->nullability == ResultNullability::FollowsInput && inputNullable;
    const bool resultNullable = alwaysNull || specNull;

    mlir::Type resultElement = baseElement;
    if (resultNullable) {
        resultElement = storage::NullableType::get(_builder.getContext(), baseElement);
    }

    const nl::ChunkType resultType = nl::ChunkType::get(_builder.getContext(), resultElement);

    setInsertionForUnaryOp(inputChunk);

    _valueMap[op->getResult(0)] = spec->emit(_builder, _builder.getUnknownLoc(), resultType, inputChunk);
}

void DBLowering::lowerBinaryFunction(mlir::Operation* op) {
    const BinaryFunctionLowering* spec = lookupBinaryFunctionLowering(*op);
    bioassert(spec, "lowerBinaryFunction called on a non-function op");

    const mlir::Value lhsChunk = mapValue(op->getOperand(0));
    const mlir::Value rhsChunk = mapValue(op->getOperand(1));

    const mlir::Type baseElement = spec->element(_builder);
    mlir::Type resultElement = baseElement;
    if (isNullableChunk(lhsChunk.getType()) || isNullableChunk(rhsChunk.getType())) {
        resultElement = storage::NullableType::get(_builder.getContext(), baseElement);
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

mlir::Block* DBLowering::deeperBlock(mlir::Value first, mlir::Value second) {
    mlir::Block* const firstBlock = ownerBlock(first);
    mlir::Block* const secondBlock = ownerBlock(second);
    if (firstBlock == secondBlock) {
        return firstBlock;
    }

    if (firstBlock == _entryBlock) {
        return secondBlock;
    } else if (secondBlock == _entryBlock) {
        return firstBlock;
    }

    throw IRException("db operands to deeperBlock must be bound in the same loop");
}

void DBLowering::lowerOutput(mlir::db::Output output) {
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
    // (e.g. a loop block), then set the anchor to be that block. Otherwise, we have no
    // loops, i.e. we are in a MATCH (n) RETURN 5 case, where the output can just be in
    // the entry block since it is independent of any loop (over n in this case).
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

    // Custom cardinality calculation for e.g. MATCH (n) RETURN 5
    mlir::Value cardinality;
    // Output would not normally be in a loop body, but toplevel
    const bool returningNonLooped = anchorBlock == _entryBlock;
    if (returningNonLooped && _innermostLoopBody) {
        // but if we have all constants, then it need be moved to the inner most loop to
        // match cardinality. An expression over constants alone is one of them: it is
        // bound where its operands are, above the loop whose rows it is projected over
        const bool allConstants = llvm::all_of(columns, [](mlir::Value column) { return yieldsConstantColumn(column); });

        if (allConstants) {
            anchorBlock = _innermostLoopBody;
            cardinality = _innermostCardinality;
        }
    }

    setInsertionInto(anchorBlock);
    _builder.create<nl::Output>(_builder.getUnknownLoc(),
                                columns,
                                mlir::Value(),
                                mlir::Value(),
                                cardinality,
                                output.getColumnNamesAttr());
}

void DBLowering::buildLoopForSource(mlir::Value iterator, mlir::Operation* dbOp) {
    // The handle this loop carries, or null when it produces no limited column -
    // giving the iterator-only builder's unbounded loop. A producing loop carries
    // its limit's handle so the break unwinds the producing nest; a consumer loop
    // (lowered after the limit) is never in the map and stays unbounded.
    const mlir::Value limitHandle = _loopLimitHandle.lookup(dbOp);
    nl::For forLoop = _builder.create<nl::For>(_builder.getUnknownLoc(), iterator, limitHandle);

    // The loop binds one variable per chunk the iterator produces, in the same
    // order as the db op's result columns: a scan binds its single node chunk;
    // an edge fetch binds sources, edge IDs, edge type IDs, targets, then one
    // filtered chunk per carried column. Recording db result -> loop variable
    // lets a later op find the chunk each column lowered to.
    mlir::Block* loopBody = forLoop.getBody();

    // This is the innermost loop opened so far in the current factor (loops
    // nest in dataflow order), so a cross product nests at this body.
    _innermostLoopBody = loopBody;

    // A loop over a call that binds no return value has no variable, so there is no chunk
    // here for a constant projection to be sized against.
    _innermostCardinality = loopBody->getNumArguments() > 0 ? loopBody->getArgument(0)
                                                           : mlir::Value();

    const mlir::ResultRange dbResults = dbOp->getResults();
    for (size_t resultIndex = 0; resultIndex < dbResults.size(); resultIndex++) {
        _valueMap[dbResults[resultIndex]] = loopBody->getArgument(static_cast<unsigned>(resultIndex));
    }
}

void DBLowering::setInsertionInto(mlir::Block* block) {
    // Every home block already has a terminator - the entry block's func.return
    // or a loop body's implicit nl.yield - so the next op goes just before it,
    // after any siblings already lowered here.
    _builder.setInsertionPoint(block->getTerminator());
}

void DBLowering::setInsertionAfterProducingLoop(mlir::Block* updateBlock) {
    if (updateBlock == _entryBlock) {
        setInsertionInto(_entryBlock);
        return;
    }

    mlir::Operation* enclosing = updateBlock->getParentOp();
    while (enclosing->getBlock() != _entryBlock) {
        enclosing = enclosing->getBlock()->getParentOp();
    }

    _builder.setInsertionPointAfter(enclosing);
}

void DBLowering::setInsertionForBinaryOp(mlir::Value lhs, mlir::Value rhs) {
    // If both operands are top-level constants (in `_entryBlock`), then place the op
    // after the latter defined constant. Otherwise place the op in the more deeply nested
    // block.
    mlir::Block* const insertBlock = deeperBlock(lhs, rhs);

    if (insertBlock != _entryBlock) {
        setInsertionInto(insertBlock);
        return;
    }

    const mlir::Operation* lhsDef = lhs.getDefiningOp();
    const mlir::Operation* rhsDef = rhs.getDefiningOp();
    const size_t defsToFind = (lhsDef == rhsDef) ? 1 : 2;

    // Walk the entry block to find which of the two defining ops appears later —
    // the new op must go after that one to stay before any nl.for that follows.
    // Each op appears once in the block, so stop as soon as both defs are seen.
    mlir::Operation* lastDef = nullptr;
    size_t defsFound = 0;
    for (mlir::Operation& op : *_entryBlock) {
        if (&op == lhsDef || &op == rhsDef) {
            lastDef = &op;
            defsFound++;

            if (defsFound == defsToFind) {
                break;
            }
        }
    }

    if (lastDef) {
        _builder.setInsertionPointAfter(lastDef);
    } else {
        _builder.setInsertionPointToStart(_entryBlock);
    }
}

mlir::Value DBLowering::mapValue(mlir::Value dbValue) const {
    const auto slotIt = _valueMap.find(dbValue);
    if (slotIt == _valueMap.end()) {
        throw IRException("db column used before the operation that produces it");
    }

    return slotIt->second;
}

void DBLowering::followCardinalityThrough(mlir::ValueRange inputChunks, mlir::ValueRange resultChunks) {
    if (!_innermostCardinality) {
        return;
    }

    // Any chunk of the driver's relation stands for it: a cut trimmed down to a column other
    // than the driver itself still narrows the relation the driver counts.
    for (size_t chunkIndex = 0; chunkIndex < inputChunks.size(); chunkIndex++) {
        if (!rowAlignedWith(inputChunks[chunkIndex], _innermostCardinality)) {
            continue;
        }

        _innermostCardinality = resultChunks[chunkIndex];
        return;
    }
}

void DBLowering::rowAlignCutChunks(llvm::SmallVectorImpl<mlir::Value>& chunks) {
    // With no relation driving the projection a constant is the single row that
    // projection is, and the cut charges it as it stands.
    if (!_innermostCardinality) {
        return;
    }

    for (mlir::Value& chunk : chunks) {
        chunk = rowAlignedChunk(chunk, _innermostCardinality);
    }
}

// A scalar a procedure yielded is a plain value chunk, while a reduction, a collect or a
// comparison with null read a nullable one: the plain chunk is read as nullable right
// where it is bound, so those kernels take it as they take a property value.
mlir::Value DBLowering::nullableValueChunk(mlir::Value chunk) {
    const auto chunkType = mlir::cast<nl::ChunkType>(chunk.getType());
    const mlir::Type element = chunkType.getElementType();
    if (mlir::isa<storage::NullableType>(element)) {
        return chunk;
    }

    mlir::Type valueElement = element;
    if (mlir::isa<storage::BoolType>(element)) {
        valueElement = _builder.getI1Type();
    }

    const bool isString = mlir::isa<storage::StringType>(valueElement);
    const bool isDouble = mlir::isa<mlir::Float64Type>(valueElement);
    const bool isInteger = mlir::isa<mlir::IntegerType>(valueElement);
    if (!isString && !isDouble && !isInteger) {
        throw IRException("Only a scalar value column can be read as a nullable value column");
    }

    mlir::MLIRContext* const context = _builder.getContext();
    const storage::NullableType nullableType = storage::NullableType::get(context, valueElement);
    const nl::ChunkType resultType = nl::ChunkType::get(context, nullableType);

    mlir::OpBuilder::InsertionGuard guard(_builder);
    if (mlir::Operation* const definingOp = chunk.getDefiningOp()) {
        _builder.setInsertionPointAfter(definingOp);
    } else {
        _builder.setInsertionPointToStart(mlir::cast<mlir::BlockArgument>(chunk).getOwner());
    }

    return _builder.create<nl::ToNullable>(_builder.getUnknownLoc(), resultType, chunk).getResult();
}

mlir::Value DBLowering::cardinalityDriver(llvm::ArrayRef<mlir::Value> chunks) const {
    for (const mlir::Value chunk : chunks) {
        if (!yieldsConstantColumn(chunk)) {
            return chunk;
        }
    }

    return _innermostCardinality;
}

mlir::Value DBLowering::rowAlignedChunk(mlir::Value chunk, mlir::Value cardinality) {
    // A chunk that carries rows is its own alignment
    if (!yieldsConstantColumn(chunk)) {
        return chunk;
    }

    const auto chunkType = mlir::cast<nl::ChunkType>(chunk.getType());
    mlir::Type valueElement = chunkType.getElementType();
    if (const auto nullable = mlir::dyn_cast<storage::NullableType>(valueElement)) {
        valueElement = nullable.getValueType();
    }

    // The rows are laid out as a nullable value chunk - present in every row - which is
    // what every fold, key serialization and reduction reads a value column as. A list is
    // laid out as the list chunk an nl.collect drain emits instead: a cell is a view over
    // the query's list buffer, which is never absent, and no nullable list column exists.
    mlir::MLIRContext* const context = _builder.getContext();
    const bool isList = llvm::isa<storage::ListType>(valueElement);
    const mlir::Type resultElement = isList ? valueElement
                                            : storage::NullableType::get(context, valueElement);
    const nl::ChunkType resultType = nl::ChunkType::get(context, resultElement);

    // With no relation driving the projection the value is laid out where the constant
    // itself is bound, over the single row that projection is
    mlir::Block* const homeBlock = cardinality ? ownerBlock(cardinality) : ownerBlock(chunk);

    setInsertionInto(homeBlock);
    nl::BroadcastConstant broadcast = _builder.create<nl::BroadcastConstant>(_builder.getUnknownLoc(), resultType, chunk, cardinality);

    return broadcast.getResult();
}

size_t DBLowering::blockNestingDepth(mlir::Block* block) {
    size_t depth = 0;
    for (mlir::Operation* parent = block->getParentOp(); parent; parent = parent->getParentOp()) {
        depth++;
    }

    return depth;
}

mlir::Block* DBLowering::ownerBlock(mlir::Value chunkValue) {
    // A lowered chunk is either an nl.for loop variable (a block argument) or a
    // chunk produced in place by a property fetch (an op result); either way the
    // block that holds it is the loop body a consumer must nest into.
    if (const mlir::BlockArgument blockArgument = mlir::dyn_cast<mlir::BlockArgument>(chunkValue)) {
        return blockArgument.getOwner();
    }

    return chunkValue.getDefiningOp()->getBlock();
}
