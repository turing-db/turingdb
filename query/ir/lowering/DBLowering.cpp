#include "DBLowering.h"

#include <algorithm>
#include <array>
#include <memory>
#include <mlir/IR/Location.h>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

#include <spdlog/fmt/fmt.h>

#include "mlir/IR/Block.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/Types.h"
#include "mlir/IR/Verifier.h"

#include "NLOps.h"

#include "IRConstantColumn.h"
#include "IRRowAlignment.h"
#include "MergePatternShape.h"
#include "Procedure.h"
#include "ProcedureManager.h"
#include "ProcedureTypeVector.h"

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

// The name a result column's value type goes out under. A rejection is read by whoever
// wrote the query, so it names the type the value has rather than the chunk spelling it
// is carried in.
void describeColumnType(mlir::Type chunkType, std::string& out) {
    out.clear();

    mlir::Type element = mlir::cast<nl::ChunkType>(chunkType).getElementType();
    if (const auto nullable = mlir::dyn_cast<storage::NullableType>(element)) {
        element = nullable.getValueType();
    }

    if (mlir::isa<storage::StringType>(element) || mlir::isa<storage::OwnedStringType>(element)) {
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

using NLUnaryFunctionEmitter = mlir::Value (*)(mlir::OpBuilder& builder,
                                               mlir::Location loc,
                                               nl::ChunkType resultType,
                                               mlir::Value input);
using UnaryFunctionElement = mlir::Type (*)(mlir::OpBuilder& builder, mlir::Type inputElement);

template <typename NLOp>
mlir::Value emitNLUnaryFunction(mlir::OpBuilder& builder,
                                mlir::Location loc,
                                nl::ChunkType resultType,
                                mlir::Value input) {
    return builder.create<NLOp>(loc, resultType, input).getResult();
}

mlir::Type ownedStringFunctionElement(mlir::OpBuilder& builder, mlir::Type inputElement) {
    return storage::OwnedStringType::get(builder.getContext());
}

mlir::Type nodeIDFunctionElement(mlir::OpBuilder& builder, mlir::Type inputElement) {
    return storage::NodeIDType::get(builder.getContext());
}

mlir::Type labelListFunctionElement(mlir::OpBuilder& builder, mlir::Type inputElement) {
    mlir::MLIRContext* const context = builder.getContext();
    return storage::ListType::get(context, storage::StringType::get(context));
}

mlir::Type integerFunctionElement(mlir::OpBuilder& builder, mlir::Type inputElement) {
    return builder.getI64Type();
}

mlir::Type floatFunctionElement(mlir::OpBuilder& builder, mlir::Type inputElement) {
    return builder.getF64Type();
}

mlir::Type booleanFunctionElement(mlir::OpBuilder& builder, mlir::Type inputElement) {
    return builder.getI1Type();
}

mlir::Type dateTimeFunctionElement(mlir::OpBuilder& builder, mlir::Type inputElement) {
    return storage::DateTimeType::get(builder.getContext());
}

// A list function reads a list cell, or the type-erased cell an unwind of a list of lists
// hands its nested lists on as. Anything else is IR no query produces.
void throwIfNotAListInput(mlir::Type inputElement) {
    if (!llvm::isa<storage::ListType, storage::ListElementType>(inputElement)) {
        throw IRException("a list function reads a list column");
    }
}

mlir::Type sizeFunctionElement(mlir::OpBuilder& builder, mlir::Type inputElement) {
    const bool readsAList = llvm::isa<storage::ListType, storage::ListElementType>(inputElement);
    const bool readsAString = llvm::isa<storage::StringType, storage::OwnedStringType>(inputElement);

    if (!readsAList && !readsAString) {
        throw IRException("size() and length() read a list column or a string column");
    }

    return builder.getI64Type();
}

// A stored list may mix the types of its elements, so one read out of a cell is the
// type-erased tagged scalar a heterogeneous list's elements ride, whatever element type
// the cell's own column resolved to.
mlir::Type listElementFunctionElement(mlir::OpBuilder& builder, mlir::Type inputElement) {
    throwIfNotAListInput(inputElement);

    return storage::ListElementType::get(builder.getContext());
}

// The list a tail leaves is over the elements the list it came from held, so it keeps that
// list's own element type. A list read out of a tagged cell names no element type of its
// own, so what is left of it is a list of tagged cells.
mlir::Type listTailFunctionElement(mlir::OpBuilder& builder, mlir::Type inputElement) {
    throwIfNotAListInput(inputElement);

    if (llvm::isa<storage::ListElementType>(inputElement)) {
        return storage::ListType::get(builder.getContext(), inputElement);
    }

    return inputElement;
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

enum class ResultNullability {
    FollowsInput,
    AlwaysNullable,
    NeverNullable,
};

struct UnaryFunctionLowering {
    NLUnaryFunctionEmitter emit {nullptr};
    UnaryFunctionElement element {nullptr};
    ResultNullability nullability {ResultNullability::FollowsInput};
};

const std::unordered_map<std::string_view, UnaryFunctionLowering> unaryFunctionLowerings = {
    // Both read an entity column, which carries its null in the ID an OPTIONAL MATCH left
    // invalid rather than in an optional, so their result is nullable whatever the input
    // chunk's own type says
    {"db.labels",     {&emitNLUnaryFunction<nl::Labels>,    &labelListFunctionElement,   ResultNullability::AlwaysNullable}},
    {"db.edge_type",  {&emitNLUnaryFunction<nl::EdgeType>,  &ownedStringFunctionElement, ResultNullability::AlwaysNullable}},

    // An end of an edge is a node, and a node column spells its null as an invalid ID, so
    // the result stays a plain node chunk where labels() and type() need a nullable one
    {"db.start_node", {&emitNLUnaryFunction<nl::StartNode>,  &nodeIDFunctionElement,      ResultNullability::NeverNullable}},
    {"db.end_node",   {&emitNLUnaryFunction<nl::EndNode>,    &nodeIDFunctionElement,      ResultNullability::NeverNullable}},

    {"db.to_integer", {&emitNLUnaryFunction<nl::ToInteger>, &integerFunctionElement,     ResultNullability::AlwaysNullable}},
    {"db.to_float",   {&emitNLUnaryFunction<nl::ToFloat>,   &floatFunctionElement,       ResultNullability::AlwaysNullable}},
    {"db.to_boolean", {&emitNLUnaryFunction<nl::ToBoolean>, &booleanFunctionElement,     ResultNullability::AlwaysNullable}},
    {"db.to_datetime", {&emitNLUnaryFunction<nl::ToDateTime>, &dateTimeFunctionElement,   ResultNullability::AlwaysNullable}},

    // A calendar field of an instant is always readable, so the field is null exactly
    // where the instant it was read off is
    {"db.datetime_year",        {&emitNLUnaryFunction<nl::DateTimeYear>,        &integerFunctionElement, ResultNullability::FollowsInput}},
    {"db.datetime_month",       {&emitNLUnaryFunction<nl::DateTimeMonth>,       &integerFunctionElement, ResultNullability::FollowsInput}},
    {"db.datetime_day",         {&emitNLUnaryFunction<nl::DateTimeDay>,         &integerFunctionElement, ResultNullability::FollowsInput}},
    {"db.datetime_hour",        {&emitNLUnaryFunction<nl::DateTimeHour>,        &integerFunctionElement, ResultNullability::FollowsInput}},
    {"db.datetime_minute",      {&emitNLUnaryFunction<nl::DateTimeMinute>,      &integerFunctionElement, ResultNullability::FollowsInput}},
    {"db.datetime_second",      {&emitNLUnaryFunction<nl::DateTimeSecond>,      &integerFunctionElement, ResultNullability::FollowsInput}},
    {"db.datetime_millisecond", {&emitNLUnaryFunction<nl::DateTimeMillisecond>, &integerFunctionElement, ResultNullability::FollowsInput}},
    {"db.datetime_microsecond", {&emitNLUnaryFunction<nl::DateTimeMicrosecond>, &integerFunctionElement, ResultNullability::FollowsInput}},

    {"db.to_string",  {&emitNLUnaryFunction<nl::ToString>,  &ownedStringFunctionElement, ResultNullability::FollowsInput}},
    {"db.element_id", {&emitNLUnaryFunction<nl::ElementID>,  &integerFunctionElement,     ResultNullability::AlwaysNullable}},
    {"db.size",       {&emitNLUnaryFunction<nl::Size>,      &sizeFunctionElement,        ResultNullability::FollowsInput}},
    {"db.head",       {&emitNLUnaryFunction<nl::Head>,      &listElementFunctionElement, ResultNullability::NeverNullable}},
    {"db.last",       {&emitNLUnaryFunction<nl::Last>,      &listElementFunctionElement, ResultNullability::NeverNullable}},
    {"db.tail",       {&emitNLUnaryFunction<nl::Tail>,      &listTailFunctionElement,    ResultNullability::FollowsInput}}
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

mlir::Type procedureElementType(mlir::OpBuilder& builder, ProcedureType procedureType) {
    mlir::MLIRContext* const context = builder.getContext();

    switch (procedureType) {
        case ProcedureType::NODE:
            return storage::NodeIDType::get(context);
        break;

        case ProcedureType::EDGE:
            return storage::EdgeIDType::get(context);
        break;

        case ProcedureType::LABEL_ID:
            return storage::LabelIDType::get(context);
        break;

        case ProcedureType::EDGE_TYPE_ID:
            return storage::EdgeTypeIDType::get(context);
        break;

        case ProcedureType::PROPERTY_TYPE_ID:
            return storage::PropertyTypeIDType::get(context);
        break;

        case ProcedureType::VALUE_TYPE:
            return storage::ValueTypeType::get(context);
        break;

        case ProcedureType::UINT_64:
            return builder.getIntegerType(64, /*isSigned=*/false);
        break;

        case ProcedureType::INT64:
            return builder.getIntegerType(64);
        break;

        case ProcedureType::DOUBLE:
            return builder.getF64Type();
        break;

        case ProcedureType::BOOL:
            return builder.getI1Type();
        break;

        case ProcedureType::STRING_VIEW:
            return storage::StringType::get(context);
        break;

        case ProcedureType::STRING:
            return storage::OwnedStringType::get(context);
        break;

        case ProcedureType::LIST:
            return storage::ListType::get(context, mlir::NoneType::get(context));
        break;

        case ProcedureType::MAP:
            throw IRException("Unsupported procedure return type: MAP");
        break;

        case ProcedureType::INVALID:
        case ProcedureType::_SIZE:
            throw IRException("Invalid procedure value type");
        break;
    }

    throw IRException("Unhandled procedure value type");
}

// A return value the procedure declared nullable is read as a nullable chunk over that
// element type - storage's ColumnOptVector - so a row the procedure has no value for is
// null rather than a value standing in for one.
nl::ChunkType procedureChunkType(mlir::OpBuilder& builder, const NamedProcedureType& returnValue) {
    mlir::MLIRContext* const context = builder.getContext();
    const mlir::Type elementType = procedureElementType(builder, returnValue._type);

    if (returnValue._nullable) {
        return nl::ChunkType::get(context, storage::NullableType::get(context, elementType));
    }

    return nl::ChunkType::get(context, elementType);
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
    const bool isString = mlir::isa<storage::StringType, storage::OwnedStringType>(inputElement);
    const bool isDateTime = mlir::isa<storage::DateTimeType>(inputElement);
    const bool isTaggedCell = mlir::isa<storage::ListElementType>(inputElement);

    // An untyped null holds no value to reduce - a name no property in the graph carries,
    // or the null literal - so every reduction over it sees nothing: min, max and avg
    // answer null and sum answers 0. It names no value type either, so the answer rides
    // the integer column an untyped null is laid out over anywhere else, except avg's,
    // which is a float whatever it reduced.
    if (mlir::isa<mlir::NoneType>(inputElement)) {
        if (kind == storage::AggregateKind::Avg) {
            return builder.getF64Type();
        }

        return builder.getIntegerType(64);
    }

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
            // numbers, strings, bools and instants - but an embedding has none.
            if (!isNumeric && !isString && !isBool && !isDateTime) {
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

    // A type-erased cell is numeric only once read, and its tag names a type per row
    // rather than one for the column: it computes in the f64 its mixed numeric tags land
    // on, as a reduction over cells does, and answers null on a row holding no number.
    if (mlir::isa<storage::ListElementType>(element)) {
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

// The value element a chunk carries, with its nullability stripped and a mask read as the
// i1 it holds: what two chunks are compared on to see whether they carry the same values.
mlir::Type chunkValueElement(mlir::OpBuilder& builder, mlir::Type chunkType) {
    mlir::Type element = mlir::cast<nl::ChunkType>(chunkType).getElementType();
    if (const auto nullable = mlir::dyn_cast<storage::NullableType>(element)) {
        element = nullable.getValueType();
    }

    if (mlir::isa<storage::BoolType>(element)) {
        return builder.getI1Type();
    }

    return element;
}

// Whether an element type is one arithmetic promotes: the two Cypher number columns.
bool isNumericElement(mlir::Type element) {
    if (mlir::isa<mlir::Float64Type>(element)) {
        return true;
    }

    const auto integerType = mlir::dyn_cast<mlir::IntegerType>(element);

    return integerType && integerType.getWidth() == 64;
}

bool isNullableChunk(mlir::Type chunkType) {
    const nl::ChunkType chunk = mlir::dyn_cast<nl::ChunkType>(chunkType);
    if (!chunk) {
        throw IRException("Tried to check nullity of non-chunk.");
    }

    return mlir::isa<storage::NullableType>(chunk.getElementType());
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

// A list column, read through the nullable a stored list wears: a property fetch produces
// its values nullable, since a node carrying no list reads as absent there.
bool isListChunk(mlir::Type chunkType) {
    const nl::ChunkType chunk = mlir::dyn_cast<nl::ChunkType>(chunkType);
    if (!chunk) {
        return false;
    }

    const mlir::Type element = chunk.getElementType();
    const auto nullable = mlir::dyn_cast<storage::NullableType>(element);
    const mlir::Type listed = nullable ? nullable.getValueType() : element;

    return mlir::isa<storage::ListType>(listed);
}

bool isIndexableChunk(mlir::Type chunkType) {
    const nl::ChunkType chunk = mlir::dyn_cast<nl::ChunkType>(chunkType);
    if (!chunk) {
        return false;
    }

    const mlir::Type element = chunk.getElementType();
    const auto nullable = mlir::dyn_cast<storage::NullableType>(element);
    const mlir::Type indexed = nullable ? nullable.getValueType() : element;

    return mlir::isa<storage::ListType, storage::ListElementType>(indexed);
}

// The element type an indexed list hands out, read through the nullable an optional list
// wears. None when the indexed operand is a type-erased cell rather than a list.
mlir::Type indexedListElementType(mlir::Type chunkType) {
    const nl::ChunkType chunk = mlir::dyn_cast<nl::ChunkType>(chunkType);
    if (!chunk) {
        return {};
    }

    const mlir::Type element = chunk.getElementType();
    const auto nullable = mlir::dyn_cast<storage::NullableType>(element);
    const mlir::Type indexed = nullable ? nullable.getValueType() : element;

    const auto listType = mlir::dyn_cast<storage::ListType>(indexed);
    return listType ? listType.getElementType() : mlir::Type {};
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
// the scalars a value column holds.
bool namesAnIndexedValueType(mlir::Type element) {
    if (!element) {
        return false;
    }

    if (const auto integerType = mlir::dyn_cast<mlir::IntegerType>(element)) {
        return integerType.getWidth() == 1 || integerType.getWidth() == 64;
    }

    return mlir::isa<mlir::Float64Type, storage::StringType, storage::EmbeddingType>(element);
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

// The null literal's chunk: nullable with no value type of its own.
bool isUntypedNullChunk(mlir::Type chunkType) {
    const nl::ChunkType chunk = mlir::dyn_cast<nl::ChunkType>(chunkType);
    if (!chunk) {
        return false;
    }

    const auto nullableType = mlir::dyn_cast<storage::NullableType>(chunk.getElementType());
    return nullableType && mlir::isa<mlir::NoneType>(nullableType.getValueType());
}

bool isTaggedCellChunk(mlir::Type chunkType) {
    const nl::ChunkType chunk = mlir::dyn_cast<nl::ChunkType>(chunkType);
    return chunk && mlir::isa<storage::ListElementType>(chunk.getElementType());
}

// The same cell behind the nullable an index wraps it in: reading an element out of a list
// answers no cell where the row has no such element, and a cell carrying its own tag where
// it has one
bool holdsTaggedCells(mlir::Type chunkType) {
    const nl::ChunkType chunk = mlir::dyn_cast<nl::ChunkType>(chunkType);
    if (!chunk) {
        return false;
    }

    const mlir::Type element = chunk.getElementType();
    const auto nullable = mlir::dyn_cast<storage::NullableType>(element);

    return mlir::isa<storage::ListElementType>(nullable ? nullable.getValueType() : element);
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
                     mlir::db::ScanNodesByPropertyValue,
                     mlir::db::ScanEdges,
                     mlir::db::ScanEdgesByType,
                     mlir::db::ScanOutEdgesByLabelSrc,
                     mlir::db::ScanInEdgesByLabelTgt,
                     mlir::db::ScanOutEdgesByLabelTgt,
                     mlir::db::ScanInEdgesByLabelSrc,
                     mlir::db::GetOutEdges,
                     mlir::db::GetInEdges,
                     mlir::db::GetEdges,
                     mlir::db::GetOutEdgesByType,
                     mlir::db::GetInEdgesByType,
                     mlir::db::GetOutEdgesByLabel,
                     mlir::db::GetInEdgesByLabel,
                     mlir::db::ExplorePaths,
                     mlir::db::CallProcedure>(operation);
}

// A body whose own dataflow cannot keep the input rows paired with what it makes of them
// runs one input row at a time, whatever it ends on
bool runsPerRow(mlir::Operation* operation) {
    if (mlir::db::CallSubquery call = mlir::dyn_cast<mlir::db::CallSubquery>(operation)) {
        return !call.getCarriesScope();
    } else if (mlir::db::ExistsSubquery exists = mlir::dyn_cast<mlir::db::ExistsSubquery>(operation)) {
        return !exists.getCarriesScope();
    }

    return false;
}

// The columns an op holding a body hands it through its block arguments, empty for an op
// holding none
mlir::OperandRange subqueryInputColumns(mlir::Operation* operation) {
    if (mlir::db::CallSubquery call = mlir::dyn_cast<mlir::db::CallSubquery>(operation)) {
        return call.getInputColumns();
    } else if (mlir::db::ExistsSubquery exists = mlir::dyn_cast<mlir::db::ExistsSubquery>(operation)) {
        return exists.getInputColumns();
    }

    return mlir::OperandRange(operation->operand_end(), operation->operand_end());
}

// The innermost body run one row at a time that holds the op, or null when none does
mlir::Operation* nearestPerRowSubquery(mlir::Operation* operation) {
    for (mlir::Operation* parent = operation->getParentOp(); parent; parent = parent->getParentOp()) {
        if (runsPerRow(parent)) {
            return parent;
        }
    }

    return nullptr;
}

// The db ops whose rows a projection is emitted over: a source, the nest a cross product
// or a hash join builds, and the emit loop a pipeline breaker opens over what it
// accumulated
bool opensRowLoop(mlir::Operation* operation) {
    const bool returningSubquery = mlir::isa<mlir::db::CallSubquery>(operation)
                                   && operation->getNumResults() > 0;

    // An EXISTS answering for the rows in flight opens no loop of its own; one run a row
    // at a time opens the loop over those rows, which the ops after it are emitted into.
    const bool perRowExists = mlir::isa<mlir::db::ExistsSubquery>(operation) && runsPerRow(operation);

    return opensSourceLoop(operation)
        || returningSubquery
        || perRowExists
        || mlir::isa<mlir::db::CrossProduct,
                     mlir::db::HashJoin,
                     mlir::db::Sort,
                     mlir::db::GroupAggregate,
                     mlir::db::OptionalMatch>(operation);
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
// are fewer than the rows a producer above it made. A unit body writes and yields nothing,
// so the rows come out of it as they went in.
bool dropsRows(mlir::Operation* operation) {
    mlir::db::CallSubquery call = mlir::dyn_cast<mlir::db::CallSubquery>(operation);
    if (call) {
        return !call.getUnit();
    }

    return mlir::isa<mlir::db::FilterOp,
                     mlir::db::HashJoin,
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
    setInsertionToEntryBlockStart();
    _builder.create<mlir::func::ReturnOp>(loc);

    // Lower each operation of the db function. Top-level scans root their loop
    // in the entry block; a cross product retargets the root per factor.
    _valueMap.clear();
    _propertyTypes.clear();
    _edgeTypeSets.clear();
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

    // Run MLIR verifier on the nlFunction
    if (mlir::failed(mlir::verify(nlFunction))) {
        throw IRException("DBLowering produced an invalid nl function");
    }

    return nlFunction;
}

void DBLowering::hoistLimitHandles(mlir::Region& region, mlir::Block* hoistBlock, mlir::Operation* holder) {
    const mlir::Location loc = _builder.getUnknownLoc();

    // Pre-scan for db.limits before any loop is built: nl.for's limit operand is
    // fixed at build time, so each handle must exist first to be threaded in, and
    // which loops a handle attaches to must be known up front. A limit fused into
    // a sort's top-K carries no streaming handle, so it is left out here - and so is
    // one inside a body run one row at a time, which that body hoists into its own
    // step, so the budget resets per input row.
    llvm::SmallVector<mlir::db::Limit, 2> limits;
    region.walk([&](mlir::db::Limit limit) {
        const bool fused = _fusedLimits.count(limit.getOperation());
        const bool heldHere = nearestPerRowSubquery(limit.getOperation()) == holder;

        if (!fused && heldHere) {
            limits.push_back(limit);
        }
    });

    // Hoist one nl.limit handle per db.limit to the top of the hoist block, where
    // each dominates the loops, the update and the truncate that read it.
    if (!limits.empty()) {
        _builder.setInsertionPointToStart(hoistBlock);
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
        _producerWalkVisits.clear();

        bool producedByALoop = false;
        for (const mlir::Value column : limit.getColumns()) {
            producedByALoop |= assignProducerLoops(column, handle, /*rowsDroppedBeforeTheCut=*/false, holder);
        }

        // A cut over constants alone walks back to no loop at all - the constants are
        // bound above the nest - so the handle goes to the relation driving the
        // projection instead. Without it the nest runs to its end and the budget only
        // stops the output, producing every row to throw all but k away.
        if (!producedByALoop) {
            assignCardinalityDriverLoop(limit, handle, holder);
        }
    }
}

void DBLowering::lowerOperation(mlir::Operation& operation) {
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
    } else if (mlir::db::MakeList makeList = mlir::dyn_cast<mlir::db::MakeList>(operation)) {
        lowerMakeList(makeList);
    } else if (mlir::db::MakeMap makeMap = mlir::dyn_cast<mlir::db::MakeMap>(operation)) {
        lowerMakeMap(makeMap);
    } else if (mlir::db::Range range = mlir::dyn_cast<mlir::db::Range>(operation)) {
        lowerRange(range);
    } else if (mlir::db::ListSlice listSlice = mlir::dyn_cast<mlir::db::ListSlice>(operation)) {
        lowerListSlice(listSlice);
    } else if (mlir::db::ListComprehension listComprehension = mlir::dyn_cast<mlir::db::ListComprehension>(operation)) {
        lowerListComprehension(listComprehension);
    } else if (mlir::db::PatternComprehension patternComprehension = mlir::dyn_cast<mlir::db::PatternComprehension>(operation)) {
        lowerPatternComprehension(patternComprehension);
    } else if (mlir::db::ScanEdges scanEdges = mlir::dyn_cast<mlir::db::ScanEdges>(operation)) {
        lowerScanEdges(scanEdges);
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
    } else if (mlir::db::ExplorePaths explorePaths = mlir::dyn_cast<mlir::db::ExplorePaths>(operation)) {
        lowerExplorePaths(explorePaths);
    } else if (mlir::db::ExpandPath expandPath = mlir::dyn_cast<mlir::db::ExpandPath>(operation)) {
        lowerExpandPath(expandPath);
    } else if (mlir::db::PathLength pathLength = mlir::dyn_cast<mlir::db::PathLength>(operation)) {
        lowerPathLength(pathLength);
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
    } else if (mlir::db::DistinctSet distinctSet = mlir::dyn_cast<mlir::db::DistinctSet>(operation)) {
        lowerDistinctSet(distinctSet);
    } else if (mlir::db::OptionalMatch optionalMatch = mlir::dyn_cast<mlir::db::OptionalMatch>(operation)) {
        lowerOptionalMatch(optionalMatch);
    } else if (mlir::db::CallSubquery callSubquery = mlir::dyn_cast<mlir::db::CallSubquery>(operation)) {
        lowerCallSubquery(callSubquery);
    } else if (mlir::db::ExistsSubquery existsSubquery = mlir::dyn_cast<mlir::db::ExistsSubquery>(operation)) {
        lowerExistsSubquery(existsSubquery);
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
    } else if (mlir::db::ConstantOp constant = mlir::dyn_cast<mlir::db::ConstantOp>(operation)) {
        lowerConstant(constant);
    } else if (mlir::db::CurrentDateTime currentDateTime = mlir::dyn_cast<mlir::db::CurrentDateTime>(operation)) {
        lowerCurrentDateTime(currentDateTime);
    } else if (mlir::db::BroadcastConstant broadcast = mlir::dyn_cast<mlir::db::BroadcastConstant>(operation)) {
        lowerBroadcastConstant(broadcast);
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
    } else if (mlir::isa<mlir::db::InOp>(operation)) {
        lowerBinaryOp<nl::In>(operation, BinaryResultKind::Membership);
    } else if (mlir::isa<mlir::db::AndOp>(operation)) {
        lowerBinaryOp<nl::And>(operation, BinaryResultKind::Boolean);
    } else if (mlir::isa<mlir::db::OrOp>(operation)) {
        lowerBinaryOp<nl::Or>(operation, BinaryResultKind::Boolean);
    } else if (mlir::isa<mlir::db::XorOp>(operation)) {
        lowerBinaryOp<nl::Xor>(operation, BinaryResultKind::Boolean);
    } else if (mlir::db::NotOp notOp = mlir::dyn_cast<mlir::db::NotOp>(operation)) {
        lowerNot(notOp);
    } else if (mlir::db::Case caseOp = mlir::dyn_cast<mlir::db::Case>(operation)) {
        lowerCase(caseOp);
    } else if (mlir::db::FilterOp filter = mlir::dyn_cast<mlir::db::FilterOp>(operation)) {
        lowerFilter(filter);
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

void DBLowering::lowerScanNodesByPropertyValue(mlir::db::ScanNodesByPropertyValue scanNodesByPropertyValue) {
    setInsertionInto(_rootBlock);

    nl::ScanNodesByPropertyValue nodes = _builder.create<nl::ScanNodesByPropertyValue>(_builder.getUnknownLoc(),
                                                                                       scanNodesByPropertyValue.getPropertyAttr(),
                                                                                       scanNodesByPropertyValue.getValue(),
                                                                                       scanNodesByPropertyValue.getLabelsAttr());
    buildLoopForSource(nodes.getResult(), scanNodesByPropertyValue.getOperation());
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
    // A cell that may be absent - what an index into a list hands back - contributes no
    // row where it is absent, so what the drain hands on is the tagged scalar itself.
    const auto nullableSource = mlir::dyn_cast<storage::NullableType>(sourceElement);
    const bool drainsATaggedCell = nullableSource
                                && mlir::isa<storage::ListElementType>(nullableSource.getValueType());

    if (drainsATaggedCell) {
        return nullableSource.getValueType();
    }

    // A list read out of a property rides a nullable chunk, the way every property value
    // does; its elements are the list's all the same, and a row holding no list drains
    // into no row rather than into a null.
    const mlir::Type unwrapped = nullableSource ? nullableSource.getValueType() : sourceElement;

    // Any source but a list keeps the column it already rides - its cells are the
    // elements, and a tagged cell holding a list gives up tagged scalars again.
    const auto listType = mlir::dyn_cast<storage::ListType>(unwrapped);
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

void DBLowering::lowerListComprehension(mlir::db::ListComprehension comprehension) {
    const mlir::Location loc = _builder.getUnknownLoc();
    mlir::MLIRContext* const context = _builder.getContext();

    llvm::SmallVector<mlir::Value, 4> carriedChunks;
    for (const mlir::Value carriedColumn : comprehension.getColumnsToFilter()) {
        carriedChunks.push_back(mapValue(carriedColumn));
    }

    // A constant source holds one cell standing for every row rather than one per row, so
    // it is laid out over the rows it is read against - as lowerUnwind lays its own out
    const mlir::Value cardinality = cardinalityDriver(carriedChunks);
    const mlir::Value sourceChunk = rowAlignedChunk(mapValue(comprehension.getSource()), cardinality);

    const mlir::Type sourceElement = mlir::cast<nl::ChunkType>(sourceChunk.getType()).getElementType();

    const nl::ChunkType rowTagType = nl::ChunkType::get(context,
                                                        _builder.getIntegerType(64, /*isSigned=*/false));

    llvm::SmallVector<mlir::Type, 4> argumentTypes {
        nl::ChunkType::get(context, unwoundElementType(context, sourceElement)),
        rowTagType
    };
    llvm::SmallVector<mlir::Location, 4> argumentLocations {loc, loc};

    for (const mlir::Value carriedChunk : carriedChunks) {
        argumentTypes.push_back(carriedChunk.getType());
        argumentLocations.push_back(loc);
    }

    llvm::SmallVector<mlir::Value, 8> operandChunks {sourceChunk};
    operandChunks.append(carriedChunks.begin(), carriedChunks.end());

    setInsertionForNaryOp(operandChunks);

    // What the body yields names the type of the lists, and that is only known once the
    // body is lowered. The op is created before it all the same, with the source's type
    // standing in: an op in the body reading a chunk bound above it takes the deeper of
    // the two blocks, which only holds when the body region hangs under them.
    nl::ListComprehension lists = _builder.create<nl::ListComprehension>(loc,
                                                                         sourceChunk.getType(),
                                                                         sourceChunk,
                                                                         carriedChunks);

    mlir::Block* const bodyBlock = &lists.getBody().emplaceBlock();
    bodyBlock->addArguments(argumentTypes, argumentLocations);

    const mlir::Value elementChunk = bodyBlock->getArgument(0);

    // Every insertion into a block goes before its terminator, so the body needs one
    // before anything is lowered into it. This one stands for the yield the lowered body
    // decides, which replaces it below.
    _builder.setInsertionPointToEnd(bodyBlock);
    mlir::Operation* const placeholderYield =
        _builder.create<nl::ComprehensionYield>(loc, bodyBlock->getArgument(1), elementChunk);

    mlir::Block& dbBodyBlock = comprehension.getBody().front();
    for (size_t argumentIndex = 0; argumentIndex < argumentTypes.size(); argumentIndex++) {
        const unsigned index = static_cast<unsigned>(argumentIndex);
        _valueMap[dbBodyBlock.getArgument(index)] = bodyBlock->getArgument(index);
    }

    // The rows the body computes over are the elements of one row's list, so a constant it
    // reads is laid out over those rather than over the rows the comprehension is read on
    mlir::Block* const previousInnermostLoopBody = _innermostLoopBody;
    const mlir::Value previousInnermostCardinality = _innermostCardinality;
    _innermostLoopBody = bodyBlock;
    _innermostCardinality = elementChunk;

    mlir::Value rowTagChunk;
    mlir::Value valueChunk;

    for (mlir::Operation& operation : dbBodyBlock) {
        mlir::db::ComprehensionYield yield = mlir::dyn_cast<mlir::db::ComprehensionYield>(operation);
        if (!yield) {
            lowerOperation(operation);
            continue;
        }

        rowTagChunk = mapValue(yield.getRowTags());
        valueChunk = mapValue(yield.getValue());
    }

    _innermostLoopBody = previousInnermostLoopBody;
    _innermostCardinality = previousInnermostCardinality;

    // ComprehensionYield::verify guarantees both, so an empty one here means unverified
    // IR - the defensive backstop lowerOptionalMatch keeps too.
    if (!rowTagChunk || !valueChunk) {
        throw IRException("db.list_comprehension yields no value");
    }

    // A value computed from constants alone holds one cell standing for every element, so
    // it is laid out over the elements it is read against - the ones the tag counts, since
    // a WHERE has cut both together
    valueChunk = rowAlignedChunk(valueChunk, rowTagChunk);

    // An entity ID, a list, a tagged cell and a CSV field's owned characters are present
    // in every row and go into the list as they stand; only a scalar value column is read
    // as nullable, the way lowerMakeList reads the columns it builds from
    const mlir::Type valueElement = mlir::cast<nl::ChunkType>(valueChunk.getType()).getElementType();
    const bool holdsCellsPresentInEveryRow = mlir::isa<storage::NodeIDType,
                                                       storage::EdgeIDType,
                                                       storage::ListType,
                                                       storage::MapType,
                                                       storage::ListElementType,
                                                       storage::OwnedStringType>(valueElement);

    if (!holdsCellsPresentInEveryRow) {
        valueChunk = nullableValueChunk(valueChunk);
    }

    _builder.setInsertionPointToEnd(bodyBlock);
    _builder.create<nl::ComprehensionYield>(loc, rowTagChunk, valueChunk);
    placeholderYield->erase();

    const mlir::Type listType = storage::ListType::get(context, listedElementType(context, valueChunk));
    const nl::ChunkType resultType = nl::ChunkType::get(context,
                                                        storage::NullableType::get(context, listType));

    mlir::Value listsChunk = lists.getResult();
    listsChunk.setType(resultType);

    _valueMap[comprehension.getResult()] = listsChunk;
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
    nl::PatternComprehensionBuffer buffer = _builder.create<nl::PatternComprehensionBuffer>(loc, cardinality);
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

    // A value computed from constants alone holds one cell standing for every match, so it
    // is laid out over the matches it is read against - the ones the tag counts, since a
    // WHERE has cut both together
    valueChunk = rowAlignedChunk(valueChunk, rowTagChunk ? rowTagChunk : _innermostCardinality);

    // An entity ID, a list, a tagged cell and a CSV field's owned characters are present
    // in every row and go into the list as they stand; only a scalar value column is read
    // as nullable, the way lowerMakeList reads the columns it builds from
    const mlir::Type valueElement = mlir::cast<nl::ChunkType>(valueChunk.getType()).getElementType();
    const bool holdsCellsPresentInEveryRow = mlir::isa<storage::NodeIDType,
                                                       storage::EdgeIDType,
                                                       storage::ListType,
                                                       storage::ListElementType,
                                                       storage::OwnedStringType>(valueElement);

    if (!holdsCellsPresentInEveryRow) {
        valueChunk = nullableValueChunk(valueChunk);
    }

    llvm::SmallVector<mlir::Value, 2> stagedChunks {valueChunk};
    if (rowTagChunk) {
        stagedChunks.push_back(rowTagChunk);
    }

    setInsertionInto(deepestOwnerBlock(stagedChunks, stepBlock));
    _builder.create<nl::PatternComprehensionCollect>(loc, state, rowTagChunk, valueChunk);

    _rootBlock = previousRoot;
    _innermostLoopBody = previousInnermostLoopBody;
    _innermostCardinality = previousInnermostCardinality;

    // Every row gets a list, the empty one where the pattern matched nothing, so the
    // lists are a container chunk rather than the nullable one a list comprehension builds
    const mlir::Type listType = storage::ListType::get(context, listedElementType(context, valueChunk));
    const nl::ChunkType resultType = nl::ChunkType::get(context, listType);

    setInsertionInto(stepBlock);
    nl::PatternComprehension lists = _builder.create<nl::PatternComprehension>(loc,
                                                                               resultType,
                                                                               state,
                                                                               cardinality);

    _valueMap[comprehension.getResult()] = lists.getResult();
}

mlir::Type DBLowering::listedElementType(mlir::MLIRContext* context, llvm::ArrayRef<mlir::Value> chunks) {
    mlir::Type shared;

    for (const mlir::Value chunk : chunks) {
        mlir::Type element = mlir::cast<nl::ChunkType>(chunk.getType()).getElementType();

        // A cell that is absent is a tagged null of the list its neighbours are in, so a
        // nullable column names the type of the value it holds.
        if (const auto nullable = mlir::dyn_cast<storage::NullableType>(element)) {
            element = nullable.getValueType();
        }

        if (mlir::isa<mlir::NoneType>(element)) {
            continue;
        }

        // A column owning its characters puts a view of them in the list, the same string
        // a borrowed column puts there, so the two agree
        if (mlir::isa<storage::OwnedStringType>(element)) {
            element = storage::StringType::get(context);
        }

        if (!shared) {
            shared = element;
        } else if (shared != element) {
            return storage::ListElementType::get(context);
        }
    }

    if (!shared) {
        return mlir::NoneType::get(context);
    }

    return shared;
}

void DBLowering::containerCellChunks(mlir::ValueRange columns, llvm::SmallVectorImpl<mlir::Value>& chunks) {
    for (const mlir::Value column : columns) {
        chunks.push_back(mapValue(column));
    }

    // An element holding one value for every row rather than one per row is laid out over
    // the rows the others carry, so every cell of a container is read at the same row index.
    const mlir::Value cardinality = cardinalityDriver(chunks);

    for (mlir::Value& chunk : chunks) {
        chunk = rowAlignedChunk(chunk, cardinality);

        // An entity ID, a list, a map, a tagged cell and a CSV field's owned characters are
        // present in every row and go into the container as they stand; only a scalar value
        // column is read as nullable, the way lowerCollect reads the column it gathers.
        const mlir::Type element = mlir::cast<nl::ChunkType>(chunk.getType()).getElementType();
        const bool holdsCellsPresentInEveryRow = mlir::isa<storage::NodeIDType,
                                                           storage::EdgeIDType,
                                                           storage::ListType,
                                                           storage::MapType,
                                                           storage::ListElementType,
                                                           storage::OwnedStringType>(element);

        if (!holdsCellsPresentInEveryRow) {
            chunk = nullableValueChunk(chunk);
        }
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

void DBLowering::lowerListSlice(mlir::db::ListSlice slice) {
    const mlir::Location loc = _builder.getUnknownLoc();
    mlir::MLIRContext* const context = _builder.getContext();

    mlir::Value listChunk = mapValue(slice.getList());

    llvm::SmallVector<mlir::Value, 3> operands {listChunk};

    mlir::Value fromChunk = slice.getFrom() ? mapValue(slice.getFrom()) : mlir::Value {};
    mlir::Value toChunk = slice.getTo() ? mapValue(slice.getTo()) : mlir::Value {};

    if (fromChunk) {
        operands.push_back(fromChunk);
    }

    if (toChunk) {
        operands.push_back(toChunk);
    }

    // A list or a bound holding one value for every row rather than one per row is laid
    // out over the rows the others carry, as lowerRange lays its bounds out
    const mlir::Value cardinality = cardinalityDriver(operands);

    listChunk = rowAlignedChunk(listChunk, cardinality);

    if (fromChunk) {
        fromChunk = nullableValueChunk(rowAlignedChunk(fromChunk, cardinality));
    }

    if (toChunk) {
        toChunk = nullableValueChunk(rowAlignedChunk(toChunk, cardinality));
    }

    // The slice holds the elements the sliced value holds, and is read as nullable: a row
    // whose list or whose bound is null gets no slice. Slicing a type-erased cell answers
    // the list its tag says it holds, whose elements are cells in their turn
    const mlir::Type listElement = mlir::cast<nl::ChunkType>(listChunk.getType()).getElementType();
    const auto nullable = mlir::dyn_cast<storage::NullableType>(listElement);
    const mlir::Type value = nullable ? nullable.getValueType() : listElement;

    const mlir::Type sliced =
        mlir::isa<storage::ListElementType>(value)
            ? storage::ListType::get(context, storage::ListElementType::get(context))
            : value;

    const nl::ChunkType resultType = nl::ChunkType::get(context,
                                                        storage::NullableType::get(context, sliced));

    // The op goes where the chunks it reads stand, which are the laid-out ones
    operands.clear();
    operands.push_back(listChunk);

    if (fromChunk) {
        operands.push_back(fromChunk);
    }

    if (toChunk) {
        operands.push_back(toChunk);
    }

    setInsertionForNaryOp(operands);

    nl::ListSlice run = _builder.create<nl::ListSlice>(loc, resultType, listChunk, fromChunk, toChunk);

    _valueMap[slice.getResult()] = run.getResult();
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
        bound = nullableValueChunk(rowAlignedChunk(bound, cardinality));
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
    // block, with the type names hoisted into the nl.get_edge_type_set handle the
    // by-type hops already share.
    const mlir::Value edgeTypeHandle = getOrCreateEdgeTypeSetHandle(scanEdgesByType.getEdgeTypes());

    setInsertionInto(_rootBlock);

    nl::ScanEdgesByType edges = _builder.create<nl::ScanEdgesByType>(_builder.getUnknownLoc(), edgeTypeHandle);
    buildLoopForSource(edges.getResult(), scanEdgesByType.getOperation());
}

void DBLowering::lowerScanOutEdgesByLabelSrc(mlir::db::ScanOutEdgesByLabelSrc scanOutEdgesByLabelSrc) {
    // The by-label sibling of lowerScanEdges: same placement at the top of the root
    // block, with the label list forwarded as-is to the nl op - translation resolves
    // the names against the schema, as it does for nl.scan_nodes_by_label.
    setInsertionInto(_rootBlock);

    nl::ScanOutEdgesByLabelSrc edges = _builder.create<nl::ScanOutEdgesByLabelSrc>(_builder.getUnknownLoc(),
                                                                                   scanOutEdgesByLabelSrc.getLabelsAttr());
    buildLoopForSource(edges.getResult(), scanOutEdgesByLabelSrc.getOperation());
}

void DBLowering::lowerScanInEdgesByLabelTgt(mlir::db::ScanInEdgesByLabelTgt scanInEdgesByLabelTgt) {
    // The reverse-direction sibling of lowerScanOutEdgesByLabelSrc, placed and forwarded
    // the same way.
    setInsertionInto(_rootBlock);

    nl::ScanInEdgesByLabelTgt edges = _builder.create<nl::ScanInEdgesByLabelTgt>(_builder.getUnknownLoc(),
                                                                                 scanInEdgesByLabelTgt.getLabelsAttr());
    buildLoopForSource(edges.getResult(), scanInEdgesByLabelTgt.getOperation());
}

void DBLowering::lowerScanOutEdgesByLabelTgt(mlir::db::ScanOutEdgesByLabelTgt scanOutEdgesByLabelTgt) {
    setInsertionInto(_rootBlock);

    nl::ScanOutEdgesByLabelTgt edges = _builder.create<nl::ScanOutEdgesByLabelTgt>(_builder.getUnknownLoc(),
                                                                                   scanOutEdgesByLabelTgt.getLabelsAttr());
    buildLoopForSource(edges.getResult(), scanOutEdgesByLabelTgt.getOperation());
}

void DBLowering::lowerScanInEdgesByLabelSrc(mlir::db::ScanInEdgesByLabelSrc scanInEdgesByLabelSrc) {
    setInsertionInto(_rootBlock);

    nl::ScanInEdgesByLabelSrc edges = _builder.create<nl::ScanInEdgesByLabelSrc>(_builder.getUnknownLoc(),
                                                                                 scanInEdgesByLabelSrc.getLabelsAttr());
    buildLoopForSource(edges.getResult(), scanInEdgesByLabelSrc.getOperation());
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
    const mlir::Value edgeTypeHandle = getOrCreateEdgeTypeSetHandle(getOutEdgesByType.getEdgeTypes());

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
    // direction, edge types hoisted into the same nl.get_edge_type_set handle.
    const mlir::Value inputChunk = mapValue(getInEdgesByType.getInputNodes());
    const mlir::Value edgeTypeHandle = getOrCreateEdgeTypeSetHandle(getInEdgesByType.getEdgeTypes());

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

void DBLowering::lowerGetOutEdgesByLabel(mlir::db::GetOutEdgesByLabel getOutEdgesByLabel) {
    const mlir::Value inputChunk = mapValue(getOutEdgesByLabel.getInputNodes());

    llvm::SmallVector<mlir::Value, 4> carriedChunks;
    for (const mlir::Value carriedColumn : getOutEdgesByLabel.getColumnsToFilter()) {
        carriedChunks.push_back(mapValue(carriedColumn));
    }

    setInsertionInto(ownerBlock(inputChunk));

    // The label list rides on the op the way a by-label scan carries it, rather than
    // through a handle: it is resolved to a LabelSet when the loop is translated.
    nl::GetOutEdgesByLabel edges = _builder.create<nl::GetOutEdgesByLabel>(_builder.getUnknownLoc(),
                                                                           inputChunk,
                                                                           getOutEdgesByLabel.getLabelsAttr(),
                                                                           carriedChunks);
    buildLoopForSource(edges.getResult(), getOutEdgesByLabel.getOperation());
}

void DBLowering::lowerGetInEdgesByLabel(mlir::db::GetInEdgesByLabel getInEdgesByLabel) {
    // The predecessor counterpart of lowerGetOutEdgesByLabel: same shape, reverse
    // direction, the labels constraining the source the hop leaves.
    const mlir::Value inputChunk = mapValue(getInEdgesByLabel.getInputNodes());

    llvm::SmallVector<mlir::Value, 4> carriedChunks;
    for (const mlir::Value carriedColumn : getInEdgesByLabel.getColumnsToFilter()) {
        carriedChunks.push_back(mapValue(carriedColumn));
    }

    setInsertionInto(ownerBlock(inputChunk));

    nl::GetInEdgesByLabel edges = _builder.create<nl::GetInEdgesByLabel>(_builder.getUnknownLoc(),
                                                                         inputChunk,
                                                                         getInEdgesByLabel.getLabelsAttr(),
                                                                         carriedChunks);
    buildLoopForSource(edges.getResult(), getInEdgesByLabel.getOperation());
}

void DBLowering::lowerGetNodeProperties(mlir::db::GetNodeProperties getNodeProperties) {
    const mlir::Value inputChunk = mapValue(getNodeProperties.getInputNodes());
    const llvm::StringRef property = getNodeProperties.getProperty();

    // Resolve the name once, hoisted above the loops, and bake the value type.
    const mlir::Value handle = getOrCreatePropertyTypeHandle(property);
    const mlir::Type valueChunkType = propertyValueChunkType(property, columnType(getNodeProperties.getResult()));

    // A property read maps the input chunk in place, one value per node, so the
    // fetch nests in the loop that binds that chunk - it opens no loop of its own.
    setInsertionInto(ownerBlock(inputChunk));

    nl::GetNodeProperties fetch = _builder.create<nl::GetNodeProperties>(_builder.getUnknownLoc(),
                                                                         valueChunkType,
                                                                         inputChunk,
                                                                         handle,
                                                                         mapOptionalMask(getNodeProperties.getPending()),
                                                                         getNodeProperties.getAllPending());
    _valueMap[getNodeProperties.getResult()] = fetch.getValues();
}

void DBLowering::lowerGetEdgeProperties(mlir::db::GetEdgeProperties getEdgeProperties) {
    const mlir::Value inputChunk = mapValue(getEdgeProperties.getInputEdges());
    const llvm::StringRef property = getEdgeProperties.getProperty();

    const mlir::Value handle = getOrCreatePropertyTypeHandle(property);
    const mlir::Type valueChunkType = propertyValueChunkType(property, columnType(getEdgeProperties.getResult()));

    setInsertionInto(ownerBlock(inputChunk));

    nl::GetEdgeProperties fetch = _builder.create<nl::GetEdgeProperties>(_builder.getUnknownLoc(),
                                                                         valueChunkType,
                                                                         inputChunk,
                                                                         handle,
                                                                         mapOptionalMask(getEdgeProperties.getPending()),
                                                                         getEdgeProperties.getAllPending());
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

void DBLowering::lowerGetEdgeTypes(mlir::db::GetEdgeTypes getEdgeTypes) {
    const mlir::Value inputChunk = mapValue(getEdgeTypes.getInputEdges());

    setInsertionInto(ownerBlock(inputChunk));

    const mlir::Type edgeTypeIDChunkType = nl::ChunkType::get(
        _builder.getContext(),
        storage::EdgeTypeIDType::get(_builder.getContext()));

    nl::GetEdgeTypes fetch = _builder.create<nl::GetEdgeTypes>(
        _builder.getUnknownLoc(),
        edgeTypeIDChunkType,
        inputChunk);

    _valueMap[getEdgeTypes.getResult()] = fetch.getEdgeTypeIds();
}

void DBLowering::lowerCheckLabelConstraint(mlir::db::CheckLabelConstraint checkLabelConstraint) {
    const mlir::Value inputChunk = mapValue(checkLabelConstraint.getLabelsetIds());

    setInsertionInto(ownerBlock(inputChunk));

    const mlir::Type boolChunkType = nl::ChunkType::get(
        _builder.getContext(),
        storage::BoolType::get(_builder.getContext()));

    nl::CheckLabelConstraint check = _builder.create<nl::CheckLabelConstraint>(
        _builder.getUnknownLoc(),
        boolChunkType,
        inputChunk,
        checkLabelConstraint.getLabels());

    _valueMap[checkLabelConstraint.getResult()] = check.getResult();
}

void DBLowering::lowerCheckEdgeTypeConstraint(mlir::db::CheckEdgeTypeConstraint checkEdgeTypeConstraint) {
    const mlir::Value inputChunk = mapValue(checkEdgeTypeConstraint.getEdgeTypeIds());

    setInsertionInto(ownerBlock(inputChunk));

    const mlir::Type boolChunkType = nl::ChunkType::get(
        _builder.getContext(),
        storage::BoolType::get(_builder.getContext()));

    nl::CheckEdgeTypeConstraint check = _builder.create<nl::CheckEdgeTypeConstraint>(
        _builder.getUnknownLoc(),
        boolChunkType,
        inputChunk,
        checkEdgeTypeConstraint.getEdgeTypes());

    _valueMap[checkEdgeTypeConstraint.getResult()] = check.getResult();
}

mlir::Block* DBLowering::deepestOwnerBlock(llvm::ArrayRef<mlir::Value> chunks, mlir::Block* fallback) {
    mlir::Block* deepest = fallback;

    for (const mlir::Value chunk : chunks) {
        deepest = deeperOfBlocks(deepest, ownerBlock(chunk));
    }

    return deepest;
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

    setInsertionInto(deepestOwnerBlock(matchedChunks, stepBlock));
    _builder.create<nl::OptionalCollect>(loc, state, matchedTag, matchedChunks);

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

    buildLoopForSource(drain.getResult(), optionalMatch.getOperation());
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

        if (unit || !_innermostLoopBody) {
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
    const mlir::Value limitHandle = _loopLimitHandle.lookup(call.getOperation());

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
        const mlir::Value limitHandle = _loopLimitHandle.lookup(call.getOperation());

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

void DBLowering::lowerExistsSubquery(mlir::db::ExistsSubquery exists) {
    const mlir::Location loc = _builder.getUnknownLoc();

    llvm::SmallVector<mlir::Value, 4> inputChunks;
    for (const mlir::Value column : exists.getInputColumns()) {
        inputChunks.push_back(mapValue(column));
    }

    // The step the accumulator covers is the one binding the columns the EXISTS answers
    // for, so it is emptied once per chunk of them - and the body's nest and the read of
    // the flags both go there, the read after the nest.
    mlir::Block* const stepBlock = deepestOwnerBlock(inputChunks, _rootBlock);

    // A body run one row at a time makes the accumulator's step one input row: the loop
    // over the rows is opened first and the body roots in it. A body carrying the scope,
    // and one over the single empty row, run in the step block as they otherwise would.
    const bool perRow = runsPerRow(exists) && !inputChunks.empty();

    mlir::Block* bodyRoot = stepBlock;
    llvm::SmallVector<mlir::Value, 4> stepChunks(inputChunks.begin(), inputChunks.end());

    if (perRow) {
        const mlir::Value limitHandle = _loopLimitHandle.lookup(exists.getOperation());

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
    nl::ExistsBuffer buffer = _builder.create<nl::ExistsBuffer>(loc, stepChunks);
    const mlir::Value state = buffer.getState();

    // The body reads this step's rows through its block arguments, and the row tag through
    // the trailing one; a body run per row, or with nothing to join onto, takes no tag.
    mlir::Block& bodyBlock = exists.getBody().front();
    for (size_t inputIndex = 0; inputIndex < stepChunks.size(); inputIndex++) {
        _valueMap[bodyBlock.getArgument(static_cast<unsigned>(inputIndex))] = stepChunks[inputIndex];
    }

    if (bodyBlock.getNumArguments() > inputChunks.size()) {
        _valueMap[bodyBlock.getArgument(static_cast<unsigned>(inputChunks.size()))] = buffer.getTag();
    }

    if (perRow) {
        hoistLimitHandles(exists.getBody(), bodyRoot, exists);
    }

    // A dataflow of its own, rooted where the accumulator sits so its loops nest inside
    // this step; the caller's root and innermost loop are restored once it is lowered.
    mlir::Block* const previousRoot = _rootBlock;
    mlir::Block* const previousInnermostLoopBody = _innermostLoopBody;
    const mlir::Value previousInnermostCardinality = _innermostCardinality;
    _rootBlock = bodyRoot;
    _innermostLoopBody = nullptr;
    _innermostCardinality = mlir::Value();

    llvm::SmallVector<mlir::Value, 4> heldChunks;
    mlir::Value heldTag;

    for (mlir::Operation& operation : bodyBlock) {
        mlir::db::ExistsYield yield = mlir::dyn_cast<mlir::db::ExistsYield>(operation);
        if (!yield) {
            lowerOperation(operation);
            continue;
        }

        for (const mlir::Value column : yield.getColumns()) {
            heldChunks.push_back(mapValue(column));
        }

        if (yield.getTag()) {
            heldTag = mapValue(yield.getTag());
        }
    }

    // The mark goes where the body left its rows - its innermost loop body - which is the
    // deepest block the tag and the columns it holds are bound in.
    llvm::SmallVector<mlir::Value, 4> markedChunks = heldChunks;
    if (heldTag) {
        markedChunks.push_back(heldTag);
    }

    setInsertionInto(deepestOwnerBlock(markedChunks, bodyRoot));
    _builder.create<nl::ExistsMark>(loc, state, heldTag, heldChunks);

    _rootBlock = previousRoot;
    _innermostLoopBody = previousInnermostLoopBody;
    _innermostCardinality = previousInnermostCardinality;

    mlir::MLIRContext* const context = _builder.getContext();
    const nl::ChunkType boolChunk = nl::ChunkType::get(context, mlir::storage::BoolType::get(context));

    setInsertionInto(bodyRoot);
    nl::ExistsResult result = _builder.create<nl::ExistsResult>(loc, boolChunk, state);
    _valueMap[exists.getResult()] = result.getResult();

    if (!perRow) {
        return;
    }

    // The answer is one flag per row of the loop's own chunks, so what follows the op
    // reads the rows in flight through those: the rest of the query goes on inside the
    // loop, where every column of the step is bound a row at a time.
    for (size_t inputIndex = 0; inputIndex < inputChunks.size(); inputIndex++) {
        _valueMap[exists.getInputColumns()[inputIndex]] = stepChunks[inputIndex];
    }

    _innermostLoopBody = bodyRoot;
    _innermostCardinality = stepChunks.front();
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

mlir::Value DBLowering::getOrCreatePropertyTypeHandle(llvm::StringRef propertyName) {
    const auto existing = _propertyTypes.find(propertyName);
    if (existing != _propertyTypes.end()) {
        return existing->second;
    }

    // The handle reads no chunk, so it sits at the very top of the entry block,
    // above every loop, where it dominates all the fetches that use it.
    setInsertionToEntryBlockStart();

    nl::GetPropertyType handleOp = _builder.create<nl::GetPropertyType>(_builder.getUnknownLoc(),
                                                                        _builder.getStringAttr(propertyName));
    const mlir::Value handle = handleOp.getResult();
    _propertyTypes[propertyName] = handle;

    return handle;
}

mlir::Value DBLowering::getOrCreateEdgeTypeSetHandle(mlir::ArrayAttr edgeTypeNames) {
    // The edge sibling of getOrCreatePropertyTypeHandle: dedup per set of names and
    // hoist the handle to the top of the entry block, above every loop, so a by-type
    // hop nested in a loop reuses one resolved handle rather than re-carrying them.
    const auto existing = _edgeTypeSets.find(edgeTypeNames);
    if (existing != _edgeTypeSets.end()) {
        return existing->second;
    }

    setInsertionToEntryBlockStart();

    nl::GetEdgeTypeSet handleOp = _builder.create<nl::GetEdgeTypeSet>(_builder.getUnknownLoc(),
                                                                     edgeTypeNames);
    const mlir::Value handle = handleOp.getResult();
    _edgeTypeSets[edgeTypeNames] = handle;

    return handle;
}

mlir::Type DBLowering::columnType(mlir::Value column) {
    return mlir::cast<mlir::db::ColumnType>(column.getType()).getType();
}

// `declared` is the db read's own result type: none for a name the graph carries, whose
// type the schema answers for, and the nullable value type the analyzer resolved for a name
// only this query's CREATE introduces, which no schema holds until the commit
mlir::Type DBLowering::propertyValueChunkType(llvm::StringRef propertyName, mlir::Type declared) {
    if (const auto nullableType = mlir::dyn_cast<storage::NullableType>(declared)) {
        return nl::ChunkType::get(_builder.getContext(), nullableType);
    }

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

    // Hoist the skip handle to the top of the root block, above every loop of this
    // dataflow, so it dominates the update and the truncate placed in the producing loop
    // body.
    // Unlike a limit, a skip threads no operand onto the loops (it cannot
    // early-exit - every row past the dropped prefix must still be produced), so it
    // needs no up-front pre-scan: the handle is created here, in program order,
    // once the producing loops already exist.
    _builder.setInsertionPointToStart(_rootBlock);
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

    rowAlignBufferedChunks(chunks);

    const mlir::Location loc = _builder.getUnknownLoc();

    // The accumulator and its sort spec are hoisted to the top of the root block,
    // above every loop of this dataflow, so the buffers exist before the producing
    // loop fills them and the handle dominates the collect and the emit loop. A sort
    // fused with a terminal db.limit carries that count as its top-K bound, so the
    // accumulator keeps only the best k rows; an unfused sort keeps every row.
    _builder.setInsertionPointToStart(_rootBlock);

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
    const mlir::Value state = _builder.create<nl::UnionBuffer>(loc).getState();

    llvm::SmallVector<nl::UnionCollect, 4> collects;

    for (mlir::Region& branch : unionOp.getBranches()) {
        _innermostLoopBody = nullptr;
        _innermostCardinality = mlir::Value();

        mlir::db::Yield branchYield = mlir::cast<mlir::db::Yield>(branch.front().back());

        for (mlir::Operation& operation : branch.front()) {
            llvm::SmallVector<std::pair<mlir::Value, mlir::Value>, 4> replacedMappings;
            convertUnionResultChunks(operation, branchYield.getColumns(), replacedMappings);

            if (&operation != branchYield.getOperation()) {
                lowerOperation(operation);
            } else {
                llvm::SmallVector<mlir::Value, 4> chunks;
                for (const mlir::Value column : branchYield.getColumns()) {
                    chunks.push_back(mapValue(column));
                }

                rowAlignBufferedChunks(chunks);

                // A branch of constants alone lays them out where they are bound, above the
                // root, which the collect must still run once per step of
                setInsertionInto(deepestOwnerBlock(chunks, root));
                collects.push_back(_builder.create<nl::UnionCollect>(loc, state, chunks));
            }

            for (const auto& [column, chunk] : replacedMappings) {
                _valueMap[column] = chunk;
            }
        }
    }

    llvm::SmallVector<mlir::MutableOperandRange, 4> branchColumns;
    for (nl::UnionCollect collect : collects) {
        branchColumns.push_back(collect.getColumnsMutable());
    }

    reconcileBranchResultTypes(branchColumns, mlir::ArrayAttr());

    nl::UnionCollect firstCollect = collects.front();
    const llvm::SmallVector<mlir::Type, 4> chunkTypes(firstCollect.getColumns().getTypes());
    const nl::IteratorType iteratorType = nl::IteratorType::get(_builder.getContext(), chunkTypes);

    setInsertionInto(root);
    nl::UnionDrain drain = _builder.create<nl::UnionDrain>(loc, iteratorType, state);

    buildLoopForSource(drain.getResult(), unionOp.getOperation());
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

// A result column carries one value type for the whole result, so the branches have to
// resolve their columns to the same types. The types are only known here: a property fetch
// is typed none until its name is resolved against the schema, so a union of columns that
// turn out to disagree is a program the db level cannot tell from a valid one.
void DBLowering::reconcileBranchResultTypes(llvm::ArrayRef<mlir::MutableOperandRange> branchColumns,
                                            mlir::ArrayAttr columnNames) {
    const size_t columnCount = branchColumns.front().size();

    for (size_t columnIndex = 0; columnIndex < columnCount; columnIndex++) {
        const mlir::Type columnType = unionResultType(branchColumns, columnIndex, columnNames);

        for (const mlir::MutableOperandRange& columns : branchColumns) {
            mlir::OpOperand& operand = columns[static_cast<unsigned>(columnIndex)];
            const mlir::Value chunk = operand.get();
            if (chunk.getType() == columnType) {
                continue;
            }

            operand.set(typedNullChunk(chunk, columnType));
        }
    }
}

// The one type a result column carries: the type the branches saying what the column holds
// agree on. A branch spelling the value null names no type of its own, so it is laid out as
// that column - and where every branch spells it null, the first branch's untyped null is
// the column, there being nothing else it could hold.
mlir::Type DBLowering::unionResultType(llvm::ArrayRef<mlir::MutableOperandRange> branchColumns,
                                       size_t columnIndex,
                                       mlir::ArrayAttr columnNames) {
    const unsigned operandIndex = static_cast<unsigned>(columnIndex);

    mlir::Type resultType;

    for (const mlir::MutableOperandRange& columns : branchColumns) {
        const mlir::Type branchType = columns[operandIndex].get().getType();
        if (isUntypedNullChunk(branchType)) {
            continue;
        }

        if (!resultType) {
            resultType = branchType;
            continue;
        }

        if (branchType != resultType) {
            throwOnDisagreeingBranchTypes(branchType, resultType, columnNames, columnIndex);
        }
    }

    if (!resultType) {
        return branchColumns.front()[operandIndex].get().getType();
    }

    // A null rides a nullable value column, so a column of anything else - an entity, a
    // path, an embedding - has no row a branch spelling the value null could fill
    const mlir::Type resultElement = mlir::cast<nl::ChunkType>(resultType).getElementType();
    if (mlir::isa<storage::NullableType>(resultElement)) {
        return resultType;
    }

    for (const mlir::MutableOperandRange& columns : branchColumns) {
        const mlir::Type branchType = columns[operandIndex].get().getType();
        if (isUntypedNullChunk(branchType)) {
            throwOnDisagreeingBranchTypes(branchType, resultType, columnNames, columnIndex);
        }
    }

    return resultType;
}

void DBLowering::throwOnDisagreeingBranchTypes(mlir::Type branchType,
                                               mlir::Type resultType,
                                               mlir::ArrayAttr columnNames,
                                               size_t columnIndex) {
    std::string branchName;
    describeColumnType(branchType, branchName);

    std::string resultName;
    describeColumnType(resultType, resultName);

    const llvm::StringRef columnName = columnNames ? mlir::cast<mlir::StringAttr>(columnNames[columnIndex]).getValue()
                                                   : llvm::StringRef();

    throw IRException(fmt::format("A UNION column holds one value type for the whole result: "
                                  "'{}' is {} in this sub-query and {} in another",
                                  std::string_view {columnName.data(), columnName.size()},
                                  branchName,
                                  resultName));
}

// The null literal's chunk read as the column the result carries: one absent value per row,
// in the type the branches saying what the column holds resolved it to.
mlir::Value DBLowering::typedNullChunk(mlir::Value chunk, mlir::Type chunkType) {
    mlir::OpBuilder::InsertionGuard guard(_builder);
    setInsertionForUnaryOp(chunk);

    return _builder.create<nl::ToNullable>(_builder.getUnknownLoc(), chunkType, chunk).getResult();
}

// The shared seen-set is hoisted to the top of the root block, above every branch's
// loops, so it is emptied once per step of that block and dominates each branch's filter -
// the same placement lowerRemoveDuplicates gives a dedup's private set.
void DBLowering::lowerDistinctSet(mlir::db::DistinctSet distinctSet) {
    _builder.setInsertionPointToStart(_rootBlock);

    _valueMap[distinctSet.getSet()] = _builder.create<nl::Distinct>(_builder.getUnknownLoc()).getState();
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
}

void DBLowering::lowerCount(mlir::db::Count count) {
    // The nl chunk the counted column lowered to; the update reads its per-step
    // non-null row count. A constant column counts the rows it stands for, not the
    // one row it is, so it is laid out over the driving relation first.
    const mlir::Value inputChunk = rowAlignedChunk(mapValue(count.getInput()), _innermostCardinality);

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
    // The tally comes from the graph's node counts rather than from a relation, so the op
    // reads no column and is loop-invariant: hoist it the way lowerConstant hoists a
    // constant, where it dominates every loop a later op may emit from.
    setInsertionToEntryBlockStart();

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
    const storage::NullableType resultNullable = storage::NullableType::get(context, resultElement);
    const nl::ChunkType resultChunkType = nl::ChunkType::get(context, resultNullable);

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

        // A list holds a view of each string, whether its column owned it or not, as
        // nl.make_list's element type says too
        if (mlir::isa<storage::OwnedStringType>(listElement)) {
            listElement = storage::StringType::get(context);
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

void DBLowering::lowerShortestPath(mlir::db::ShortestPath shortestPath) {
    const mlir::Value sourceChunk = mapValue(shortestPath.getSource());
    const mlir::Value targetChunk = mapValue(shortestPath.getTarget());
    const llvm::StringRef edgeProperty = shortestPath.getEdgeProperty();

    const mlir::Location loc = _builder.getUnknownLoc();
    mlir::MLIRContext* const context = _builder.getContext();

    // The search accumulator is hoisted above every loop, so its node-set buffers exist
    // before the producing loops fill them and the handle dominates both updates.
    _builder.setInsertionPointToStart(_rootBlock);
    const mlir::Value state = _builder.create<nl::ShortestPathBuffer>(loc).getState();

    // The source and target sets are filled by two updates, each spliced into the loop that
    // binds its node chunk. Disconnected patterns are cross-producted upstream, so in
    // practice both chunks are bound by the same loop and both updates land in it; placing
    // the drain after that loop guarantees each set is complete before the search runs.
    setInsertionInto(ownerBlock(sourceChunk));
    _builder.create<nl::ShortestPathUpdate>(loc, state, sourceChunk, /*target=*/false);

    setInsertionInto(ownerBlock(targetChunk));
    _builder.create<nl::ShortestPathUpdate>(loc, state, targetChunk, /*target=*/true);

    // The emit phase: an nl.shortest_path source runs the weighted search once, reading
    // the edge weight through the hoisted property handle, and yields the one result row.
    // The distance chunk is the weight property's value type (resolved here against the
    // schema, since the db distance column is a none placeholder), and it is plain, not
    // nullable - the one emitted row always carries a distance. The path chunk is a path.
    const mlir::Value handle = getOrCreatePropertyTypeHandle(edgeProperty);

    if (!_view) {
        throw IRException("Lowering a shortest path needs a graph to resolve the weight property '" + edgeProperty.str() + "'");
    }

    const std::optional<PropertyType> weightType = _view->metadata().propTypes().get(edgeProperty);
    if (!weightType) {
        throw IRException("Unknown property '" + edgeProperty.str() + "'");
    }

    const mlir::Type distanceElement = valueTypeToElementType(_builder, weightType->_valueType);

    llvm::SmallVector<mlir::Type, 2> chunkTypes;
    chunkTypes.push_back(nl::ChunkType::get(context, distanceElement));
    chunkTypes.push_back(nl::ChunkType::get(context, storage::PathType::get(context)));

    const nl::IteratorType iteratorType = nl::IteratorType::get(context, chunkTypes);

    setInsertionInto(_rootBlock);
    nl::ShortestPath searchOp = _builder.create<nl::ShortestPath>(loc, iteratorType, state, handle);
    buildLoopForSource(searchOp.getResult(), shortestPath.getOperation());
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
        const NamedProcedureType& returnValue = procedure->returnValues()[returnIndex];

        chunkTypes.push_back(procedureChunkType(_builder, returnValue));
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

    // The call handle is hoisted to the top of the root block, above every loop of
    // this dataflow, so the procedure is prepared once per step of that block - its
    // data allocated and its result columns bound - before the loops that drive it,
    // and the handle dominates every op that names it.
    _builder.setInsertionPointToStart(_rootBlock);
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
    const mlir::Value anchorChunk = deepestBoundChunk(operandChunks);

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
                                     bool rowsDroppedBeforeTheCut,
                                     mlir::Operation* holder) {
    // A visit with rows dropped claims a subset of what one without claims, and reaches
    // the same loops, so only a visit without them can add to one made with them
    const auto visitIt = _producerWalkVisits.find(column);
    const bool wasVisited = visitIt != _producerWalkVisits.end();
    const bool isCovered = wasVisited && (rowsDroppedBeforeTheCut || !visitIt->second._rowsDroppedBeforeTheCut);
    if (isCovered) {
        return visitIt->second._reachedALoop;
    }

    const bool reachedALoop = walkProducerLoops(column, handle, rowsDroppedBeforeTheCut, holder);
    _producerWalkVisits[column] = ProducerWalkVisit {reachedALoop, rowsDroppedBeforeTheCut};

    return reachedALoop;
}

bool DBLowering::walkProducerLoops(mlir::Value column,
                                   mlir::Value handle,
                                   bool rowsDroppedBeforeTheCut,
                                   mlir::Operation* holder) {
    mlir::Operation* const definingOp = column.getDefiningOp();
    if (!definingOp) {
        // A subquery body reads the rows in flight through its block arguments, so the
        // walk carries on from the input column each one stands for. A cross-product
        // factor's loop variable is a block argument too, with no producer to reach from
        // here: its loop is reached through the factor's yield in the branch below.
        const mlir::BlockArgument argument = mlir::cast<mlir::BlockArgument>(column);
        mlir::Operation* const bodyHolder = argument.getOwner()->getParentOp();
        const mlir::OperandRange inputs = subqueryInputColumns(bodyHolder);

        if (argument.getArgNumber() >= inputs.size()) {
            return false;
        }

        // The handle this body holds is created inside the loop over its input rows, so a
        // loop outside the body cannot carry it: the walk stops at the boundary.
        if (bodyHolder == holder) {
            return false;
        }

        return assignProducerLoops(inputs[argument.getArgNumber()],
                                   handle,
                                   rowsDroppedBeforeTheCut,
                                   holder);
    }

    const bool opensLoop = opensSourceLoop(definingOp);
    const bool isCrossProduct = mlir::isa<mlir::db::CrossProduct>(definingOp);
    const bool isHashJoin = mlir::isa<mlir::db::HashJoin>(definingOp);
    const bool isSubquery = mlir::isa<mlir::db::CallSubquery>(definingOp);

    const bool emitsThroughLoop = isSubquery
                                  || mlir::isa<mlir::db::Sort,
                                               mlir::db::GroupAggregate,
                                               mlir::db::OptionalMatch>(definingOp);

    // A sort or a grouped aggregate accumulates the whole relation before emitting any of
    // it, so the loops feeding it have to see every row: the walk stops and the limit
    // budgets the emit loop alone. An optional match accumulates one step of the rows it
    // joins onto rather than the relation, and emits them in input order, so once the
    // budget is spent no later step can contribute a row - the walk carries on and bounds
    // the loops feeding it too.
    const bool accumulatesTheRelation = mlir::isa<mlir::db::Sort, mlir::db::GroupAggregate>(definingOp);
    const bool breaksPipeline = accumulatesTheRelation || reducesToOneRow(definingOp);

    bool reachedALoop = opensLoop || isCrossProduct || isHashJoin || emitsThroughLoop;

    // A loop's budget only stops it from taking another step, so bounding one never trims
    // the step it is in. A cross product's or a hash join's budget cuts the rows it pairs,
    // which stands only while every row it makes reaches the output: an op below the cut
    // that drops rows - a filter, a skip, a dedup - would leave it discarding rows that
    // would have survived, and the cut short of its count. So either keeps the handle only
    // on a path that drops nothing; its factor loops take it either way.
    // Declining the handle is not the same as reaching no loop: a cross product is still a
    // producer the walk found, so reachedALoop stands and the cut keeps its nest.
    const bool boundsCrossProduct = isCrossProduct && !rowsDroppedBeforeTheCut;
    const bool boundsHashJoin = isHashJoin && !rowsDroppedBeforeTheCut;
    const bool takesTheHandle = opensLoop || boundsCrossProduct || boundsHashJoin || emitsThroughLoop;

    // The first limit, in program order, to claim a producer wins, so a loop
    // shared by two limits' nests carries the outer one and never two handles.
    if (takesTheHandle && !_loopLimitHandle.count(definingOp)) {
        _loopLimitHandle[definingOp] = handle;
    }

    if (breaksPipeline) {
        return reachedALoop;
    }

    const bool rowsDropped = rowsDroppedBeforeTheCut || dropsRows(definingOp);

    if (isHashJoin) {
        // A hash join's built side has to be read whole - a build loop stopped short of
        // its rows would leave the probe unable to match them - so the budget reaches only
        // the probed factor's loops. That is the left factor, the one the probe walks.
        mlir::db::HashJoin join = mlir::cast<mlir::db::HashJoin>(definingOp);
        mlir::Operation* const probeYield = join.getLeftFactor().front().getTerminator();
        for (const mlir::Value yielded : probeYield->getOperands()) {
            reachedALoop |= assignProducerLoops(yielded, handle, rowsDropped, holder);
        }
    } else if (isCrossProduct) {
        // A cross product takes no column operands - its factors are regions - so
        // recurse through each factor's db.yield operands to reach the factor
        // scans/edge loops that produce the crossed columns.
        mlir::db::CrossProduct cross = mlir::cast<mlir::db::CrossProduct>(definingOp);
        mlir::Region* const factors[] = {&cross.getLeftFactor(), &cross.getRightFactor()};
        for (mlir::Region* const factor : factors) {
            mlir::Operation* const yield = factor->front().getTerminator();
            for (const mlir::Value yielded : yield->getOperands()) {
                reachedALoop |= assignProducerLoops(yielded, handle, rowsDropped, holder);
            }
        }
    } else if (isSubquery) {
        // A subquery's results come out of its body, so the walk goes through the body's
        // yield to the loops inside it, and from them through the block arguments back to
        // the inputs' own loops. A body that carries nothing hands its inputs back through
        // the op itself, so the walk reaches them from here.
        mlir::db::CallSubquery call = mlir::cast<mlir::db::CallSubquery>(definingOp);
        mlir::Operation* const yield = call.getBody().front().getTerminator();
        for (const mlir::Value yielded : yield->getOperands()) {
            reachedALoop |= assignProducerLoops(yielded, handle, rowsDropped, holder);
        }

        if (!call.getCarriesScope()) {
            for (const mlir::Value input : call.getInputColumns()) {
                reachedALoop |= assignProducerLoops(input, handle, rowsDropped, holder);
            }
        }
    } else {
        // A non-loop producer (a property fetch) is traversed but not assigned -
        // it opens no loop - so its input chunk's loop is still reached.
        for (const mlir::Value operand : definingOp->getOperands()) {
            reachedALoop |= assignProducerLoops(operand, handle, rowsDropped, holder);
        }
    }

    return reachedALoop;
}

void DBLowering::assignCardinalityDriverLoop(mlir::db::Limit limit, mlir::Value handle, mlir::Operation* holder) {
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

    assignProducerLoops(driver->getResult(0), handle, rowsDropped, holder);
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

    llvm::SmallVector<mlir::Value, 8> operandChunks(boundNodes);
    operandChunks.append(boundPending.begin(), boundPending.end());
    operandChunks.append(nodePropValues.begin(), nodePropValues.end());
    operandChunks.append(edgePropValues.begin(), edgePropValues.end());
    operandChunks.append(carriedColumns.begin(), carriedColumns.end());

    // Inserted into the deepest block, where all the operands are defined. A merge over
    // literals alone reads no chunk, and so opens where the program does.
    mlir::Block* targetBlock = _entryBlock;
    if (const mlir::Value insertionReference = deepestBoundChunk(operandChunks)) {
        targetBlock = ownerBlock(insertionReference);
    }

    setInsertionInto(targetBlock);

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
        mapOptionalMask(setNodeProperty.getRows()),
        setNodeProperty.getAllPending());
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
        mapOptionalMask(setEdgeProperty.getRows()),
        setEdgeProperty.getAllPending());
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
    setInsertionAfterHoistedConstants();

    nl::Constant nlConstant = _builder.create<nl::Constant>(_builder.getUnknownLoc(), constant.getValue());
    _valueMap[constant.getResult()] = nlConstant.getResult();
    _lastHoistedConstant = nlConstant.getOperation();
}

void DBLowering::lowerCurrentDateTime(mlir::db::CurrentDateTime currentDateTime) {
    setInsertionAfterHoistedConstants();

    const nl::ChunkType resultType
        = nl::ChunkType::get(_builder.getContext(), storage::DateTimeType::get(_builder.getContext()));

    nl::CurrentDateTime lowered
        = _builder.create<nl::CurrentDateTime>(_builder.getUnknownLoc(), resultType);

    _valueMap[currentDateTime.getResult()] = lowered.getResult();
    _lastHoistedConstant = lowered.getOperation();
}

void DBLowering::setInsertionAfterHoistedConstants() {
    if (_lastHoistedConstant) {
        _builder.setInsertionPointAfter(_lastHoistedConstant);
    } else {
        _builder.setInsertionPointToStart(_entryBlock);
    }
}

void DBLowering::setInsertionToEntryBlockStart() {
    _builder.setInsertionPointToStart(_entryBlock);
    _lastHoistedConstant = nullptr;
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

    // A tagged cell holds its null in its own tag, so a null test on one always answers:
    // the mask carries no null of its own, unlike the one a nullable value column gives.
    const bool testsATaggedCellForNull =
        (isTaggedCellChunk(lhsType) && isUntypedNullChunk(rhsType))
        || (isTaggedCellChunk(rhsType) && isUntypedNullChunk(lhsType));

    const bool operandNullable = !testsATaggedCellForNull
                              && (isNullableChunk(lhsType) || isNullableChunk(rhsType));

    switch (kind) {
        case BinaryResultKind::Boolean: {
            const mlir::Type boolElement = _builder.getI1Type();

            // List comparison always nullable; either having a null element => null
            const bool comparesTwoLists = isListChunk(lhsType) && isListChunk(rhsType);
            const bool alwaysNullable = operandNullable || comparesTwoLists;

            return alwaysNullable ? storage::NullableType::get(ctx, boolElement)
                                  : boolElement;
        }
        break;

        case BinaryResultKind::Numeric: {
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
            const bool concatenatesLists = isListChunk(lhsType) && isListChunk(rhsType);

            if (!concatenatesLists) {
                if (isListChunk(lhsType) || isListChunk(rhsType)) {
                    throw IRException("db.concat joins two lists or two strings, not one of each");
                }

                // A type-erased cell holds text only where its tag says so, so a row
                // holding a null or a nested list concatenates to null
                const bool readsACell = holdsTaggedCells(lhsType) || holdsTaggedCells(rhsType);

                const mlir::Type stringElement = storage::StringType::get(ctx);
                return operandNullable || readsACell ? storage::NullableType::get(ctx, stringElement)
                                                     : stringElement;
            }

            // list concat
            const mlir::Type lhsInnerType = listInternalType(lhsType);
            const mlir::Type rhsInnerType = listInternalType(rhsType);

            const bool sharedType = lhsInnerType == rhsInnerType;
            const mlir::Type resultInnerType =
                sharedType ? lhsInnerType : storage::ListElementType::get(ctx);
            const mlir::Type concatenated = storage::ListType::get(ctx, resultInnerType);

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
                                           : storage::ListElementType::get(ctx);

            return storage::NullableType::get(ctx, indexed);
        }
        break;

        case BinaryResultKind::Membership: {
            // A tagged cell holds whatever its row put there, a list among it, so the test
            // reads the list out of the tag rather than out of the column's own type
            if (!isListChunk(rhsType) && !holdsTaggedCells(rhsType)) {
                throw IRException("db.in requires a list as its right operand");
            }

            return storage::NullableType::get(ctx, _builder.getI1Type());
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

    const bool comparesTwoEntities = isEntityChunk(lhsChunkType) && isEntityChunk(rhsChunkType);

    const bool readsScalarOperands = kind != BinaryResultKind::Index
                                  && kind != BinaryResultKind::Membership;

    // A tagged cell holds its null in its own tag, and the comparison reads that tag, so
    // such a column is compared as it stands.
    const bool lhsReadsItsOwnNull = isNullableChunk(lhsChunkType) || isTaggedCellChunk(lhsChunkType);
    const bool rhsReadsItsOwnNull = isNullableChunk(rhsChunkType) || isTaggedCellChunk(rhsChunkType);

    const bool nullAgainstRhs = readsScalarOperands
                             && isUntypedNullChunk(rhsChunkType)
                             && !lhsReadsItsOwnNull;

    const bool nullAgainstLhs = readsScalarOperands
                             && isUntypedNullChunk(lhsChunkType)
                             && !rhsReadsItsOwnNull;

    // x IS NULL over a plain scalar column meets kernels reading a nullable value column
    if (nullAgainstRhs) {
        lhsChunk = nullableValueChunk(lhsChunk);
    } else if (nullAgainstLhs) {
        rhsChunk = nullableValueChunk(rhsChunk);
    } else if (comparesTwoEntities) {
        // Two entities are compared as the IDs they are, and an ID column carries its null
        // in the ID: read raw, the invalid ID two missed matches hold compares equal to
        // itself. Read as nullable ui64, a null compares as a null - which a WHERE drops.
        lhsChunk = nullableValueChunk(lhsChunk);
        rhsChunk = nullableValueChunk(rhsChunk);
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

        if (!unified || unified == element) {
            unified = element;
            continue;
        }

        const bool bothNumeric = isNumericElement(unified) && isNumericElement(element);
        if (!bothNumeric) {
            throw IRException("db.case requires branches of one type, or of numeric types "
                              "that promote against each other");
        }

        unified = promoteNumeric(_builder, unified, element);
    }

    // Every branch is the null literal, so no branch says what the column holds. The rows
    // are all absent whichever type carries them, so they ride the integer column an
    // untyped null is laid out over anywhere else.
    if (!unified) {
        return _builder.getIntegerType(64);
    }

    const auto integerType = mlir::dyn_cast<mlir::IntegerType>(unified);
    const bool isBool = integerType && integerType.getWidth() == 1;
    const bool isString = mlir::isa<storage::StringType>(unified);

    // A node or an edge is selected as the entity it is rather than as its ID's integer,
    // so the selection answers a column the rest of the query still reads as an entity.
    // Mixing one with a scalar, or a node with an edge, was already turned away above:
    // neither pair promotes.
    const bool isEntity = mlir::isa<storage::NodeIDType, storage::EdgeIDType>(unified);

    if (!isNumericElement(unified) && !isBool && !isString && !isEntity) {
        throw IRException("db.case requires scalar or entity branches: a list or an "
                          "embedding is not a value a branch can select");
    }

    return unified;
}

mlir::Value DBLowering::caseBranchChunk(mlir::Value chunk, mlir::Type resultElement) {
    const mlir::Type chunkType = chunk.getType();

    // A null branch is left as the untyped null it is: the selection writes an absent
    // value for the rows that take it, whatever the column holds elsewhere
    if (isUntypedNullChunk(chunkType)) {
        return chunk;
    } else if (chunkValueElement(_builder, chunkType) == resultElement) {
        return chunk;
    }

    mlir::MLIRContext* const context = _builder.getContext();
    const nl::ChunkType convertedType
        = nl::ChunkType::get(context, storage::NullableType::get(context, resultElement));

    mlir::OpBuilder::InsertionGuard guard(_builder);
    setInsertionForUnaryOp(chunk);

    if (mlir::isa<mlir::Float64Type>(resultElement)) {
        return _builder.create<nl::ToFloat>(_builder.getUnknownLoc(), convertedType, chunk).getResult();
    }

    return _builder.create<nl::ToInteger>(_builder.getUnknownLoc(), convertedType, chunk).getResult();
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
        value = caseBranchChunk(value, resultElement);
    }

    if (defaultValue) {
        defaultValue = caseBranchChunk(defaultValue, resultElement);
    }

    gatherOperands(operands);

    // A row matching no branch of a defaultless CASE is absent, and so is one taking a
    // null branch, so the selection always lands in a column that can hold an absent row.
    // An entity column already can - it carries its null in the ID an OPTIONAL MATCH left
    // invalid - so entities land in a plain ID chunk and every scalar in a nullable one.
    mlir::MLIRContext* const context = _builder.getContext();
    const bool selectsEntities = mlir::isa<storage::NodeIDType, storage::EdgeIDType>(resultElement);
    const mlir::Type resultChunkElement = selectsEntities
        ? resultElement
        : storage::NullableType::get(context, resultElement);
    const nl::ChunkType resultType = nl::ChunkType::get(context, resultChunkElement);

    setInsertionForNaryOp(operands);

    nl::Case nlCase = _builder.create<nl::Case>(_builder.getUnknownLoc(),
                                                resultType,
                                                conditions,
                                                values,
                                                defaultValue);
    _valueMap[caseOp.getResult()] = nlCase.getResult();
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

    // NOT of an unknown truth value is unknown, and an untyped null names no boolean
    // column to carry it: the negation is the same untyped null its operand is
    if (isUntypedNullChunk(operandType)) {
        resultElement = mlir::cast<nl::ChunkType>(operandType).getElementType();
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
            setInsertionToEntryBlockStart();
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
            setInsertionToEntryBlockStart();
        }
    }
}

void DBLowering::lowerUnaryFunction(mlir::Operation* op) {
    const UnaryFunctionLowering* spec = lookupUnaryFunctionLowering(*op);
    bioassert(spec, "lowerUnaryFunction called on a non-function op");

    const mlir::Value inputChunk = mapValue(op->getOperand(0));

    const mlir::Type inputElement = mlir::cast<nl::ChunkType>(inputChunk.getType()).getElementType();
    const auto inputNullableElement = mlir::dyn_cast<storage::NullableType>(inputElement);
    const mlir::Type inputValueElement = inputNullableElement ? inputNullableElement.getValueType() : inputElement;

    const mlir::Type baseElement = spec->element(_builder, inputValueElement);

    // A tagged cell is always there, but the value it holds may be the null its tag says
    // it is, so what a function reads out of one can be absent as a nullable column's is
    const bool inputCanBeNull = inputNullableElement != nullptr
                             || isTaggedCellChunk(inputChunk.getType());

    const bool alwaysNull = spec->nullability == ResultNullability::AlwaysNullable;
    const bool specNull = spec->nullability == ResultNullability::FollowsInput && inputCanBeNull;
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

    const mlir::Type lhsElement = mlir::cast<nl::ChunkType>(lhsChunk.getType()).getElementType();
    const mlir::Type baseElement = spec->element(_builder, lhsElement);

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

mlir::Value DBLowering::deepestBoundChunk(llvm::ArrayRef<mlir::Value> chunks) {
    mlir::Value deepest;
    for (const mlir::Value chunk : chunks) {
        if (!deepest) {
            deepest = chunk;
            continue;
        }

        mlir::Block* const block = deeperBlock(deepest, chunk);
        if (ownerBlock(chunk) == block) {
            deepest = chunk;
        }
    }

    return deepest;
}

mlir::Block* DBLowering::deeperBlock(mlir::Value first, mlir::Value second) {
    return deeperOfBlocks(ownerBlock(first), ownerBlock(second));
}

mlir::Block* DBLowering::deeperOfBlocks(mlir::Block* first, mlir::Block* second) {
    if (first == second) {
        return first;
    }

    // A chunk bound in a block that encloses the other is read once per step of it - a
    // hoisted constant, a metadata tally, the one row a per-row body walks - so the op
    // reading both belongs in the deeper block.
    if (enclosesBlock(first, second)) {
        return second;
    } else if (enclosesBlock(second, first)) {
        return first;
    }

    throw IRException("db operands read together must be bound in the same loop");
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

    return _builder.create<nl::Output>(_builder.getUnknownLoc(),
                                       columns,
                                       mlir::Value(),
                                       mlir::Value(),
                                       cardinality,
                                       output.getColumnNamesAttr());
}

mlir::Operation* DBLowering::topLevelNestOf(mlir::Block* block) const {
    if (block == _entryBlock) {
        return nullptr;
    }

    mlir::Operation* enclosing = block->getParentOp();
    while (enclosing->getBlock() != _entryBlock) {
        enclosing = enclosing->getBlock()->getParentOp();
    }

    return enclosing;
}

mlir::Value DBLowering::buildEndNodeSet(mlir::Value endNodeColumn, mlir::Value inputChunk) {
    const mlir::Value endChunk = mapValue(endNodeColumn);
    mlir::Block* const collectBlock = ownerBlock(endChunk);

    mlir::Operation* const collectNest = topLevelNestOf(collectBlock);
    mlir::Operation* const walkNest = topLevelNestOf(ownerBlock(inputChunk));
    const bool fillsBeforeTheWalk = collectNest && walkNest && collectNest->isBeforeInBlock(walkNest);
    if (!fillsBeforeTheWalk) {
        throw IRException("The end nodes of an exploration must be bound by a loop of their own, closing before the loop the walk runs in");
    }

    const mlir::Location loc = _builder.getUnknownLoc();

    _builder.setInsertionPointToStart(_entryBlock);
    const mlir::Value state = _builder.create<nl::NodeSetBuffer>(loc).getState();

    setInsertionInto(collectBlock);
    _builder.create<nl::NodeSetCollect>(loc, state, endChunk);

    return state;
}

void DBLowering::lowerExplorePaths(mlir::db::ExplorePaths explorePaths) {
    const mlir::Value inputChunk = mapValue(explorePaths.getInputNodes());

    llvm::SmallVector<mlir::Value, 4> carriedChunks;
    for (const mlir::Value carriedColumn : explorePaths.getColumnsToFilter()) {
        carriedChunks.push_back(mapValue(carriedColumn));
    }

    mlir::Value endNodeSet;
    if (const mlir::Value endNodeColumn = explorePaths.getEndNodes()) {
        endNodeSet = buildEndNodeSet(endNodeColumn, inputChunk);
    }

    setInsertionInto(ownerBlock(inputChunk));

    nl::ExplorePaths exploration = _builder.create<nl::ExplorePaths>(_builder.getUnknownLoc(),
                                                                     inputChunk,
                                                                     carriedChunks,
                                                                     endNodeSet,
                                                                     explorePaths.getDirection(),
                                                                     explorePaths.getMinHops(),
                                                                     explorePaths.getMaxHopsAttr(),
                                                                     explorePaths.getEdgeTypeAttr(),
                                                                     explorePaths.getEndLabelsAttr(),
                                                                     explorePaths.getEndColumnAttr(),
                                                                     explorePaths.getEndsOnSeed(),
                                                                     explorePaths.getDistinct());

    mlir::Region& dbHop = explorePaths.getHop();
    if (!dbHop.empty()) {
        const mlir::OpBuilder::InsertionGuard guard(_builder);
        lowerHopRegion(dbHop.front(), exploration.getHop());
    }

    buildLoopForSource(exploration.getResult(), explorePaths.getOperation());
}

void DBLowering::lowerHopRegion(mlir::Block& dbHop, mlir::Region& nlHop) {
    mlir::MLIRContext* context = _builder.getContext();
    const mlir::Location loc = _builder.getUnknownLoc();

    const mlir::Type nodeChunk = nl::ChunkType::get(context, storage::NodeIDType::get(context));
    const mlir::Type edgeChunk = nl::ChunkType::get(context, storage::EdgeIDType::get(context));
    const llvm::SmallVector<mlir::Type, 3> argumentTypes {nodeChunk, edgeChunk, nodeChunk};
    const llvm::SmallVector<mlir::Location, 3> argumentLocations {loc, loc, loc};

    mlir::Block* nlBlock = _builder.createBlock(&nlHop, nlHop.end(), argumentTypes, argumentLocations);
    for (unsigned argumentIndex = 0; argumentIndex < argumentTypes.size(); argumentIndex++) {
        _valueMap[dbHop.getArgument(argumentIndex)] = nlBlock->getArgument(argumentIndex);
    }

    // The ops lowered into the block insert ahead of its terminator, so the block gets one
    // before the mask that the real terminator yields exists
    nl::Yield placeholder = _builder.create<nl::Yield>(loc);

    mlir::Value mask;
    for (mlir::Operation& operation : dbHop) {
        if (mlir::db::Yield yield = mlir::dyn_cast<mlir::db::Yield>(operation)) {
            mask = mapValue(yield.getColumns().front());
        } else {
            lowerOperation(operation);
        }
    }

    _builder.setInsertionPoint(placeholder);
    const mlir::Value maskChunk = rowAlignedChunk(mask, nlBlock->getArgument(1));
    _builder.create<nl::Yield>(loc, mlir::ValueRange {maskChunk});
    placeholder.erase();
}

void DBLowering::lowerExpandPath(mlir::db::ExpandPath expandPath) {
    const mlir::Value pathsChunk = mapValue(expandPath.getPaths());
    const mlir::Value seedsChunk = expandPath.getSrcids() ? mapValue(expandPath.getSrcids()) : mlir::Value();

    setInsertionInto(ownerBlock(pathsChunk));

    const auto resultColumn = mlir::cast<mlir::db::ColumnType>(expandPath.getResult().getType());
    const nl::ChunkType resultType = nl::ChunkType::get(_builder.getContext(), resultColumn.getType());

    nl::ExpandPath expansion = _builder.create<nl::ExpandPath>(_builder.getUnknownLoc(),
                                                               resultType,
                                                               pathsChunk,
                                                               seedsChunk,
                                                               expandPath.getKind());
    _valueMap[expandPath.getResult()] = expansion.getResult();
}

void DBLowering::lowerPathLength(mlir::db::PathLength pathLength) {
    const mlir::Value pathsChunk = mapValue(pathLength.getPaths());

    setInsertionInto(ownerBlock(pathsChunk));

    mlir::MLIRContext* context = _builder.getContext();
    const nl::ChunkType resultType = nl::ChunkType::get(context, mlir::IntegerType::get(context, 64, mlir::IntegerType::Unsigned));

    nl::PathLength length = _builder.create<nl::PathLength>(_builder.getUnknownLoc(), resultType, pathsChunk);
    _valueMap[pathLength.getResult()] = length.getResult();
}

void DBLowering::lowerMakePath(mlir::db::MakePath makePath) {
    llvm::SmallVector<mlir::Value, 4> entityChunks;
    for (const mlir::Value entityColumn : makePath.getEntities()) {
        entityChunks.push_back(mapValue(entityColumn));
    }

    setInsertionForNaryOp(entityChunks);

    const auto resultColumn = mlir::cast<mlir::db::ColumnType>(makePath.getResult().getType());
    const nl::ChunkType resultType = nl::ChunkType::get(_builder.getContext(), resultColumn.getType());

    nl::MakePath path = _builder.create<nl::MakePath>(_builder.getUnknownLoc(), resultType, entityChunks);
    _valueMap[makePath.getResult()] = path.getResult();
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
    if (updateBlock == _rootBlock) {
        setInsertionInto(_rootBlock);
        return;
    }

    mlir::Operation* const enclosing = _rootBlock->findAncestorOpInBlock(*updateBlock->getParentOp());
    if (!enclosing) {
        setInsertionInto(_rootBlock);
        return;
    }

    _builder.setInsertionPointAfter(enclosing);
}

void DBLowering::setInsertionForBinaryOp(mlir::Value lhs, mlir::Value rhs) {
    const std::array<mlir::Value, 2> operands {lhs, rhs};
    setInsertionForNaryOp(operands);
}

void DBLowering::setInsertionForNaryOp(llvm::ArrayRef<mlir::Value> operands) {
    // If every operand is a top-level constant (in `_entryBlock`), then place the op
    // after the last defined constant. Otherwise place the op in the more deeply nested
    // block.
    mlir::Value deepest = operands.front();
    for (const mlir::Value operand : operands.drop_front()) {
        if (deeperBlock(deepest, operand) != ownerBlock(deepest)) {
            deepest = operand;
        }
    }

    mlir::Block* const insertBlock = ownerBlock(deepest);

    if (insertBlock != _entryBlock) {
        setInsertionInto(insertBlock);
        return;
    }

    llvm::SmallPtrSet<const mlir::Operation*, 4> defs;
    for (const mlir::Value operand : operands) {
        if (const mlir::Operation* const definingOp = operand.getDefiningOp()) {
            defs.insert(definingOp);
        }
    }

    // Walk the entry block to find which of the defining ops appears last — the new op
    // must go after that one to stay before any nl.for that follows. Each op appears once
    // in the block, so stop as soon as every def is seen.
    mlir::Operation* lastDef = nullptr;
    size_t defsFound = 0;
    for (mlir::Operation& op : *_entryBlock) {
        if (defs.contains(&op)) {
            lastDef = &op;
            defsFound++;

            if (defsFound == defs.size()) {
                break;
            }
        }
    }

    if (lastDef) {
        _builder.setInsertionPointAfter(lastDef);
    } else {
        setInsertionToEntryBlockStart();
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

void DBLowering::rowAlignFactorChunks(llvm::SmallVectorImpl<mlir::Value>& chunks) {
    mlir::Value cardinality;
    for (const mlir::Value chunk : chunks) {
        if (!yieldsConstantColumn(chunk)) {
            cardinality = chunk;
            break;
        }
    }

    for (mlir::Value& chunk : chunks) {
        chunk = rowAlignedChunk(chunk, cardinality);
    }
}

// A buffer holds rows, and a constant holds one value standing for every row of the step
// instead, so what an accumulator would append is a column with no rows in it. The
// constants are laid out first: over the relation driving the step, or - where none drives
// it - over the single row the projection is.
void DBLowering::rowAlignBufferedChunks(llvm::SmallVectorImpl<mlir::Value>& chunks) {
    mlir::Value cardinality = _innermostCardinality;
    for (const mlir::Value chunk : chunks) {
        if (!yieldsConstantColumn(chunk)) {
            cardinality = chunk;
            break;
        }
    }

    for (mlir::Value& chunk : chunks) {
        chunk = rowAlignedChunk(chunk, cardinality);
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

    // A node or an edge reads as its ID's integer, absent where an OPTIONAL MATCH left the
    // ID invalid: that is what makes n IS NULL the ordinary null test over a value column
    if (mlir::isa<storage::NodeIDType, storage::EdgeIDType>(element)) {
        valueElement = _builder.getIntegerType(64, /*isSigned=*/false);
    }

    const bool isString = mlir::isa<storage::StringType>(valueElement);
    const bool isDouble = mlir::isa<mlir::Float64Type>(valueElement);
    const bool isInteger = mlir::isa<mlir::IntegerType>(valueElement);
    if (!isString && !isDouble && !isInteger) {
        throw IRException("Only a scalar value column can be read as a nullable value column");
    }

    return toNullableChunk(chunk, valueElement);
}

// The nullable column a list rides where a row of it can be absent - the drain padding the
// input rows a subquery body yielded nothing for. Kept apart from the value sibling so the
// reductions and comparisons that read only a scalar still turn a list away.
mlir::Value DBLowering::nullableListChunk(mlir::Value chunk) {
    const auto chunkType = mlir::cast<nl::ChunkType>(chunk.getType());
    const mlir::Type element = chunkType.getElementType();
    if (mlir::isa<storage::NullableType>(element)) {
        return chunk;
    }

    return toNullableChunk(chunk, element);
}

mlir::Value DBLowering::toNullableChunk(mlir::Value chunk, mlir::Type valueElement) {
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

// The one column a union result carries, whichever branch filled it. A count is an
// unsigned tally that is never null and a property a nullable signed value, and the two
// are one Cypher INTEGER, so a scalar reaches the column as the nullable signed form. An
// entity, a path, a list and an embedding have one spelling already and are left alone.
mlir::Value DBLowering::unionColumnChunk(mlir::Value chunk) {
    const mlir::Type element = chunkValueElement(_builder, chunk.getType());

    const bool isString = mlir::isa<storage::StringType, storage::OwnedStringType>(element);
    const bool isDouble = mlir::isa<mlir::Float64Type>(element);
    const mlir::IntegerType integerType = mlir::dyn_cast<mlir::IntegerType>(element);

    if (!isString && !isDouble && !integerType) {
        return chunk;
    }

    if (isString) {
        return ownedStringColumnChunk(chunk);
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

// One branch reads a string property, borrowing its characters from the graph, and another
// builds its own - type(e), a CSV field, a procedure's yield. The column carries the owned
// form, since a view into a chunk the next step refills is not what a dedup keys on, a sort
// buffers or a sink reads.
mlir::Value DBLowering::ownedStringColumnChunk(mlir::Value chunk) {
    mlir::MLIRContext* const context = _builder.getContext();
    const storage::OwnedStringType ownedElement = storage::OwnedStringType::get(context);
    const storage::NullableType nullableElement = storage::NullableType::get(context, ownedElement);
    const nl::ChunkType resultType = nl::ChunkType::get(context, nullableElement);

    if (chunk.getType() == resultType) {
        return chunk;
    }

    mlir::OpBuilder::InsertionGuard guard(_builder);
    setInsertionForUnaryOp(chunk);

    return _builder.create<nl::ToOwnedString>(_builder.getUnknownLoc(), resultType, chunk).getResult();
}

// The chunk shape a drain can pad a missed row of. A mask holds a bit and a number a zero,
// which the query cannot tell from a value it was given, so those are read as nullable
// value chunks, and a string as the owned nullable one nl.to_owned_string produces. An ID
// column needs none of it: it spells its null as the invalid ID.
mlir::Value DBLowering::paddedColumnChunk(mlir::Value chunk) {
    const nl::ChunkType chunkType = mlir::dyn_cast<nl::ChunkType>(chunk.getType());
    if (!chunkType) {
        return chunk;
    }

    const mlir::Type element = chunkType.getElementType();

    if (mlir::isa<storage::OwnedStringType>(element)) {
        return ownedStringColumnChunk(chunk);
    } else if (mlir::isa<storage::ListType>(element)) {
        return nullableListChunk(chunk);
    } else if (mlir::isa<storage::BoolType, storage::StringType, mlir::Float64Type, mlir::IntegerType>(element)) {
        return nullableValueChunk(chunk);
    }

    return chunk;
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
    // what every fold, key serialization and reduction reads a value column as. A list or
    // a map is laid out as the plain container chunk an nl.collect drain emits instead: the
    // value being laid out is a literal, so no row of it is absent.
    mlir::MLIRContext* const context = _builder.getContext();
    const bool isContainer = llvm::isa<storage::ListType, storage::MapType>(valueElement);
    const mlir::Type resultElement = isContainer ? valueElement
                                                : storage::NullableType::get(context, valueElement);
    const nl::ChunkType resultType = nl::ChunkType::get(context, resultElement);

    // With no relation driving the projection the value is laid out over the single row
    // that projection is, right where the constant is bound: a layout read from a loop
    // nested under it must not sit below that nest.
    if (cardinality) {
        setInsertionInto(ownerBlock(cardinality));
    } else {
        _builder.setInsertionPointAfter(chunk.getDefiningOp());
    }

    nl::BroadcastConstant broadcast = _builder.create<nl::BroadcastConstant>(_builder.getUnknownLoc(), resultType, chunk, cardinality);

    return broadcast.getResult();
}

bool DBLowering::enclosesBlock(mlir::Block* outer, mlir::Block* inner) {
    if (outer == inner) {
        return true;
    }

    mlir::Operation* const parent = inner->getParentOp();

    return parent && outer->findAncestorOpInBlock(*parent);
}

mlir::Block* DBLowering::accumulatorUpdateBlock(mlir::Block* producingBlock) const {
    if (enclosesBlock(_rootBlock, producingBlock)) {
        return producingBlock;
    }

    return _rootBlock;
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
