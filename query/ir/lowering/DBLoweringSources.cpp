#include "DBLowering.h"

#include <mlir/IR/Location.h>
#include <string>
#include <string_view>

#include "mlir/IR/Block.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/Types.h"

#include "NLOps.h"
#include "DBLoweringChunkTypes.h"

#include "NullLiteralType.h"
#include "Procedure.h"
#include "ProcedureManager.h"
#include "ProcedureTypeVector.h"

#include "IRValueTypes.h"

#include "IRException.h"
#include "llvm/Support/Casting.h"

using namespace db;

namespace nl = mlir::nl;
namespace storage = mlir::storage;

namespace {

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

        case ProcedureType::LIST:
            return storage::ListType::get(context, storage::AnyType::get(context));
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
        return nl::ChunkType::get(context, nullableElementType(elementType));
    }

    return nl::ChunkType::get(context, elementType);
}

}

void DBLowering::lowerUnwindConst(mlir::db::UnwindConst unwindConst) {
    // The literal-list sibling of lowerConstScanNodes: a source reads no column, so
    // its loop sits at the top of the current root block. The literals are the rows to
    // emit, forwarded as-is; translation materializes them into a ListView. Unlike a
    // node-ID scan the chunk element type varies with the list (a homogeneous value
    // type, or !storage.any for a heterogeneous one), so - like nl.collect - the
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
    // A heterogeneous list keeps its type-erased !storage.any chunk, which only
    // pass-through consumers accept.
    const nl::ChunkType chunk = nl::ChunkType::get(context, nullableElementType(elementType));
    const nl::IteratorType iteratorType = nl::IteratorType::get(context, {chunk});

    nl::UnwindConst rows = _builder.create<nl::UnwindConst>(_builder.getUnknownLoc(),
                                                            iteratorType,
                                                            unwindConst.getElementsAttr());
    buildLoopForSource(rows.getResult(), unwindConst.getOperation());
}

void DBLowering::lowerLoadCSV(mlir::db::LoadCSV loadCSV) {
    // The file sibling of lowerUnwindConst: a source reads no row, so its loop sits at
    // the top of the current root block. The path, the field list and the flags are
    // forwarded as-is; the path and the header names are resolved when the loop runs. One
    // string chunk per field, so - like nl.unwind_const - the iterator type is
    // spelled here rather than inferred.
    setInsertionInto(_rootBlock);

    mlir::MLIRContext* const context = _builder.getContext();
    const nl::ChunkType chunk = nl::ChunkType::get(context, storage::StringType::get(context));

    const llvm::SmallVector<mlir::Type> chunks(loadCSV.getResults().size(), chunk);
    const nl::IteratorType iteratorType = nl::IteratorType::get(context, chunks);

    const mlir::Value boundPath = loadCSV.getBoundPath();

    nl::LoadCSV records = _builder.create<nl::LoadCSV>(_builder.getUnknownLoc(),
                                                       iteratorType,
                                                       loadCSV.getPathAttr(),
                                                       boundPath ? mapValue(boundPath) : mlir::Value(),
                                                       loadCSV.getFieldsAttr(),
                                                       loadCSV.getWithHeadersAttr(),
                                                       loadCSV.getSkipOnErrorAttr());
    buildLoopForSource(records.getResult(), loadCSV.getOperation());
}

void DBLowering::lowerVectorSearch(mlir::db::VectorSearch vectorSearch) {
    // The neighbour sibling of lowerUnwindConst: a source reads no row, so its loop
    // sits at the top of the current root block. The index, the neighbour count and the
    // query vector are forwarded as-is; the two chunk types are fixed, so the iterator
    // type is inferred rather than spelled.
    setInsertionInto(_rootBlock);

    const mlir::Value boundIndexName = vectorSearch.getBoundIndexName();
    const mlir::Value boundCount = vectorSearch.getBoundK();
    const mlir::Value boundQueryVector = vectorSearch.getBoundQueryVector();

    nl::VectorSearch neighbours = _builder.create<nl::VectorSearch>(_builder.getUnknownLoc(),
                                                                    vectorSearch.getIndexNameAttr(),
                                                                    searchedValueChunk(boundIndexName),
                                                                    vectorSearch.getKAttr(),
                                                                    searchedValueChunk(boundCount),
                                                                    vectorSearch.getQueryVectorAttr(),
                                                                    searchedValueChunk(boundQueryVector));
    buildLoopForSource(neighbours.getResult(), vectorSearch.getOperation());
}

// A bound value typed none is a null: it reaches the search as a null cell, which the
// search raises on when it starts
mlir::Value DBLowering::searchedValueChunk(mlir::Value bound) {
    if (!bound) {
        return mlir::Value();
    }

    const mlir::Value chunk = mapValue(bound);
    const bool holdsOnlyNull = mlir::isa<mlir::NoneType>(chunkValueElement(_builder, chunk.getType()));
    if (!holdsOnlyNull) {
        return chunk;
    }

    mlir::MLIRContext* const context = _builder.getContext();

    return toAnyChunk(chunk, nl::ChunkType::get(context, storage::AnyType::get(context)));
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

    // Inserted into the deepest block, where every operand is bound - as lowerFilter's is.
    // The unwind opens a loop, so like a scan it sits no higher than the root block: a
    // factor unwinding a list of constants loops inside the factor, not where they were hoisted.
    llvm::SmallVector<mlir::Value, 4> operandChunks {sourceChunk};
    operandChunks.append(carriedChunks.begin(), carriedChunks.end());

    setInsertionInto(deepestOwnerBlock(operandChunks, _rootBlock));

    mlir::MLIRContext* const context = _builder.getContext();
    const mlir::Type sourceElement = mlir::cast<nl::ChunkType>(sourceChunk.getType()).getElementType();

    llvm::SmallVector<mlir::Type, 4> chunkTypes;
    chunkTypes.push_back(nl::ChunkType::get(context, ::db::unwoundElementType(context, sourceElement)));

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

void DBLowering::lowerUnwindRange(mlir::db::UnwindRange unwindRange) {
    llvm::SmallVector<mlir::Value, 4> carriedChunks;
    for (const mlir::Value carriedColumn : unwindRange.getColumnsToFilter()) {
        carriedChunks.push_back(mapValue(carriedColumn));
    }

    const mlir::Value from = unwindRange.getFrom();
    const mlir::Value to = unwindRange.getTo();

    llvm::SmallVector<mlir::Value, 5> bounds {mapValue(unwindRange.getStart()),
                                              mapValue(unwindRange.getEnd()),
                                              mapValue(unwindRange.getStep())};

    if (from) {
        bounds.push_back(mapValue(from));
    }

    if (to) {
        bounds.push_back(mapValue(to));
    }

    llvm::SmallVector<mlir::Value, 8> operandChunks(carriedChunks.begin(), carriedChunks.end());
    operandChunks.append(bounds.begin(), bounds.end());

    const mlir::Value cardinality = cardinalityDriver(operandChunks);

    for (mlir::Value& bound : bounds) {
        bound = numberOperandChunk(rowAlignedChunk(bound, cardinality));
    }

    operandChunks.assign(bounds.begin(), bounds.end());
    operandChunks.append(carriedChunks.begin(), carriedChunks.end());

    setInsertionInto(deepestOwnerBlock(operandChunks, _rootBlock));

    mlir::MLIRContext* const context = _builder.getContext();

    llvm::SmallVector<mlir::Type, 4> chunkTypes;
    chunkTypes.push_back(nl::ChunkType::get(context, storage::NullableType::get(context, _builder.getI64Type())));

    for (const mlir::Value carriedChunk : carriedChunks) {
        chunkTypes.push_back(carriedChunk.getType());
    }

    const nl::IteratorType iteratorType = nl::IteratorType::get(context, chunkTypes);

    const mlir::Value fromChunk = from ? bounds[3] : mlir::Value {};
    const mlir::Value toChunk = to ? bounds.back() : mlir::Value {};

    nl::UnwindRange rows = _builder.create<nl::UnwindRange>(_builder.getUnknownLoc(),
                                                            iteratorType,
                                                            bounds[0],
                                                            bounds[1],
                                                            bounds[2],
                                                            fromChunk,
                                                            toChunk,
                                                            carriedChunks,
                                                            unwindRange.getReversed());
    buildLoopForSource(rows.getResult(), unwindRange.getOperation());
}

void DBLowering::lowerUnwindComputed(mlir::db::UnwindComputed unwind) {
    const mlir::Location loc = _builder.getUnknownLoc();
    mlir::MLIRContext* const context = _builder.getContext();

    llvm::SmallVector<mlir::Value, 4> carriedChunks;
    llvm::SmallVector<mlir::Type, 4> carriedTypes;
    llvm::SmallVector<mlir::Location, 4> carriedLocations;
    for (const mlir::Value carriedColumn : unwind.getColumnsToFilter()) {
        const mlir::Value carriedChunk = mapValue(carriedColumn);
        carriedChunks.push_back(carriedChunk);
        carriedTypes.push_back(carriedChunk.getType());
        carriedLocations.push_back(loc);
    }

    setInsertionInto(deepestOwnerBlock(carriedChunks, _rootBlock));

    // The element type is the source's to decide, and the source is only known once the
    // region is lowered, so the iterator is typed after it
    nl::UnwindComputed rows = _builder.create<nl::UnwindComputed>(loc,
                                                                  nl::IteratorType::get(context, carriedTypes),
                                                                  carriedChunks);

    mlir::Block& dbSource = unwind.getSource().front();
    mlir::Block* const nlSource = &rows.getSource().emplaceBlock();
    nlSource->addArguments(carriedTypes, carriedLocations);

    for (size_t argumentIndex = 0; argumentIndex < carriedChunks.size(); argumentIndex++) {
        const unsigned index = static_cast<unsigned>(argumentIndex);
        _valueMap[dbSource.getArgument(index)] = nlSource->getArgument(index);
    }

    const mlir::Value runCardinality = nlSource->getArgument(0);

    _builder.setInsertionPointToEnd(nlSource);
    mlir::Operation* const placeholderYield = _builder.create<nl::UnwindYield>(loc, runCardinality);

    mlir::Block* const previousInnermostLoopBody = _innermostLoopBody;
    mlir::Block* const previousInnermostRegionBody = _innermostRegionBody;
    const mlir::Value previousInnermostCardinality = _innermostCardinality;
    _innermostLoopBody = nlSource;
    _innermostRegionBody = nlSource;
    _innermostCardinality = runCardinality;

    mlir::Value sourceChunk;
    for (mlir::Operation& operation : dbSource) {
        mlir::db::UnwindYield yield = mlir::dyn_cast<mlir::db::UnwindYield>(operation);
        if (yield) {
            sourceChunk = rowAlignedChunk(mapValue(yield.getSource()), runCardinality);
        } else {
            lowerOperation(operation);
        }
    }

    _innermostLoopBody = previousInnermostLoopBody;
    _innermostRegionBody = previousInnermostRegionBody;
    _innermostCardinality = previousInnermostCardinality;

    _builder.setInsertionPointToEnd(nlSource);
    _builder.create<nl::UnwindYield>(loc, sourceChunk);
    eraseOp(placeholderYield);

    const mlir::Type sourceElement = mlir::cast<nl::ChunkType>(sourceChunk.getType()).getElementType();

    llvm::SmallVector<mlir::Type, 4> chunkTypes;
    chunkTypes.push_back(nl::ChunkType::get(context, ::db::unwoundElementType(context, sourceElement)));
    chunkTypes.append(carriedTypes.begin(), carriedTypes.end());

    rows.getResult().setType(nl::IteratorType::get(context, chunkTypes));

    _builder.setInsertionPointAfter(rows);
    buildLoopForSource(rows.getResult(), unwind.getOperation());
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
