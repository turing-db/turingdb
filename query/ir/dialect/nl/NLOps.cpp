#include "NLOps.h"
#include "NLDialect.h"

#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/SmallVector.h"

using namespace mlir;
using namespace mlir::nl;

namespace storage = mlir::storage;

#define GET_OP_CLASSES
#include "NLOps.cpp.inc"

namespace {

Type getNodeIDChunkType(MLIRContext* context) {
    return ChunkType::get(context, storage::NodeIDType::get(context));
}

Type getEdgeIDChunkType(MLIRContext* context) {
    return ChunkType::get(context, storage::EdgeIDType::get(context));
}

Type getEdgeTypeIDChunkType(MLIRContext* context) {
    return ChunkType::get(context, storage::EdgeTypeIDType::get(context));
}

// Out- and in-edge fetches expose the same row of chunks - source node IDs,
// edge IDs, edge type IDs and target node IDs - followed by one filtered chunk
// per carried column, so a loop body written for one direction works unchanged
// for the other.
Type getEdgeIteratorType(MLIRContext* context, TypeRange carriedChunkTypes) {
    const Type sources = getNodeIDChunkType(context);
    const Type edgeIDs = getEdgeIDChunkType(context);
    const Type edgeTypeIDs = getEdgeTypeIDChunkType(context);
    const Type targets = getNodeIDChunkType(context);

    // The four fixed edge chunks, then the carry set comes back with the same
    // chunk types, mirroring db.get_out_edges' filtered_columns.
    llvm::SmallVector<Type> chunkTypes {sources, edgeIDs, edgeTypeIDs, targets};
    chunkTypes.append(carriedChunkTypes.begin(), carriedChunkTypes.end());

    return IteratorType::get(context, chunkTypes);
}

}

// A node scan always produces one chunk of node IDs per step
LogicalResult ScanNodes::inferReturnTypes(MLIRContext* context,
                                          std::optional<Location> location,
                                          ScanNodes::Adaptor adaptor,
                                          SmallVectorImpl<Type>& inferredReturnTypes) {
    inferredReturnTypes.push_back(IteratorType::get(context, {getNodeIDChunkType(context)}));
    return success();
}

// An out-edges fetch produces one row of edge chunks per step, then one
// filtered chunk per carried column, mirroring db.get_out_edges
LogicalResult GetOutEdges::inferReturnTypes(MLIRContext* context,
                                            std::optional<Location> location,
                                            GetOutEdges::Adaptor adaptor,
                                            SmallVectorImpl<Type>& inferredReturnTypes) {
    inferredReturnTypes.push_back(getEdgeIteratorType(context, adaptor.getColumnsToFilter()));
    return success();
}

// An in-edges fetch produces the same row of edge chunks as out-edges, plus the
// same carried chunks; the input chunk names the nodes whose in-edges are gathered
LogicalResult GetInEdges::inferReturnTypes(MLIRContext* context,
                                           std::optional<Location> location,
                                           GetInEdges::Adaptor adaptor,
                                           SmallVectorImpl<Type>& inferredReturnTypes) {
    inferredReturnTypes.push_back(getEdgeIteratorType(context, adaptor.getColumnsToFilter()));
    return success();
}

// A cross product yields one chunk per crossed column - the outer columns
// followed by the inner - each keeping its input chunk's element type, since
// the broadcast only changes the row count, not the element kind.
LogicalResult CrossProduct::inferReturnTypes(MLIRContext* context,
                                             std::optional<Location> location,
                                             CrossProduct::Adaptor adaptor,
                                             SmallVectorImpl<Type>& inferredReturnTypes) {
    for (const Type columnType : adaptor.getOuterColumns().getTypes()) {
        inferredReturnTypes.push_back(columnType);
    }

    for (const Type columnType : adaptor.getInnerColumns().getTypes()) {
        inferredReturnTypes.push_back(columnType);
    }

    return success();
}

// A truncate passes its columns through unchanged - only the row count is cut -
// so each result keeps its input column's chunk type.
LogicalResult LimitTruncate::inferReturnTypes(MLIRContext* context,
                                              std::optional<Location> location,
                                              LimitTruncate::Adaptor adaptor,
                                              SmallVectorImpl<Type>& inferredReturnTypes) {
    for (const Type columnType : adaptor.getColumns().getTypes()) {
        inferredReturnTypes.push_back(columnType);
    }

    return success();
}

// A skip truncate, like a limit truncate, passes its columns through unchanged -
// only the dropped prefix is removed - so each result keeps its input column's
// chunk type.
LogicalResult SkipTruncate::inferReturnTypes(MLIRContext* context,
                                             std::optional<Location> location,
                                             SkipTruncate::Adaptor adaptor,
                                             SmallVectorImpl<Type>& inferredReturnTypes) {
    for (const Type columnType : adaptor.getColumns().getTypes()) {
        inferredReturnTypes.push_back(columnType);
    }

    return success();
}

void For::build(OpBuilder& builder, OperationState& state, Value iterator) {
    For::build(builder, state, iterator, Value());
}

// Build a loop from the iterator value and an optional limit handle: the
// iterator type carries the chunk types, which become the loop variables (the
// body block arguments); a non-null limit handle is added as the second
// operand, so the loop early-exits once that budget is spent. The body is
// created with its implicit nl.yield terminator, ready for the caller to set
// the insertion point into it.
void For::build(OpBuilder& builder, OperationState& state, Value iterator, Value limit) {
    const OpBuilder::InsertionGuard guard(builder);

    state.addOperands(iterator);

    // The limit handle is optional; a null value leaves the loop unbounded.
    if (limit) {
        state.addOperands(limit);
    }

    const auto iteratorType = cast<IteratorType>(iterator.getType());
    const ArrayRef<Type> chunkTypes = iteratorType.getChunkTypes();
    const llvm::SmallVector<Location> chunkLocations(chunkTypes.size(), state.location);

    Region* bodyRegion = state.addRegion();
    builder.createBlock(bodyRegion, bodyRegion->end(), chunkTypes, chunkLocations);
    For::ensureTerminator(*bodyRegion, builder, state.location);
}

// Custom syntax, in the style of scf.for / LingoDB's dsa.for:
//
//   nl.for %chunk0, %chunk1, ... in %iterator : !nl.iter<...> { body }
//
// The loop variables are the entry block arguments of the body region. Their
// types are not spelled out in the text: each variable takes the corresponding
// chunk type of the iterator.
ParseResult For::parse(OpAsmParser& parser, OperationState& result) {
    llvm::SmallVector<OpAsmParser::Argument, 4> chunkArguments;
    OpAsmParser::UnresolvedOperand iterator;

    // Loop variables, `in` keyword, iterator operand
    const bool headerFailed = parser.parseArgumentList(chunkArguments)
                              || parser.parseKeyword("in")
                              || parser.parseOperand(iterator);
    if (headerFailed) {
        return failure();
    }

    // Optional `limit %handle` between the iterator and its type, naming the
    // loop's row-limit counter so it stops once the budget is spent
    OpAsmParser::UnresolvedOperand limit;
    const bool hasLimit = succeeded(parser.parseOptionalKeyword("limit"));
    if (hasLimit && parser.parseOperand(limit)) {
        return failure();
    }

    if (parser.parseColon()) {
        return failure();
    }

    const llvm::SMLoc typeLocation = parser.getCurrentLocation();
    Type iteratorType;
    if (parser.parseType(iteratorType)) {
        return failure();
    }

    const auto chunkIteratorType = dyn_cast<IteratorType>(iteratorType);
    if (!chunkIteratorType) {
        return parser.emitError(typeLocation, "expected an !nl.iter iterator type");
    }

    // Bind one loop variable per chunk produced by an iterator step
    const ArrayRef<Type> chunkTypes = chunkIteratorType.getChunkTypes();
    if (chunkArguments.size() != chunkTypes.size()) {
        return parser.emitError(typeLocation, "expected one loop variable per iterator chunk");
    }

    for (size_t chunkIndex = 0; chunkIndex < chunkArguments.size(); chunkIndex++) {
        chunkArguments[chunkIndex].type = chunkTypes[chunkIndex];
    }

    if (parser.resolveOperand(iterator, iteratorType, result.operands)) {
        return failure();
    }

    // Resolve the optional limit operand after the iterator, so it lands at
    // operand index 1; its type is the buildable !nl.limit_state handle.
    if (hasLimit) {
        const Type limitType = LimitStateType::get(parser.getContext());
        if (parser.resolveOperand(limit, limitType, result.operands)) {
            return failure();
        }
    }

    // The body region, with the loop variables as entry block arguments
    Region* bodyRegion = result.addRegion();
    if (parser.parseRegion(*bodyRegion, chunkArguments)) {
        return failure();
    } else if (parser.parseOptionalAttrDict(result.attributes)) {
        return failure();
    }

    // The nl.yield terminator may be left out in the text; insert it
    For::ensureTerminator(*bodyRegion, parser.getBuilder(), result.location);

    return success();
}

void For::print(OpAsmPrinter& printer) {
    // Loop variables: the entry block arguments of the body region
    printer << " ";
    llvm::interleaveComma(getBody()->getArguments(), printer, [&](BlockArgument chunkArgument) {
        printer << chunkArgument;
    });

    const Value iterator = getIterator();
    printer << " in " << iterator;

    // The optional limit handle prints between the iterator and its type,
    // mirroring the parse order: `%vars in %iter limit %h : !nl.iter<...>`.
    if (const Value limit = getLimit()) {
        printer << " limit " << limit;
    }

    printer << " : " << iterator.getType() << " ";

    // The loop variables are already printed in the loop header, and the
    // implicit nl.yield terminator carries nothing: elide both.
    printer.printRegion(getBodyRegion(), /*printEntryBlockArgs=*/false, /*printBlockTerminators=*/false);
    printer.printOptionalAttrDict((*this)->getAttrs());
}

LogicalResult For::verify() {
    const auto iteratorType = cast<IteratorType>(getIterator().getType());
    const ArrayRef<Type> chunkTypes = iteratorType.getChunkTypes();
    Block* body = getBody();

    if (body->getNumArguments() != chunkTypes.size()) {
        return emitOpError("expects the body to take one block argument per iterator chunk");
    }

    for (size_t chunkIndex = 0; chunkIndex < chunkTypes.size(); chunkIndex++) {
        const Type argumentType = body->getArgument(static_cast<unsigned>(chunkIndex)).getType();
        if (argumentType != chunkTypes[chunkIndex]) {
            return emitOpError("body argument ") << chunkIndex
                                                 << " must have the iterator chunk type "
                                                 << chunkTypes[chunkIndex];
        }
    }

    return success();
}

