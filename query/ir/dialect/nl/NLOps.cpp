#include "NLOps.h"
#include "NLDialect.h"

#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/SmallVector.h"

using namespace mlir;
using namespace mlir::nl;

#define GET_OP_CLASSES
#include "NLOps.cpp.inc"

namespace {

Type getNodeIDChunkType(MLIRContext* context) {
    return ChunkType::get(context, NodeIDType::get(context));
}

Type getEdgeIDChunkType(MLIRContext* context) {
    return ChunkType::get(context, EdgeIDType::get(context));
}

Type getEdgeTypeIDChunkType(MLIRContext* context) {
    return ChunkType::get(context, EdgeTypeIDType::get(context));
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

// Build a loop from just the iterator value: its iterator type carries the
// chunk types, which become the loop variables (the body block arguments).
// The body is created with its implicit nl.yield terminator, ready for the
// caller to set the insertion point into it.
void For::build(OpBuilder& builder, OperationState& state, Value iterator) {
    const OpBuilder::InsertionGuard guard(builder);

    state.addOperands(iterator);

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

    // Loop variables, `in` keyword, iterator operand, `:` of the iterator type
    const bool headerFailed = parser.parseArgumentList(chunkArguments)
                              || parser.parseKeyword("in")
                              || parser.parseOperand(iterator)
                              || parser.parseColon();
    if (headerFailed) {
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

    printer << " in " << getIterator() << " : " << getIterator().getType() << " ";

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
