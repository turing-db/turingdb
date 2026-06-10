#include "NLOps.h"
#include "NLDialect.h"

#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/SmallVector.h"

using namespace mlir;
using namespace mlir::nl;

#define GET_OP_CLASSES
#include "NLOps.cpp.inc"

namespace {

bool isChunkOf(Type type, auto isElement) {
    const auto chunkType = dyn_cast<ChunkType>(type);
    return chunkType && isElement(chunkType.getElementType());
}

bool isNodeIDChunk(Type type) {
    return isChunkOf(type, [](Type element) { return isa<NodeIDType>(element); });
}

bool isEdgeIDChunk(Type type) {
    return isChunkOf(type, [](Type element) { return isa<EdgeIDType>(element); });
}

bool isEdgeTypeIDChunk(Type type) {
    return isChunkOf(type, [](Type element) { return isa<EdgeTypeIDType>(element); });
}

}

LogicalResult ScanNodes::verify() {
    const auto iteratorType = cast<IteratorType>(getResult().getType());
    const ArrayRef<Type> chunkTypes = iteratorType.getChunkTypes();

    const bool yieldsNodeIDChunk = chunkTypes.size() == 1 && isNodeIDChunk(chunkTypes[0]);
    if (!yieldsNodeIDChunk) {
        return emitOpError("must return an iterator yielding one chunk of node IDs");
    }

    return success();
}

LogicalResult GetOutEdges::verify() {
    const auto inputType = cast<ChunkType>(getInputNodes().getType());
    const auto iteratorType = cast<IteratorType>(getResult().getType());
    const ArrayRef<Type> chunkTypes = iteratorType.getChunkTypes();

    const bool yieldsEdgeChunks = chunkTypes.size() == 4
                                  && isNodeIDChunk(chunkTypes[0])
                                  && isEdgeIDChunk(chunkTypes[1])
                                  && isEdgeTypeIDChunk(chunkTypes[2])
                                  && isNodeIDChunk(chunkTypes[3]);

    if (!isa<NodeIDType>(inputType.getElementType())) {
        return emitOpError("expects a chunk of node IDs as input");
    } else if (!yieldsEdgeChunks) {
        return emitOpError("must return an iterator yielding chunks of source node IDs, "
                           "edge IDs, edge type IDs and target node IDs");
    }

    return success();
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
