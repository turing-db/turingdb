#ifndef TURINGDB_NL_TYPES
#define TURINGDB_NL_TYPES

include "mlir/IR/AttrTypeBase.td"

include "NLDialect.td"

// Base type. All nested-loop dialect types derived from this.
class NLType<string name, string typeMnemonic, list<Trait> traits = []>
    : TypeDef<NL, name, traits> {
  let mnemonic = typeMnemonic;
}

def NLNodeID : NLType<"NodeID", "node_id"> {
    let summary = "A node identifier";
    let description = "The element type of a chunk holding node IDs";
}

def NLEdgeID : NLType<"EdgeID", "edge_id"> {
    let summary = "An edge identifier";
    let description = "The element type of a chunk holding edge IDs";
}

def NLEdgeTypeID : NLType<"EdgeTypeID", "edge_type_id"> {
    let summary = "An edge type identifier";
    let description = "The element type of a chunk holding edge type IDs";
}

def NLChunk : NLType<"Chunk", "chunk"> {
    let summary = "A bounded batch of values";
    let description = "One column chunk filled by a single database iterator step";
    let parameters = (ins "::mlir::Type":$elementType);
    let assemblyFormat = "`<` $elementType `>`";
}

def NLIterator : NLType<"Iterator", "iter"> {
    let summary = "A database iterator producing chunks";
    let description = "Each step of the iterator fills one chunk per parameter type";
    let parameters = (ins ArrayRefParameter<"::mlir::Type">:$chunkTypes);
    let assemblyFormat = "`<` $chunkTypes `>`";
}

// NLNodeIDChunk is a type *constraint*, not a new type: it restricts an op
// operand to !nl.chunk<!nl.node_id> without defining anything new. Ops use it
// in their (ins ...) clause where NLChunk would accept a chunk of anything.
//
// It is built from two tablegen pieces:
//
// - Type<predicate, summary, cppClassName>: a constraint over types.
//   * The CPred is a C++ expression evaluated against the operand type
//     ($_self) by the generated op verifier: the type must be a ChunkType
//     and its element type must be NodeIDType.
//   * "chunk of node IDs" is the wording used in verifier error messages.
//   * ::mlir::nl::ChunkType makes the generated operand accessor return
//     ChunkType instead of the generic mlir::Type.
//
// - BuildableType<builderCall>: a recipe to construct, via an OpBuilder
//   ($_builder), the single concrete type satisfying the constraint. This is
//   what lets assembly formats omit the operand type entirely: the parser
//   does not read the type from the text, it builds it from this recipe.
def NLNodeIDChunk : Type<
        CPred<"::llvm::isa<::mlir::nl::ChunkType>($_self) && "
              "::llvm::isa<::mlir::nl::NodeIDType>(::llvm::cast<::mlir::nl::ChunkType>($_self).getElementType())">,
        "chunk of node IDs", "::mlir::nl::ChunkType">,
    BuildableType<"::mlir::nl::ChunkType::get($_builder.getContext(), "
                  "::mlir::nl::NodeIDType::get($_builder.getContext()))">;

#endif // TURINGDB_NL_TYPES
