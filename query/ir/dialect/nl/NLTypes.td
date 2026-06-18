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

def NLString : NLType<"String", "string"> {
    let summary = "A string property value";
    let description = [{
        The element type a string property's nullable value chunk wraps. Strings
        have no MLIR builtin type, so the dialect names one; the characters live
        in storage (a string_view per row), this is only the IR placeholder.
    }];
}

def NLEmbedding : NLType<"Embedding", "embedding"> {
    let summary = "An embedding property value";
    let description = [{
        The element type an embedding property's nullable value chunk wraps. The
        vector (a span of floats per row) lives in storage; the dimension is part
        of the schema, not spelled here, so this is a plain placeholder type.
    }];
}

def NLNullable : NLType<"Nullable", "nullable"> {
    let summary = "A nullable property value";
    let description = [{
        The element type of a property value chunk: a value of the wrapped type
        that may be absent, mirroring storage's ColumnOptVector. The wrapped type
        is the concrete primitive the property resolved to during lowering (f64,
        i64, ...), so a property chunk is fully typed before execution. Missing
        values are null; no row is dropped.
    }];
    let parameters = (ins "::mlir::Type":$valueType);
    let assemblyFormat = "`<` $valueType `>`";
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

def NLPropertyType : NLType<"PropertyType", "property_type"> {
    let summary = "A resolved property type handle";
    let description = [{
        Stands for one property of the graph - storage's PropertyTypeID - once
        its name has been resolved. Produced by nl.get_property_type ahead of
        the loops and consumed by the property fetches, so the name is looked up
        once rather than per chunk. It is not a chunk element type: it names a
        property, it does not hold values.
    }];
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

// The edge-chunk counterpart of NLNodeIDChunk: constrains an operand to
// !nl.chunk<!nl.edge_id> and carries a BuildableType recipe so an assembly
// format can omit the type. See NLNodeIDChunk above for the piece-by-piece
// breakdown of Type<CPred, summary, cppClass> + BuildableType.
def NLEdgeIDChunk : Type<
        CPred<"::llvm::isa<::mlir::nl::ChunkType>($_self) && "
              "::llvm::isa<::mlir::nl::EdgeIDType>(::llvm::cast<::mlir::nl::ChunkType>($_self).getElementType())">,
        "chunk of edge IDs", "::mlir::nl::ChunkType">,
    BuildableType<"::mlir::nl::ChunkType::get($_builder.getContext(), "
                  "::mlir::nl::EdgeIDType::get($_builder.getContext()))">;

// Constrains an operand to a resolved !nl.property_type handle, the same way
// NLNodeIDChunk constrains a chunk operand. The single concrete type - there is
// only ever !nl.property_type - is what lets the property fetches omit the
// handle's type in their assembly format, building it from this recipe instead.
def NLPropertyTypeHandle : Type<
        CPred<"::llvm::isa<::mlir::nl::PropertyTypeType>($_self)">,
        "property type handle", "::mlir::nl::PropertyTypeType">,
    BuildableType<"::mlir::nl::PropertyTypeType::get($_builder.getContext())">;

#endif // TURINGDB_NL_TYPES
