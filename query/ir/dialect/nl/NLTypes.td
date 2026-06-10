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

#endif // TURINGDB_NL_TYPES
