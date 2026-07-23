#ifndef STORAGE_TYPES
#define STORAGE_TYPES

include "mlir/IR/AttrTypeBase.td"

include "StorageDialect.td"
include "StorageEnums.td"

class StorageType<string name, string typeMnemonic, list<Trait> traits = []>
    : TypeDef<Storage, name, traits> {
  let mnemonic = typeMnemonic;
}

def NodeID : StorageType<"NodeID", "node_id"> {
    let summary = "A node identifier";
    let description = "The element type of a chunk holding node IDs";
}

def EdgeID : StorageType<"EdgeID", "edge_id"> {
    let summary = "An edge identifier";
    let description = "The element type of a chunk holding edge IDs";
}

def EdgeTypeID : StorageType<"EdgeTypeID", "edge_type_id"> {
    let summary = "An edge type identifier";
    let description = "The element type of a chunk holding edge type IDs";
}

def String : StorageType<"String", "string"> {
    let summary = "A string property value";
    let description = [{
        The element type a string property's nullable value chunk wraps. Strings
        have no MLIR builtin type, so the dialect names one; the characters live
        in storage (a string_view per row), this is only the IR placeholder.
    }];
}

def Bool : StorageType<"Bool", "bool"> {
    let summary = "TuringDB Boolean type";
}

def LabelSetID : StorageType<"LabelSetID", "labelset_id"> {
    let summary = "A label-set identifier";
    let description = "The element type of a chunk holding label set IDs";
}

def Embedding : StorageType<"Embedding", "embedding"> {
    let summary = "An embedding property value";
    let description = [{
        The element type an embedding property's nullable value chunk wraps. The
        vector (a span of floats per row) lives in storage; the dimension is part
        of the schema, not spelled here, so this is a plain placeholder type.
    }];
}

def Nullable : StorageType<"Nullable", "nullable"> {
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

#endif // STORAGE_TYPES
