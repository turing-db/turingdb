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

def LabelID : StorageType<"LabelID", "label_id"> {
    let summary = "A single label identifier";
    let description = [{
        The element type of a chunk holding label IDs - one label, where
        !storage.labelset_id names a whole set. A CALL of a procedure yielding a
        LABEL_ID column (db.labels' `id`) produces such a chunk.
    }];
}

def PropertyTypeID : StorageType<"PropertyTypeID", "property_type_id"> {
    let summary = "A property type identifier";
    let description = [{
        The element type of a chunk holding property type IDs - the schema handle
        an !nl.property_type resolves a name to, here as a column value rather
        than a resolved handle. A CALL of a procedure yielding a PROPERTY_TYPE_ID
        column (db.propertyTypes' `id`) produces such a chunk.
    }];
}

def ValueType : StorageType<"ValueType", "value_type"> {
    let summary = "The kind of a property value";
    let description = [{
        The element type of a chunk holding value-type codes - storage's
        ValueType enum (String, Int64, Double, ...), the *kind* of a value rather
        than a value of that kind. A CALL of a procedure yielding a VALUE_TYPE
        column (db.propertyTypes' `valueType`) produces such a chunk.
    }];
}

def OwnedString : StorageType<"OwnedString", "owned_string"> {
    let summary = "A string value owned by the column";
    let description = [{
        The owning counterpart of !storage.string. A !storage.string row is a
        string_view borrowed from the graph, so it may only name characters that
        live in storage; a row of this type owns its characters, which is what a
        producer that formats its own text needs (a procedure yielding a STRING
        column, e.g. db.procedures' `signature`). The two are distinct types
        because they are distinct columns - a view column and an owning one - and
        a chunk must say which one it is.
    }];
}

def Embedding : StorageType<"Embedding", "embedding"> {
    let summary = "An embedding property value";
    let description = [{
        The element type an embedding property's nullable value chunk wraps. The
        vector (a span of floats per row) lives in storage; the dimension is part
        of the schema, not spelled here, so this is a plain placeholder type.
    }];
}

def ChangeID : StorageType<"ChangeID", "change_id"> {
    let summary = "A change identifier";
    let description = [{
        The element type of a chunk holding change IDs - storage's ChangeID. It
        wraps a 64-bit value but is not an integer: it is rendered in hexadecimal
        and only ever compared for identity, so a plain i64 would print the wrong
        thing. Produced by db.change / nl.change, which report the changes an
        operation created, submitted, deleted or listed.
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

def ListElement : StorageType<"ListElement", "list_element"> {
    let summary = "A single type-tagged scalar value";
    let description = [{
        The element type of a chunk holding one type-tagged scalar per row - storage's
        ColumnVector<ListElementView>. Each cell carries its own type tag beside its
        value, so a heterogeneous list - whose elements share no single type - unwinds
        into a column of these. Unlike !storage.list it is a fixed-width scalar, not a
        variable-length list: one value per cell, not a span, so it needs none of the
        list machinery. Produced by db.unwind_const for a heterogeneous (or empty)
        literal list, and only ever passed through (a cross_product broadcast) or
        rendered - never fed to an op that needs a concrete element type.
    }];
}

def List : StorageType<"List", "list"> {
    let summary = "A list value";
    let description = [{
        The element type of a chunk holding one list per row - storage's
        ColumnVector<ListView>, per docs/lists.md. Each cell is a ListView: a span
        over the list's elements in the query-scoped ListBuffer, so a row copies in
        O(1) regardless of list length. The wrapped type is the (homogeneous) element
        type - `!storage.list<none>` until the element primitive is resolved. It is
        produced by a collect and drained element-by-element by an unwind, and by a
        CALL of a procedure yielding a LIST return value.
    }];
    let parameters = (ins "::mlir::Type":$elementType);
    let assemblyFormat = "`<` $elementType `>`";
}

// One string array per entity of a MERGE pattern: the label set of each chain node,
// or the property names of each node and each hop. Both db.merge and nl.merge carry
// these lists, so the constraint lives beside the types the two dialects share.
def StrArrayArrayAttr : TypedArrayAttrBase<StrArrayAttr, "array of string arrays">;

#endif // STORAGE_TYPES
