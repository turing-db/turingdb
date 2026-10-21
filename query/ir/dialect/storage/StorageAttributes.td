#ifndef STORAGE_ATTRIBUTES
#define STORAGE_ATTRIBUTES

include "mlir/IR/AttrTypeBase.td"
include "mlir/IR/BuiltinAttributeInterfaces.td"

include "StorageDialect.td"

def ParameterAttr : AttrDef<Storage, "Parameter", [TypedAttrInterface]> {
    let mnemonic = "parameter";

    let summary = "A query parameter, bound by name when the program is translated";
    let description = [{
        The type is what the bind checks the value against. A label parameter carries
        !storage.label_id, an edge type parameter !storage.edge_type_id, and a value
        parameter the column type the value is read into. A value parameter whose bind
        accepts several types is !storage.dynamic, which names them.

          #storage.parameter<"min"> : i64
          #storage.parameter<"p"> : !storage.dynamic<[DateTime, Duration]>
    }];

    let parameters = (ins StringRefParameter<"name">:$name,
                          AttributeSelfTypeParameter<"">:$type);

    let assemblyFormat = "`<` $name `>`";
    let genVerifyDecl = 1;

    let extraClassDeclaration = [{
        ::db::ValueTypeSet getAcceptedSet() const;
    }];
}

#endif // STORAGE_ATTRIBUTES
