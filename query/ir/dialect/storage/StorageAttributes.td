#ifndef STORAGE_ATTRIBUTES
#define STORAGE_ATTRIBUTES

include "mlir/IR/AttrTypeBase.td"
include "mlir/IR/BuiltinAttributeInterfaces.td"

include "StorageDialect.td"

def ParameterAttr : AttrDef<Storage, "Parameter", [TypedAttrInterface]> {
    let mnemonic = "parameter";

    let summary = "A query parameter, bound by name when the program is translated";

    let parameters = (ins StringRefParameter<"name">:$name, AttributeSelfTypeParameter<"">:$type);

    let assemblyFormat = "`<` $name `>`";
}

#endif // STORAGE_ATTRIBUTES
