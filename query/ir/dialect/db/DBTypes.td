#ifndef TURINGDB_TYPES
#define TURINGDB_TYPES

include "mlir/IR/AttrTypeBase.td"

include "DBDialect.td"

// Base type. All turingdb dialect types derived from this.
class TuringType<string name, string typeMnemonic, list<Trait> traits = []>
    : TypeDef<DB, name, traits> {
  let mnemonic = typeMnemonic;
}

def Column : TuringType<"Column", "column"> {
    let summary = "Generic column type";
    let description = "Generic vector-like storage";

    let parameters = (ins "::mlir::Type":$type);
    let assemblyFormat = "`<` $type`>`";

    // Add default constructor with unspecified type
    let extraClassDeclaration = [{
        static ColumnType get(::mlir::MLIRContext* ctx) {
            return get(ctx, ::mlir::Type());
        }
    }];
}

def NodeID : TuringType<"NodeID", "nodeid"> {
    let summary = "TuringDB NodeID";
}

def EdgeID : TuringType<"EdgeID", "edgeid"> {
    let summary = "TuringDB EdgeID";
}

#endif // TURINGDB_TYPES
