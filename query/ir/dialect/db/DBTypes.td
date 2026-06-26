#ifndef TURINGDB_TYPES
#define TURINGDB_TYPES

include "mlir/IR/AttrTypeBase.td"

include "DBDialect.td"
include "StorageTypes.td"

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
    // FIXME: Remove this
    let extraClassDeclaration = [{
        static ColumnType get(::mlir::MLIRContext* ctx) {
            return get(ctx, ::mlir::NoneType::get(ctx));
        }
    }];
}

// Helper macro to define a strictly typed column
//             type name       display name
class ColumnOf<string cppType, string mnemonic> : Type<
      CPred<"llvm::isa<::mlir::db::ColumnType>($_self) && "
            "llvm::isa<" # cppType # ">("
            "  llvm::cast<::mlir::db::ColumnType>($_self).getType())">,
      "column<" # mnemonic # ">">;

def ColumnNodeIDs : ColumnOf<"::mlir::storage::NodeIDType", "nodeid">;
def ColumnEdgeIDs : ColumnOf<"::mlir::storage::EdgeIDType", "edgeid">;
def ColumnEdgeTypes : ColumnOf<"::mlir::storage::EdgeTypeIDType", "edge_type_id">;

#endif // TURINGDB_TYPES
