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

// Internal ID types
def ColumnNodeIDs : ColumnOf<"::mlir::storage::NodeIDType", "nodeid">;
def ColumnEdgeIDs : ColumnOf<"::mlir::storage::EdgeIDType", "edgeid">;
def ColumnEdgeTypes : ColumnOf<"::mlir::storage::EdgeTypeIDType", "edge_type_id">;

def ColumnChangeIDs : ColumnOf<"::mlir::storage::ChangeIDType", "change_id">;

// Property Types
def ColumnBools : ColumnOf<"::mlir::storage::BoolType", "bool">;

// The two element types the system commands report their result tables in: a
// name or signature they looked up, and a size or tally they counted.
def ColumnStrings : ColumnOf<"::mlir::storage::StringType", "string">;
def ColumnCounts : ColumnOf<"::mlir::IntegerType", "ui64">;

// The two element types a vector search reports a neighbour in: the ID the index
// holds it under, and the distance the metric scored it at.
def ColumnInts : ColumnOf<"::mlir::IntegerType", "i64">;
def ColumnDoubles : ColumnOf<"::mlir::FloatType", "f64">;

#endif // TURINGDB_TYPES
