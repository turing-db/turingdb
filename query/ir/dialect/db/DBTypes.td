#ifndef TURINGDB_TYPES
#define TURINGDB_TYPES

include "mlir/IR/AttrTypeBase.td"

include "DBDialect.td"

class TuringType<string name, string typeMnemonic, list<Trait> traits = []>
    : TypeDef<TuringDB, name, traits> {
  let mnemonic = typeMnemonic;
}

#endif // TURINGBB_TYPES
