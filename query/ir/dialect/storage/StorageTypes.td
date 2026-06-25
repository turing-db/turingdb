#ifndef STORAGE_TYPES
#define STORAGE_TYPES

include "mlir/IR/AttrTypeBase.td"

include "StorageDialect.td"

class StorageType<string name, string typeMnemonic, list<Trait> traits = []>
    : TypeDef<Storage, name, traits> {
  let mnemonic = typeMnemonic;
}

#endif // STORAGE_TYPES
