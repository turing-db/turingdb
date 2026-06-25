#ifndef STORAGE_DIALECT
#define STORAGE_DIALECT

include "mlir/IR/OpBase.td"

def Storage : Dialect {
    let name = "storage";
    let summary = "TuringDB storage dialect";
    let cppNamespace = "::mlir::storage";

    let useDefaultTypePrinterParser = 1;

    let extraClassDeclaration = [{
        void registerTypes();
    }];
}

class StorageOp<string mnemonic, list<Trait> traits = []>
    : Op<Storage, mnemonic, traits>;

#endif // STORAGE_DIALECT
