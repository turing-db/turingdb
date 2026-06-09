#pragma once

#include <ostream>

namespace mlir {
class ModuleOp;
}

namespace db {

class IRModuleInspector {
public:
    IRModuleInspector(mlir::ModuleOp* mod)
        : _mod(mod)
    {
    }

    void dumpFunctionTypes(std::ostream& out) const;
    void dumpFunctions(std::ostream& out) const;

private:
    mlir::ModuleOp* _mod {nullptr};
};

}
