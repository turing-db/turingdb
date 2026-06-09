#pragma once

#include <vector>

#include "Path.h"

namespace mlir {
class MLIRContext;
class ModuleOp;
}

namespace db {

class IRAssembler {
public:
    explicit IRAssembler(mlir::MLIRContext* ctxt, mlir::ModuleOp* mod);
    ~IRAssembler();

    void addFile(const fs::Path& path);

    void assemble() const;

private:
    mlir::MLIRContext* _ctxt {nullptr};
    std::vector<fs::Path> _files;
    mlir::ModuleOp* _mod {nullptr};
};

}
