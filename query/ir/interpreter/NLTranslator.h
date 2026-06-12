#pragma once

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "llvm/ADT/DenseMap.h"
#include "llvm/ADT/SmallVector.h"

#include "NLOps.h"

#include "NLProgram.h"

namespace db {

// Translates an MLIR func.func in the nl dialect into an NLProgram
class NLTranslator {
public:
    explicit NLTranslator(NLProgram* program);
    ~NLTranslator();

    void translate(const mlir::func::FuncOp& function);

private:
    enum class IteratorKind {
        ScanNodes,
        GetOutEdges,
        GetInEdges,
    };

    struct IteratorConfig {
        IteratorKind _kind {IteratorKind::ScanNodes};
        mlir::Value _inputNodes;
        llvm::SmallVector<mlir::Value, 4> _carriedColumns;
    };

    NLProgram* _program {nullptr};
    llvm::DenseMap<mlir::Value, Column*> _valueSlots;
    llvm::DenseMap<mlir::Value, IteratorConfig> _iteratorConfigs;

    void translateBlock(mlir::Block& block, NLStmtContainer* body);
    void translateFor(const mlir::nl::For& forLoop, NLStmtContainer* body);
    void translateScanLoop(mlir::Block& loopBody, NLStmtContainer* body);
    void translateEdgeLoop(const IteratorConfig& config,
                           mlir::Block& loopBody,
                           NLStmtContainer* body);
    void translateOutput(const mlir::nl::Output& output, NLStmtContainer* body);

    Column* allocColumn(mlir::Value chunkValue);
    Column* getColumn(mlir::Value chunkValue) const;
    static NLChunkKind getChunkKind(mlir::Type chunkType);
};

}
