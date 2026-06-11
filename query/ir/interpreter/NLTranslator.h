#pragma once

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "llvm/ADT/DenseMap.h"
#include "llvm/ADT/SmallVector.h"

#include "NLOps.h"

#include "NLProgram.h"

namespace db {

// Translates one func.func of nl ops into an NLProgram, once. This is the
// only MLIR-facing file of the interpreter: translation is where SSA chunk
// values become preallocated slots and handlers are selected, so the runtime
// never touches MLIR and the module can be destroyed after translating.
class NLTranslator {
public:
    explicit NLTranslator(NLProgram& program);
    ~NLTranslator();

    void translate(mlir::func::FuncOp function);

private:
    // The source ops (nl.scan_nodes, nl.get_out_edges, nl.get_in_edges) emit
    // no descriptor: they only configure the iterator that a consuming nl.for
    // drives, so translation records one config per iterator value and folds
    // it into the loop that uses it.
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

    NLProgram& _program;
    llvm::DenseMap<mlir::Value, Column*> _valueSlots;
    llvm::DenseMap<mlir::Value, IteratorConfig> _iteratorConfigs;

    void translateBlock(mlir::Block& block, std::vector<NLFunctionDescriptor>& body);
    void translateFor(mlir::nl::For forLoop, std::vector<NLFunctionDescriptor>& body);
    void translateScanLoop(mlir::Block* loopBody, std::vector<NLFunctionDescriptor>& body);
    void translateEdgeLoop(const IteratorConfig& config,
                           mlir::Block* loopBody,
                           std::vector<NLFunctionDescriptor>& body);
    void translateOutput(mlir::nl::Output output, std::vector<NLFunctionDescriptor>& body);

    Column* resolveChunkSlot(mlir::Value chunkValue);
    static NLChunkKind chunkKindOf(mlir::Type chunkType);
};

}
