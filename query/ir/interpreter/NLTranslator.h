#pragma once

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "llvm/ADT/DenseMap.h"
#include "llvm/ADT/SmallVector.h"

#include "metadata/PropertyType.h"

#include "NLOps.h"

#include "NLProgram.h"

namespace db {

class LocalMemory;
class GraphView;

// Translates an MLIR func.func in the nl dialect into an NLProgram
class NLTranslator {
public:
    NLTranslator(NLProgram* program, LocalMemory* memory, const GraphView* view);
    ~NLTranslator();

    void translate(const mlir::func::FuncOp& function);

private:
    // Kind of iterators passed to each for loop
    enum class IteratorKind {
        ScanNodes,
        GetOutEdges,
        GetInEdges,
    };

    // Settings of the iterators passed to each for loop
    struct IteratorConfig {
        IteratorKind _kind {IteratorKind::ScanNodes};
        mlir::Value _inputNodes;
        llvm::SmallVector<mlir::Value, 4> _carriedColumns;
    };

    NLProgram* _program {nullptr};
    LocalMemory* _memory {nullptr};
    const GraphView* _view {nullptr};
    llvm::DenseMap<mlir::Value, Column*> _valueSlots;
    llvm::DenseMap<mlir::Value, IteratorConfig> _iteratorConfigs;

    // nl.limit handle SSA value -> the runtime counter it produces, so the loops,
    // nl.limit_update and nl.output that name the handle find the same counter
    llvm::DenseMap<mlir::Value, NLLimitState*> _limitStates;

    void translateBlock(mlir::Block& block, NLStmtContainer* body);
    void translateFor(mlir::nl::For forLoop, NLStmtContainer* body);
    void translateScanLoop(mlir::Block& loopBody, NLLimitState* limit, NLStmtContainer* body);
    void translateEdgeLoop(const IteratorConfig& config,
                           mlir::Block& loopBody,
                           NLLimitState* limit,
                           NLStmtContainer* body);

    // Translate an nl.limit: allocate its runtime counter, map the handle to it,
    // and record the reset statement (run each time the enclosing block runs)
    void translateLimit(mlir::nl::Limit limit, NLStmtContainer* body);

    // Translate an nl.limit_update: look up the counter the handle names and
    // record the charge against the representative chunk's row count
    void translateLimitUpdate(mlir::nl::LimitUpdate update, NLStmtContainer* body);

    // The runtime counter an optional limit handle names: null for a null handle
    // (an unbounded loop or output), the mapped counter otherwise. Throws if the
    // handle was not produced by an nl.limit translated earlier.
    NLLimitState* limitStateFor(mlir::Value handle) const;

    // Translate an nl.get_node_properties / nl.get_edge_properties: resolve the
    // property name (carried by the nl.get_property_type that produced the
    // handle) to a PropertyTypeID and value type, allocate the nullable value
    // column, and record the with-null fetch statement in body
    void translatePropertyFetch(mlir::Value inputValue,
                                mlir::Value propertyTypeValue,
                                mlir::Value resultValue,
                                bool isNode,
                                NLStmtContainer* body);
    void translateOutput(mlir::nl::Output output, NLStmtContainer* body);

    // Translate an nl.cross_product: allocate an output column per crossed
    // column, map each to the matching op result, and record the broadcast
    // statement (outer columns block-repeated, inner columns tiled)
    void translateCrossProduct(mlir::nl::CrossProduct cross, NLStmtContainer* body);

    // Allocate the output column for one crossed column, map the op result to
    // it, and append it (with its block-repeat/tile broadcast) to the outer or
    // inner list of data
    void addCrossColumn(mlir::Value inputValue,
                        mlir::Value resultValue,
                        bool isOuter,
                        NLCrossProductData* data);

    Column* allocColumn(mlir::Value chunkValue);
    Column* allocColumnForKind(NLChunkKind kind);
    Column* allocOptColumnForValueType(ValueType valueType);
    Column* getColumn(mlir::Value chunkValue) const;
    static NLChunkKind getChunkKind(mlir::Type chunkType);
};

}
