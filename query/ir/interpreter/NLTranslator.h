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
        Sort,
    };

    // Settings of the iterators passed to each for loop
    struct IteratorConfig {
        IteratorKind _kind {IteratorKind::ScanNodes};
        mlir::Value _inputNodes;
        llvm::SmallVector<mlir::Value, 4> _carriedColumns;

        // The accumulator a Sort iterator drains; null for the other kinds.
        NLSortState* _sortState {nullptr};
    };

    NLProgram* _program {nullptr};
    LocalMemory* _memory {nullptr};
    const GraphView* _view {nullptr};
    llvm::DenseMap<mlir::Value, Column*> _valueSlots;
    llvm::DenseMap<mlir::Value, IteratorConfig> _iteratorConfigs;

    // nl.limit handle SSA value -> the runtime counter it produces, so the loops,
    // nl.limit_update and nl.output that name the handle find the same counter
    llvm::DenseMap<mlir::Value, NLLimitState*> _limitStates;

    // nl.skip handle SSA value -> the runtime counter it produces, so nl.skip_update
    // and nl.skip_truncate that name the handle find the same counter
    llvm::DenseMap<mlir::Value, NLSkipState*> _skipStates;

    // nl.sort_buffer handle SSA value -> the runtime accumulator it produces, so
    // nl.sort_collect and the nl.for over nl.sort find the same buffers
    llvm::DenseMap<mlir::Value, NLSortState*> _sortStates;

    // nl.distinct handle SSA value -> the runtime seen-set it produces, so the
    // nl.distinct_filter that names the handle finds the same set
    llvm::DenseMap<mlir::Value, NLDistinctState*> _distinctStates;

    // nl.count handle SSA value -> the runtime tally it produces, so the
    // nl.count_update and nl.count_result that name the handle find the same counter
    llvm::DenseMap<mlir::Value, NLCountState*> _countStates;

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

    // Translate an nl.limit_truncate: allocate one fresh output column per input,
    // map each result to its output, and record the prefix-copy statement (each
    // column block-repeated with factor 1 up to the counter's emitThisStep)
    void translateLimitTruncate(mlir::nl::LimitTruncate truncate, NLStmtContainer* body);

    // The runtime counter an optional limit handle names: null for a null handle
    // (an unbounded loop or output), the mapped counter otherwise. Throws if the
    // handle was not produced by an nl.limit translated earlier.
    NLLimitState* limitStateFor(mlir::Value handle) const;

    // Translate an nl.skip: allocate its runtime counter, map the handle to it,
    // and record the reset statement (run each time the enclosing block runs)
    void translateSkip(mlir::nl::Skip skip, NLStmtContainer* body);

    // Translate an nl.skip_update: look up the counter the handle names and record
    // the charge against the representative chunk's row count
    void translateSkipUpdate(mlir::nl::SkipUpdate update, NLStmtContainer* body);

    // Translate an nl.skip_truncate: allocate one fresh output column per input,
    // map each result to its output, and record the suffix-copy statement (each
    // column's surviving suffix copied to the front of the output)
    void translateSkipTruncate(mlir::nl::SkipTruncate truncate, NLStmtContainer* body);

    // The runtime counter a skip handle names. The handle is a required operand of
    // its consumers, so this throws if it was not produced by an nl.skip.
    NLSkipState* skipStateFor(mlir::Value handle) const;

    // Translate an nl.sort_buffer: allocate its runtime accumulator, map the
    // handle to it, and record the reset statement (run each time the block runs)
    void translateSortBuffer(mlir::nl::SortBuffer buffer, NLStmtContainer* body);

    // Translate an nl.sort_collect: allocate one growing buffer per collected
    // column (mapped into the accumulator), build the key comparators from the
    // nl.sort_buffer spec, and record the per-step append statement
    void translateSortCollect(mlir::nl::SortCollect collect, NLStmtContainer* body);

    // Translate the nl.for over an nl.sort iterator: allocate one loop variable
    // per buffer, set up the gather that fills it from the buffer in permutation
    // order, and record the emit-loop statement (sort once, then re-chunk)
    void translateSortLoop(const IteratorConfig& config,
                           mlir::Block& loopBody,
                           NLStmtContainer* body);

    // The runtime accumulator a sort handle names. Throws if the handle was not
    // produced by an nl.sort_buffer translated earlier.
    NLSortState* sortStateFor(mlir::Value handle) const;

    // Translate an nl.distinct: allocate its runtime seen-set, map the handle to
    // it, and record the reset statement (run each time the block runs)
    void translateDistinctState(mlir::nl::Distinct distinct, NLStmtContainer* body);

    // Translate an nl.distinct_filter: look up the seen-set the handle names,
    // allocate one fresh output column per input, map each result to its output,
    // and record the filter statement (serialize each row's key across all
    // columns, keep the not-yet-seen rows, gather them into the outputs)
    void translateDistinctFilter(mlir::nl::DistinctFilter filter, NLStmtContainer* body);

    // Allocate the fresh output column for one filtered column, map the op result
    // to it, and append it (with its key-append serializer and its survivor
    // gather) to data
    void addDistinctColumn(mlir::Value inputValue,
                           mlir::Value resultValue,
                           NLDistinctFilterData* data);

    // The runtime seen-set a distinct handle names. The handle is a required
    // operand of nl.distinct_filter, so this throws if it was not produced by an
    // nl.distinct.
    NLDistinctState* distinctStateFor(mlir::Value handle) const;

    // Translate an nl.count: allocate its runtime tally, map the handle to it, and
    // record the reset statement (run each time the block runs)
    void translateCountState(mlir::nl::Count count, NLStmtContainer* body);

    // Translate an nl.count_update: look up the tally the handle names and record
    // the charge of the chunk's non-null rows against it (all rows for an ID chunk,
    // the present values for a nullable value chunk)
    void translateCountUpdate(mlir::nl::CountUpdate update, NLStmtContainer* body);

    // Translate an nl.count_result: look up the tally the handle names, allocate the
    // unsigned i64 count chunk it produces, map the op result to it, and record the
    // emit statement (materialize the final tally as the chunk's single row)
    void translateCountResult(mlir::nl::CountResult result, NLStmtContainer* body);

    // The runtime tally a count handle names. The handle is a required operand of
    // nl.count_update and nl.count_result, so this throws if it was not produced by
    // an nl.count.
    NLCountState* countStateFor(mlir::Value handle) const;

    // Pool-allocate a buffer/loop column matching a chunk type - an ID column for
    // an ID chunk, a nullable value column for a !nl.nullable<...> chunk - and the
    // append/gather/compare handler for that element type. The compare selector
    // throws for chunk types that have no order (an embedding key).
    Column* allocColumnForChunkType(mlir::Type chunkType);
    static NLAppendFunction selectAppendForChunkType(mlir::Type chunkType);
    static NLGatherFunction selectGatherForChunkType(mlir::Type chunkType);
    static NLCompareFunction selectCompareForChunkType(mlir::Type chunkType);
    static NLKeyAppendFunction selectKeyAppendForChunkType(mlir::Type chunkType);

    // The non-null row count handler for a chunk type - the all-rows count for an
    // ID chunk, the present-value count for a !nl.nullable<...> chunk. Used by
    // nl.count_update.
    static NLCountFunction selectCountForChunkType(mlir::Type chunkType);

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

    // Allocate the fresh output column for one truncated column, map the op
    // result to it, and append it (with its block-repeat prefix-copy) to data
    void addTruncateColumn(mlir::Value inputValue,
                           mlir::Value resultValue,
                           NLLimitTruncateData* data);

    // Allocate the fresh output column for one skipped column, map the op result
    // to it, and append it (with its range suffix-copy) to data
    void addSkipColumn(mlir::Value inputValue,
                       mlir::Value resultValue,
                       NLSkipTruncateData* data);

    Column* allocColumn(mlir::Value chunkValue);
    Column* allocColumnForKind(NLChunkKind kind);
    Column* allocOptColumnForValueType(ValueType valueType);
    Column* getColumn(mlir::Value chunkValue) const;
    static NLChunkKind getChunkKind(mlir::Type chunkType);
    static NLChunkKind chunkKindFromElementType(mlir::Type elementType);
};

}
