#pragma once

#include <memory>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "llvm/ADT/DenseMap.h"
#include "llvm/ADT/DenseSet.h"
#include "llvm/ADT/SmallVector.h"
#include "llvm/ADT/StringRef.h"

#include "columns/ColumnOperator.h"
#include "ProcedureTypeVector.h"
#include "metadata/PropertyType.h"

#include "NLOps.h"

#include "NLProgram.h"

namespace db {

class LocalMemory;
class GraphView;
class MetadataBuilder;
class NLSystemTranslator;
class Procedure;
class ProcedureContext;

// Translates an MLIR func.func in the nl dialect into an NLProgram
class NLTranslator {
public:
    NLTranslator(NLProgram* program,
                 LocalMemory* memory,
                 const GraphView* view,
                 MetadataBuilder* metadataBuilder = nullptr,
                 const ProcedureContext* procedureContext = nullptr);
    ~NLTranslator();

    void translate(const mlir::func::FuncOp& function);

private:
    // Kind of iterators passed to each for loop
    enum class IteratorKind {
        ScanNodes,
        ScanNodesByLabel,
        ConstScanNodes,
        ScanEdges,
        GetOutEdges,
        GetInEdges,
        GetEdges,
        GetOutEdgesByType,
        GetInEdgesByType,
        Sort,
        GroupAggregate,
        UnwindCollect,
        Collect,
        UnwindConst,
        VectorSearch,
        ProcedureInit,
    };

    // Settings of the iterators passed to each for loop
    struct IteratorConfig {
        IteratorKind _kind {IteratorKind::ScanNodes};
        mlir::Value _inputNodes;
        llvm::SmallVector<mlir::Value, 4> _carriedColumns;

        // The accumulator a Sort iterator drains; null for the other kinds.
        NLSortState* _sortState {nullptr};

        // The accumulator a GroupAggregate iterator drains; null for the other kinds.
        NLGroupAggregateState* _groupAggregateState {nullptr};

        // The accumulator an UnwindCollect / Collect iterator drains; null otherwise.
        NLCollectState* _collectState {nullptr};

        // The call a ProcedureInit iterator drives; null for the other kinds.
        NLProcedureState* _procedureState {nullptr};

        // The argument chunks a ProcedureInit iterator hands the procedure, in its
        // declaration order; empty for the other kinds (and for a source call).
        llvm::SmallVector<mlir::Value, 4> _procedureInputs;

        // The label names a ScanNodesByLabel iterator filters by; empty for the
        // other kinds. These are views into the op's interned StringAttr storage,
        // which the MLIRContext keeps alive for the whole translation; they are
        // resolved to a LabelSet as soon as the loop is translated.
        llvm::SmallVector<llvm::StringRef, 4> _labels;

        // The edge type name a GetOutEdgesByType / GetInEdgesByType iterator
        // filters by; empty for the other kinds. Like _labels, a view into the
        // op's interned StringAttr storage, which the MLIRContext keeps alive for
        // the whole translation; resolved to an EdgeTypeID when the loop is
        // translated.
        llvm::StringRef _edgeType;

        // The node IDs a ConstScanNodes iterator emits; empty for the other kinds.
        // A view into the op's DenseI64ArrayAttr storage, which the MLIRContext
        // keeps alive for the whole translation; resolved to owned NodeIDs when the
        // loop is translated.
        llvm::ArrayRef<int64_t> _nodeIDs;

        // The literal list an UnwindConst iterator emits; a null (empty) ListView
        // for the other kinds. Materialized by materializeListView into the
        // query-scoped ListBuffer at config setup.
        ListView _list;

        // What a VectorSearch iterator searches for: the index, how many neighbours to
        // report, and the query vector. Empty and zero for the other kinds. Like _labels
        // and _nodeIDs, views into the op's interned attribute storage, which the
        // MLIRContext keeps alive for the whole execution.
        llvm::StringRef _indexName;
        size_t _neighbourCount {0};
        llvm::ArrayRef<float> _queryVector;
    };

    NLProgram* _program {nullptr};
    LocalMemory* _memory {nullptr};
    const GraphView* _view {nullptr};
    MetadataBuilder* _metadataBuilder {nullptr};

    // The execution context a procedure reads the graph and the request through,
    // borrowed from the caller: it also carries the registry an nl.procedure's name
    // is resolved against. Null when the caller runs without procedures, which only
    // a function containing an nl.procedure notices.
    const ProcedureContext* _procedureContext {nullptr};
    llvm::DenseMap<mlir::Value, Column*> _valueSlots;

    std::unique_ptr<NLSystemTranslator> _systemTranslator;

    llvm::DenseMap<mlir::Value, IteratorConfig> _iteratorConfigs;

    // Set of nl.create_node result SSA values
    llvm::DenseSet<mlir::Value> _pendingNodeValues;

    // Set of nl.create_edge result SSA values
    llvm::DenseSet<mlir::Value> _pendingEdgeValues;

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

    // nl.aggregate handle SSA value -> the runtime accumulator it produces, so the
    // nl.aggregate_update and nl.aggregate_result that name the handle find the same
    // accumulator
    llvm::DenseMap<mlir::Value, NLAggregateState*> _aggregateStates;

    // nl.group_aggregate_buffer handle SSA value -> the runtime accumulator it
    // produces, so nl.group_aggregate_update and the nl.for over nl.group_aggregate
    // find the same group table and per-group state
    llvm::DenseMap<mlir::Value, NLGroupAggregateState*> _groupAggregateStates;

    // nl.collect_buffer handle SSA value -> the runtime accumulator it produces, so
    // nl.collect_update (and later the drain) that name the handle find the same group
    // table and per-group lists
    llvm::DenseMap<mlir::Value, NLCollectState*> _collectStates;

    // nl.procedure handle SSA value -> the runtime call it produces, so every op that
    // names the handle - the nl.for over nl.procedure_init - drives the same procedure
    llvm::DenseMap<mlir::Value, NLProcedureState*> _procedureStates;

    void translateBlock(mlir::Block& block, NLStmtContainer* body);
    void translateFor(mlir::nl::For forLoop, NLStmtContainer* body);
    void translateScanLoop(mlir::Block& loopBody, NLLimitState* limit, NLStmtContainer* body);

    // Translate the nl.for over an nl.scan_nodes_by_label iterator: resolve the
    // config's label names against the schema into a LabelSet (marking the scan
    // unmatchable if any name is absent), allocate the node loop variable, and
    // record the label-filtered scan loop statement in body
    void translateScanByLabelLoop(const IteratorConfig& config,
                                  mlir::Block& loopBody,
                                  NLLimitState* limit,
                                  NLStmtContainer* body);

    // Translate the nl.for over an nl.const_scan_nodes iterator: allocate the node
    // loop variable, resolve the config's constant i64 list into the owned NodeIDs
    // the loop emits, and record the const-scan loop statement in body. The fixed
    // sibling of translateScanLoop - a source loop that emits a known set of node
    // IDs rather than walking the graph.
    void translateConstScanLoop(const IteratorConfig& config,
                                mlir::Block& loopBody,
                                NLLimitState* limit,
                                NLStmtContainer* body);

    // Translate the nl.for over an nl.unwind_const iterator: allocate the single
    // value loop variable (a nullable value column for a homogeneous list, a
    // ColumnVector<ListElementView> for a heterogeneous one) and record the
    // unwind-const loop statement in body. The literal sibling of
    // translateConstScanLoop - a source loop that streams a fixed ListView one chunk
    // at a time rather than a set of node IDs.
    void translateUnwindConstLoop(const IteratorConfig& config,
                                  mlir::Block& loopBody,
                                  NLLimitState* limit,
                                  NLStmtContainer* body);

    // Translate the nl.for over an nl.vector_search iterator: allocate the two nullable
    // value loop variables (the neighbour IDs and the distances they scored) and record
    // the vector-search loop statement in body. The neighbour sibling of
    // translateUnwindConstLoop - a source loop whose rows come from a vector index
    // rather than from a fixed list.
    void translateVectorSearchLoop(const IteratorConfig& config,
                                   mlir::Block& loopBody,
                                   NLLimitState* limit,
                                   NLStmtContainer* body);

    // Materialize a literal element array - an nl.unwind_const's or an nl.const_list's -
    // into a ListView in the query-scoped ListBuffer, which the unwind loop then reads
    // chunk by chunk and a constant list keeps whole. A nested array is materialized
    // first and kept as one element of the list holding it. A string element stores the
    // string_view, not the characters, so the module the literals came from must outlive
    // execution - not just this call.
    ListView materializeListView(mlir::ArrayAttr elements);

    // The bytes the elements' values occupy, which sizes the region materializeListView
    // reserves before writing them
    static size_t listValueBytes(mlir::ArrayAttr elements);

    // Translate the nl.for over an nl.scan_edges iterator: allocate the four
    // fixed edge loop variables (sources, edge IDs, edge type IDs, targets) and
    // record the edge-scan loop statement in body. The edge sibling of
    // translateScanLoop - a source loop with no input chunk and no carry set.
    void translateScanEdgesLoop(mlir::Block& loopBody, NLLimitState* limit, NLStmtContainer* body);

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
    // order, and record the emit-loop statement (sort once, then re-chunk). limit is
    // the counter the drain early-exits on, or null for an unbounded drain.
    void translateSortLoop(const IteratorConfig& config,
                           mlir::Block& loopBody,
                           NLLimitState* limit,
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

    // Translate an nl.aggregate: allocate its runtime accumulator (a single-row
    // nullable value column of the state handle's element type), map the handle to
    // it, and record the reset statement (run each time the block runs)
    void translateAggregateState(mlir::nl::Aggregate aggregate, NLStmtContainer* body);

    // Translate an nl.aggregate_update: look up the accumulator the handle names and
    // record the fold of the chunk's non-null values into it (selected from the
    // reduction and the input value type)
    void translateAggregateUpdate(mlir::nl::AggregateUpdate update, NLStmtContainer* body);

    // Translate an nl.aggregate_result: look up the accumulator the handle names,
    // allocate the single-row nullable value chunk it produces, map the op result to
    // it, and record the emit statement (materialize the reduced value)
    void translateAggregateResult(mlir::nl::AggregateResult result, NLStmtContainer* body);

    // The runtime accumulator an aggregate handle names. The handle is a required
    // operand of nl.aggregate_update and nl.aggregate_result, so this throws if it
    // was not produced by an nl.aggregate.
    NLAggregateState* aggregateStateFor(mlir::Value handle) const;

    // Translate an nl.group_aggregate_buffer: allocate its runtime accumulator, map
    // the handle to it, and record the reset statement (run each time the block
    // runs). The key buffers and per-aggregate state are allocated by the update,
    // which knows their types, the same way nl.sort_collect allocates the sort
    // buffers.
    void translateGroupAggregateBuffer(mlir::nl::GroupAggregateBuffer buffer, NLStmtContainer* body);

    // Translate an nl.group_aggregate_update: look up the accumulator the handle
    // names, split the collected columns into grouping keys and aggregate inputs
    // (by the keyCount / kinds on the producing nl.group_aggregate_buffer), allocate
    // each key buffer and each aggregate's per-group state with its grow/fold/emit
    // handlers, and record the per-step fold statement.
    // The per-group accumulator one aggregate kind needs over a column: the grow, fold
    // and emit handlers baked from the kind and the input's value type. Shared by the
    // grouped aggregation and the collect that reduces beside its list.
    void buildGroupAggregate(mlir::storage::GroupAggregateKind mlirKind,
                             mlir::Value column,
                             NLGroupAggregateState::Aggregate& aggregate);

    void translateGroupAggregateUpdate(mlir::nl::GroupAggregateUpdate update, NLStmtContainer* body);

    // Translate the nl.for over an nl.group_aggregate iterator: allocate one loop
    // variable per output column (a grouping key or an aggregate result), wire it as
    // that column's emit output, and record the emit-loop statement (re-chunk the
    // groups). limit is the counter the drain early-exits on, or null for an
    // unbounded drain.
    void translateGroupAggregateLoop(const IteratorConfig& config,
                                     mlir::Block& loopBody,
                                     NLLimitState* limit,
                                     NLStmtContainer* body);

    // The runtime accumulator a group-aggregate handle names. Throws if the handle
    // was not produced by an nl.group_aggregate_buffer translated earlier.
    NLGroupAggregateState* groupAggregateStateFor(mlir::Value handle) const;

    // Translate an nl.collect_buffer: allocate its runtime accumulator, map the handle
    // to it, and record the reset statement (run each time the block runs). The key
    // buffers and the value buffer are allocated by the update, which knows their
    // types, the same way nl.group_aggregate_update allocates the group state.
    void translateCollectBuffer(mlir::nl::CollectBuffer buffer, NLStmtContainer* body);

    // Translate an nl.collect_update: look up the accumulator the handle names, split
    // the collected columns into grouping keys and the single value column (by the
    // keyCount on the producing nl.collect_buffer), allocate each key buffer and the
    // flat value buffer with its fold handler, and record the per-step append
    // statement.
    void translateCollectUpdate(mlir::nl::CollectUpdate update, NLStmtContainer* body);

    // The runtime accumulator a collect handle names. Throws if the handle was not
    // produced by an nl.collect_buffer translated earlier.
    NLCollectState* collectStateFor(mlir::Value handle) const;

    // Translate the nl.for over an nl.unwind_collect iterator: allocate one loop variable per
    // grouping key plus the element value, wire the key outputs and value output onto
    // the shared state, and record the per-element emit-loop statement.
    void translateUnwindCollectLoop(const IteratorConfig& config,
                             mlir::Block& loopBody,
                             NLStmtContainer* body);

    // Translate the nl.for over an nl.collect iterator: allocate one loop variable per
    // grouping key plus the list cell, wire the outputs onto the shared state, and
    // record the per-group emit-loop statement.
    void translateCollectLoop(const IteratorConfig& config,
                              mlir::Block& loopBody,
                              NLStmtContainer* body);

    // Pool-allocate the flat value buffer for a collected value type: a plain
    // ColumnVector<Primitive> (not nullable - collect drops nulls) that grows as
    // present values are appended across steps.
    Column* allocValueColumnForValueType(ValueType valueType);

    void translateProcedure(mlir::nl::Procedure procedureOp, NLStmtContainer* body);

    // Bind the argument chunks of a call as the procedure's input columns, one per
    // declared argument in declaration order. The chunks are loop variables refilled in
    // place, so binding them once holds for every step.
    void bindProcedureInputs(NLProcedureState* state, mlir::ValueRange inputs);

    void addProcedureCarriedColumns(const IteratorConfig& config,
                                    mlir::Block& loopBody,
                                    size_t yieldCount,
                                    NLProcedureLoopData* loopData);

    // Translate the nl.for over an nl.procedure_init iterator: bind one loop variable
    // per yielded return value as the procedure's result columns, and record the
    // drive-loop statement (run the procedure once per step until it finishes).
    void translateProcedureInitLoop(const IteratorConfig& config,
                                    mlir::Block& loopBody,
                                    NLLimitState* limit,
                                    NLStmtContainer* body);

    // The runtime call a procedure handle names. The handle is a required operand of
    // its consumers, so this throws if it was not produced by an nl.procedure.
    NLProcedureState* procedureStateFor(mlir::Value handle) const;

    // Allocate one result column per yielded return value of the call, bind it to
    // that return value's slot in the procedure's data - so the procedure writes
    // where the engine reads - and map the matching chunk value to it. The chunks are
    // an op's results, or a drive loop's variables; either way there is one per
    // yielded name, in yield order.
    void bindProcedureResults(NLProcedureState* state, mlir::ValueRange chunks);

    // Pool-allocate a result column for one of a procedure's declared return types -
    // an ID column, a value column or a list column - reserving a full chunk so
    // execution stays allocation-free. This is what fixes the column type a procedure
    // writes through, so it mirrors the pipeline engine's allocReturnValues exactly.
    Column* allocColumnForProcedureType(ProcedureType procedureType);

    // Allocate an emit output column for a group-aggregate output chunk type: an ID
    // column for an ID chunk (a grouping key), a nullable value column for a
    // !storage.nullable<...> chunk (a key or a sum/min/max/avg result), or a
    // ColumnVector<uint64_t> for a ui64 chunk (a count result).
    Column* allocColumnForResultChunkType(mlir::Type chunkType);

    // The key gather-append / range emit-copy for a group-aggregate key column of
    // this chunk type - by chunk kind for an ID chunk, by value type for a nullable
    // value chunk. Used to grow the key buffers and to slice them at emit.
    static NLGroupKeyGatherFunction selectGroupKeyGatherForChunkType(mlir::Type chunkType);
    static NLCopyFunction selectCopyForChunkType(mlir::Type chunkType);

    // Pool-allocate a buffer/loop column matching a chunk type - an ID column for
    // an ID chunk, a nullable value column for a !storage.nullable<...> chunk, a
    // ColumnVector<uint64_t> for a count chunk - and the append/gather/compare handler
    // for that element type. The compare selector throws for chunk types that have no
    // order (an embedding key).
    Column* allocColumnForChunkType(mlir::Type chunkType);
    static NLAppendFunction selectAppendForChunkType(mlir::Type chunkType);
    static NLGatherFunction selectGatherForChunkType(mlir::Type chunkType);
    static NLCompareFunction selectCompareForChunkType(mlir::Type chunkType);
    static NLKeyAppendFunction selectKeyAppendForChunkType(mlir::Type chunkType);

    // The non-null row count handler for a chunk type - the all-rows count for an
    // ID chunk, the present-value count for a !storage.nullable<...> chunk. Used by
    // nl.count_update.
    static NLCountFunction selectCountForChunkType(mlir::Type chunkType);

    // The fold handler for a value reduction over a chunk type. Used by
    // nl.aggregate_update.
    static NLAggregateUpdateFunction selectAggregateUpdateForChunkType(AggregateKind kind,
                                                                      mlir::Type chunkType);

    // Translate an nl.get_node_properties / nl.get_edge_properties: resolve the
    // property name (carried by the nl.get_property_type that produced the
    // handle) to a PropertyTypeID and value type, allocate the nullable value
    // column, and record the with-null fetch statement in body
    void translatePropertyFetch(mlir::Value inputValue,
                                mlir::Value propertyTypeValue,
                                mlir::Value resultValue,
                                bool isNode,
                                NLStmtContainer* body);

    void translateGetNodeLabelSet(mlir::nl::GetNodeLabelSet op, NLStmtContainer* body);

    void translateCheckLabelConstraint(mlir::nl::CheckLabelConstraint op, NLStmtContainer* body);
    void translateCheckEdgeTypeConstraint(mlir::nl::CheckEdgeTypeConstraint op, NLStmtContainer* body);

    void translateCreateNode(mlir::nl::CreateNode createNode, NLStmtContainer* body);

    void translateCreateEdge(mlir::nl::CreateEdge createEdge, NLStmtContainer* body);

    void translateSetNodeProperty(mlir::nl::SetNodeProperty setNodeProperty, NLStmtContainer* body);

    void translateSetEdgeProperty(mlir::nl::SetEdgeProperty setEdgeProperty, NLStmtContainer* body);

    void translateDeleteNode(mlir::nl::DeleteNode deleteNode, NLStmtContainer* body);

    void translateDeleteEdge(mlir::nl::DeleteEdge deleteEdge, NLStmtContainer* body);

    // Allocates singleton column for the constant and assigns MLIR value
    void translateConstant(mlir::nl::Constant constant);

    // Allocates the row-aligned column a constant is laid out into, and binds the
    // fill that writes the driving relation's row count of its value each step
    void translateBroadcastConstant(mlir::nl::BroadcastConstant broadcast, NLStmtContainer* body);
    // The list sibling of translateConstant: materializes the literals into the query's
    // ListBuffer and allocates a singleton column holding a view of them

    template <ColumnOperator Op, typename OpType>
    void translateBinaryOp(OpType op, NLStmtContainer* body);

    void translateNot(mlir::nl::Not notOp, NLStmtContainer* body);
    void translateToNullable(mlir::nl::ToNullable toNullable, NLStmtContainer* body);

    void translateUnaryFunction(mlir::Operation* op, NLStmtContainer* body);

    void translateBinaryFunction(mlir::Operation* op, NLStmtContainer* body);

    void translateFilter(mlir::nl::Filter filter, NLStmtContainer* body);

    void translateOutput(mlir::nl::Output output, NLStmtContainer* body);

    // Whether a step emitting from this block keeps a single row: at function scope the
    // one emission the function makes, and in a loop body only the drain of a keyless
    // accumulator, which carries the single group its reset created.
    bool stepKeepsASingleRow(mlir::Block* block) const;

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
    Column* allocColumnIfUsed(mlir::Value chunkValue);
    Column* allocColumnForKind(NLChunkKind kind);

    // Allocate a nullable value column for the value type's primitive. The
    // per-step variant reserves a full chunk so the loop body stays
    // allocation-free; the single-row variant reserves one element for an
    // accumulator that never grows past its one row.
    Column* allocOptColumnForValueType(ValueType valueType);
    Column* allocSingleRowOptColumnForValueType(ValueType valueType);
    Column* allocOptColumn(ValueType valueType, size_t reserveSize);

    // A count result is a ui64 tally, the pipeline's one non-nullable value chunk, so
    // it is neither an ID chunk nor a !storage.nullable<...> one and takes a plain
    // ColumnVector<uint64_t>.
    static bool isPlainValueElementType(mlir::Type elementType);
    Column* allocPlainColumn(ValueType valueType);
    ColumnVector<uint64_t>* allocCountColumn();

    // Allocate a type-erased column of tagged scalars - the shape a heterogeneous
    // unwind emits and a cross product broadcasts - reserving a full chunk.
    // A column of list cells, each a view over the query's list buffer
    Column* allocListColumn();

    Column* allocListElementColumn();

    Column* getColumn(mlir::Value chunkValue) const;
    static NLChunkKind getChunkKind(mlir::Type chunkType);
    static NLChunkKind chunkKindFromElementType(mlir::Type elementType);
};

}
