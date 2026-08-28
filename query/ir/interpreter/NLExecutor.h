#pragma once

#include <stddef.h>

#include "columns/ColumnOperator.h"
#include "metadata/PropertyType.h"

#include "NLProgram.h"

namespace vec {
class VectorDatabase;
}

namespace db {

class GraphView;
class NLOutputSink;
class LocalMemory;
class CommitWriteBuffer;
class NLSystemContext;
class NLVectorSearchLoopData;

class NLExecutionContext {
public:
    NLExecutionContext(const GraphView* view,
                       NLOutputSink* sink,
                       size_t chunkSize,
                       CommitWriteBuffer* writeBuffer = nullptr,
                       const NLSystemContext* system = nullptr)
        : _view(view),
        _sink(sink),
        _chunkSize(chunkSize),
        _writeBuffer(writeBuffer),
        _system(system)
    {
    }

    const GraphView* getView() const { return _view; }
    NLOutputSink* getSink() const { return _sink; }
    size_t getChunkSize() const { return _chunkSize; }
    CommitWriteBuffer* getWriteBuffer() const { return _writeBuffer; }

    // The server-level facilities only the system commands reach for; null for a
    // program with none, which is every ordinary query
    const NLSystemContext* getSystem() const { return _system; }

    // The vector indexes a search reads, borrowed from the session's accessor. They are
    // the one store outside the graph an ordinary query reaches, so - unlike the rest of
    // the system context - a dataflow loop reads them; null for a session that opened no
    // accessor, which the search reports as a user-facing error.
    vec::VectorDatabase* getVectorDatabase() const;

private:
    const GraphView* _view {nullptr};
    NLOutputSink* _sink {nullptr};
    size_t _chunkSize {0};
    CommitWriteBuffer* _writeBuffer {nullptr};
    const NLSystemContext* _system {nullptr};
};

// Executes a translated NLProgram against a graph view
class NLExecutor {
public:
    NLExecutor(const GraphView* view,
               const NLProgram* prog,
               NLOutputSink* sink,
               CommitWriteBuffer* writeBuffer = nullptr,
               const NLSystemContext* system = nullptr);
    ~NLExecutor();

    void run();

    static void runScanNodesLoop(NLExecutionContext* context, NLFunctionData* data);
    static void runScanNodesByLabelLoop(NLExecutionContext* context, NLFunctionData* data);

    // The fixed sibling of runScanNodesLoop: emit the loop data's constant node ID
    // list one chunk at a time - no graph walk - running the body over each slice.
    static void runConstScanNodesLoop(NLExecutionContext* context, NLFunctionData* data);

    // The literal-list sibling of runConstScanNodesLoop: stream the loop data's
    // ListView one chunk at a time into its value column - a nullable value column
    // for a homogeneous list, a ColumnVector<ListElementView> for a heterogeneous one -
    // running the body over each slice. A null limit leaves it unbounded.
    static void runUnwindConstLoop(NLExecutionContext* context, NLFunctionData* data);

    // The neighbour sibling of runUnwindConstLoop: search the named vector index for the
    // loop data's query vector, then stream the neighbours it found one chunk at a time
    // into the ID and score columns, running the body over each slice. A null limit
    // leaves it unbounded.
    static void runVectorSearchLoop(NLExecutionContext* context, NLFunctionData* data);

    static void runScanEdgesLoop(NLExecutionContext* context, NLFunctionData* data);
    static void runGetOutEdgesLoop(NLExecutionContext* context, NLFunctionData* data);
    static void runGetInEdgesLoop(NLExecutionContext* context, NLFunctionData* data);

    static void runGetEdgesLoop(NLExecutionContext* context, NLFunctionData* data);

    // The by-type edge hops: like runGetOutEdgesLoop / runGetInEdgesLoop, but the
    // chunk writer keeps only the edges of the loop data's resolved edge type. An
    // unmatchable type (a name absent from the schema) emits nothing.
    static void runGetOutEdgesByTypeLoop(NLExecutionContext* context, NLFunctionData* data);
    static void runGetInEdgesByTypeLoop(NLExecutionContext* context, NLFunctionData* data);

    static void runGetNodeLabelSet(NLExecutionContext* context, NLFunctionData* data);

    // Fills a boolean mask
    static void runCheckLabelConstraint(NLExecutionContext* context, NLFunctionData* data);
    static void runCheckEdgeTypeConstraint(NLExecutionContext* context, NLFunctionData* data);

    static void runCrossProduct(NLExecutionContext* context, NLFunctionData* data);

    // Reset a limit counter to its budget; runs each time its block runs.
    static void runLimitInit(NLExecutionContext* context, NLFunctionData* data);

    // Charge the representative chunk's rows against a limit counter, recording
    // how many rows the truncate should copy this step. The sole counter mutator.
    static void runLimitUpdate(NLExecutionContext* context, NLFunctionData* data);

    // Copy the first emitThisStep rows of each column into a fresh chunk, so a
    // downstream consumer reads a genuinely truncated chunk. Reads the counter,
    // never mutates it.
    static void runLimitTruncate(NLExecutionContext* context, NLFunctionData* data);

    // Reset a skip counter to its rows-to-drop; runs each time its block runs.
    static void runSkipInit(NLExecutionContext* context, NLFunctionData* data);

    // Charge the representative chunk's rows against a skip counter, recording how
    // many rows the truncate should drop off the front this step and how many
    // survive. The sole skip-counter mutator.
    static void runSkipUpdate(NLExecutionContext* context, NLFunctionData* data);

    // Copy the surviving suffix of each column into a fresh, front-aligned chunk,
    // so a downstream consumer reads a genuinely skipped chunk. Reads the counter,
    // never mutates it.
    static void runSkipTruncate(NLExecutionContext* context, NLFunctionData* data);

    // Empty the buffers of a sort accumulator; runs each time its block runs.
    static void runSortReset(NLExecutionContext* context, NLFunctionData* data);

    // Append the current chunk of every column to its sort buffer. Runs once per
    // producing-loop step, growing the buffers row-aligned.
    static void runSortCollect(NLExecutionContext* context, NLFunctionData* data);

    // The emit phase of an ORDER BY: sort the accumulator once, then re-chunk the
    // sorted rows - gathering each chunk-sized permutation slice into the loop
    // variables and running the body (the nl.output) per chunk.
    static void runSortLoop(NLExecutionContext* context, NLFunctionData* data);

    // Empty the seen-set of a DISTINCT; runs each time its block runs.
    static void runDistinctReset(NLExecutionContext* context, NLFunctionData* data);

    // Keep only the rows of this step's chunk not seen before: serialize each row
    // across all columns into a key, insert the new keys into the seen-set, and
    // gather the surviving rows into fresh output chunks. The sole seen-set mutator.
    static void runDistinctFilter(NLExecutionContext* context, NLFunctionData* data);

    // Apply a mask to the entire carry set
    static void runFilter(NLExecutionContext* context, NLFunctionData* data);

    // Zero the tally of a COUNT; runs each time its block runs.
    static void runCountReset(NLExecutionContext* context, NLFunctionData* data);

    // Add this step's chunk's non-null row count to the tally. The sole tally
    // mutator.
    static void runCountUpdate(NLExecutionContext* context, NLFunctionData* data);

    // The emit step of a COUNT: materialize the final tally as the output chunk's
    // single unsigned i64 row. Runs once, after the producing loop; nl.output emits
    // the chunk at function scope.
    static void runCountResult(NLExecutionContext* context, NLFunctionData* data);

    // Re-initialize an aggregate accumulator; runs each time its block runs.
    static void runAggregateReset(NLExecutionContext* context, NLFunctionData* data);

    // Fold this step's chunk's non-null values into the accumulator. The sole
    // accumulator mutator.
    static void runAggregateUpdate(NLExecutionContext* context, NLFunctionData* data);

    // The emit step of an aggregate: materialize the reduced value as the output
    // chunk's single nullable value row. Runs once, after the producing loop;
    // nl.output emits the chunk at function scope.
    static void runAggregateResult(NLExecutionContext* context, NLFunctionData* data);

    // Empty a grouped accumulator's group table, key buffers and per-group state;
    // runs each time its block runs.
    static void runGroupAggregateReset(NLExecutionContext* context, NLFunctionData* data);

    // Assign this step's rows to their groups (creating groups on first sight) and
    // fold each aggregate input into the per-group state. The sole mutator of the
    // group table.
    static void runGroupAggregateUpdate(NLExecutionContext* context, NLFunctionData* data);

    // The emit phase of a grouped aggregation: re-chunk the accumulated groups -
    // each step materializes a chunk of group rows (the key values sliced from the
    // buffers, the aggregates finalized from the per-group state) into the loop
    // variables and runs the body (the nl.output) per chunk.
    static void runGroupAggregateLoop(NLExecutionContext* context, NLFunctionData* data);

    // Empty a collect accumulator's group table, key buffers, value buffer and
    // per-group positions; runs each time its block runs.
    static void runCollectReset(NLExecutionContext* context, NLFunctionData* data);

    // Assign this step's rows to their groups (creating groups on first sight) and
    // append each present value to its group's list. The sole mutator of a collect
    // accumulator.
    static void runCollectUpdate(NLExecutionContext* context, NLFunctionData* data);

    // The per-element drain of a collect: walk every (group, element) pair in group
    // order, emitting one row per element (the group's key values repeated, then the
    // element value) chunk by chunk and running the body over each.
    static void runUnwindCollectLoop(NLExecutionContext* context, NLFunctionData* data);

    // The per-group drain of a collect: walk the groups, emitting one row per group
    // (the key values sliced, then a list cell spanning the group's elements) chunk by
    // chunk and running the body over each.
    static void runCollectLoop(NLExecutionContext* context, NLFunctionData* data);

    static void runCreateNode(NLExecutionContext* context, NLFunctionData* data);

    static void runCreateEdge(NLExecutionContext* context, NLFunctionData* data);

    static void runSetNodeProperty(NLExecutionContext* context, NLFunctionData* data);

    static void runSetEdgeProperty(NLExecutionContext* context, NLFunctionData* data);

    static void runDeleteNode(NLExecutionContext* context, NLFunctionData* data);

    static void runDeleteEdge(NLExecutionContext* context, NLFunctionData* data);

    // The drive loop of a row-producing procedure: rewind it, then run it once per step
    // - each call refilling the loop variables in place - rebuild any carried column,
    // and run the body over every step that produced rows, until the procedure declares
    // itself finished. One entry covers one chunk of arguments, however many chunks of
    // rows the procedure answers it with.
    static void runProcedureInitLoop(NLExecutionContext* context, NLFunctionData* data);

    static void runOutput(NLExecutionContext* context, NLFunctionData* data);

    // Run a row-wise binary op (nl.add): invoke the typed kernel bound at
    // translation, which fills the pre-allocated result chunk from the two operands.
    static void runBinary(NLExecutionContext* context, NLFunctionData* data);

    // Get the binary function pointer to execute this op
    template <ColumnOperator Op>
    static NLBinaryFn selectBinary(const Column* lhs, const Column* rhs,
                                   LocalMemory* memory, Column*& result);

    static void runUnary(NLExecutionContext* context, NLFunctionData* data);

    static NLUnaryFn selectNot(const Column* operand, LocalMemory* memory, Column*& result);
    static NLUnaryFn selectToNullable(ValueType valueType, const Column* operand, LocalMemory* memory, Column*& result);

    // Lay a constant chunk's single value out over the driving relation's rows
    // (nl.broadcast_constant), so a fold that walks rows is handed the step's rows
    // rather than the one row a constant column is.
    static void runBroadcastConstant(NLExecutionContext* context, NLFunctionData* data);

    static void runLabels(NLExecutionContext* context, NLFunctionData* data);
    static void runEdgeTypes(NLExecutionContext* context, NLFunctionData* data);

    static void runUnaryFunction(NLExecutionContext* context, NLFunctionData* data);

    template <typename Functor>
    static NLUnaryFunctionKernel selectFunction(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);

    static NLGatherFunction selectGatherFunction(NLChunkKind kind);

    // Gather for a nullable value chunk of this value type (sort emit re-chunk).
    static NLGatherFunction selectOptGatherFunction(ValueType valueType);

    // Gather for the count result chunk: one non-nullable uint64 tally per row.
    static NLGatherFunction selectCountGatherFunction();

    // Gather for a collected list chunk: one ListView per row, each spanning its
    // group's run in the collect accumulator's list buffer, which outlives the sort.
    static NLGatherFunction selectListGatherFunction();

    // The mask survivor collector for an nl.filter, chosen by the mask chunk's
    // nullability: a nullable mask drops null rows as well as false ones.
    static NLMaskSurvivorFunction selectMaskSurvivorFunction(bool nullable);

    // Append (onto a buffer tail) for an ID chunk of this kind / a nullable value
    // chunk of this value type. Used by nl.sort_collect.
    static NLAppendFunction selectAppendFunction(NLChunkKind kind);
    static NLAppendFunction selectOptAppendFunction(ValueType valueType);
    static NLAppendFunction selectCountAppendFunction();
    static NLAppendFunction selectListAppendFunction();

    // The 3-way row comparator for an ID key column of this kind / a nullable
    // value key column of this value type. The value-type selector throws for a
    // value type with no order (an embedding), which cannot be a sort key.
    static NLCompareFunction selectCompareFunction(NLChunkKind kind);
    static NLCompareFunction selectOptCompareFunction(ValueType valueType);

    // The 3-way row comparator for a collected list chunk, ordering two lists
    // lexicographically over their elements.
    static NLCompareFunction selectListCompareFunction();

    // The handlers of a plain value column - a ColumnVector<Primitive> rather than the
    // nullable ColumnOptVector a property fetch yields. A tally comes out this way, and so
    // does an expression over one: both are present in every row. Numeric only, since
    // these are the shapes an aggregate and the arithmetic over it produce.
    static NLAppendFunction selectPlainAppendFunction(ValueType valueType);
    static NLGatherFunction selectPlainGatherFunction(ValueType valueType);
    static NLCompareFunction selectPlainCompareFunction(ValueType valueType);
    static NLCopyFunction selectPlainCopyFunction(ValueType valueType);
    static NLBroadcastFunction selectPlainBlockRepeatFunction(ValueType valueType);
    static NLBroadcastFunction selectPlainTileFunction(ValueType valueType);
    static NLKeyAppendFunction selectPlainKeyAppendFunction(ValueType valueType);
    static NLGroupKeyGatherFunction selectPlainGroupKeyGather(ValueType valueType);

    // Block-repeat for an ID chunk of this kind (outer column).
    static NLBroadcastFunction selectBlockRepeatFunction(NLChunkKind kind);

    // Block-repeat for the count result chunk, which a limit truncates with factor 1.
    static NLBroadcastFunction selectCountBlockRepeatFunction();

    // Block-repeat for a constant chunk, which a limit over a projection of
    // constants alone cuts down to the one row it holds or to no row at all.
    static NLBroadcastFunction selectConstBlockRepeatFunction();

    // Tile for an ID chunk of this kind (inner column).
    static NLBroadcastFunction selectTileFunction(NLChunkKind kind);

    // Block-repeat for a nullable value chunk of this value type (outer column).
    static NLBroadcastFunction selectOptBlockRepeatFunction(ValueType valueType);

    // Tile for a nullable value chunk of this value type (inner column).
    static NLBroadcastFunction selectOptTileFunction(ValueType valueType);

    // The fill that lays a constant column's single value out over a step's rows,
    // for a nullable value chunk of this value type (nl.broadcast_constant).
    static NLBroadcastConstantFunction selectConstantBroadcast(ValueType valueType);

    // The broadcast of the null literal, whose rows are the absent value rather than
    // copies of a value the constant holds
    static NLBroadcastConstantFunction selectNullConstantBroadcast();

    // The list sibling: a list constant lays its one view out over the step's rows
    static NLBroadcastConstantFunction selectConstantListBroadcast();

    // Block-repeat (outer column) and tile (inner column) for a list_element chunk: a
    // tagged scalar carries its own type, so there is no value type to dispatch on.
    static NLBroadcastFunction selectListElementBlockRepeatFunction();
    static NLBroadcastFunction selectListElementTileFunction();

    // Sort-accumulator append, emit-phase gather and 3-way compare for a list_element
    // chunk, so a heterogeneous unwind can be sorted and its rows re-emitted.
    static NLAppendFunction selectListElementAppendFunction();
    static NLGatherFunction selectListElementGatherFunction();
    static NLCompareFunction selectListElementCompareFunction();

    // Distinct row key and non-null tally for a list_element chunk, so a heterogeneous
    // unwind can be deduped and counted.
    static NLKeyAppendFunction selectListElementKeyAppendFunction();
    static NLCountFunction selectListElementCountFunction();

    // Key-buffer gather-append and emit-phase range copy for a list_element chunk, so a
    // heterogeneous unwind's cells can be a grouping key rather than only a counted column.
    static NLGroupKeyGatherFunction selectListElementGroupKeyGatherFunction();
    static NLCopyFunction selectListElementCopyFunction();

    // The cut families for a list chunk: a list cell copies as a view, so a prefix or a
    // suffix of them is the plain range copy every other cell column uses
    static NLBroadcastFunction selectListBlockRepeatFunction();
    static NLCopyFunction selectListCopyFunction();

    // Range copy for an ID chunk of this kind (skip suffix copy).
    static NLCopyFunction selectCopyFunction(NLChunkKind kind);

    // Range copy for the count result chunk, which a skip lifts to a chunk front.
    static NLCopyFunction selectCountCopyFunction();

    // Range copy for a constant chunk, which a skip over a projection of constants
    // alone cuts down to the one row it holds or to no row at all.
    static NLCopyFunction selectConstCopyFunction();

    // Range copy for a nullable value chunk of this value type (skip suffix copy).
    static NLCopyFunction selectOptCopyFunction(ValueType valueType);

    // Row-key serialization for an ID chunk of this kind / a nullable value chunk
    // of this value type. Used by nl.distinct_filter to build each row's seen-set
    // key. The value-type selector throws for a value type with no byte identity
    // as a key (an embedding), which cannot be a DISTINCT key.
    static NLKeyAppendFunction selectKeyAppendFunction(NLChunkKind kind);
    static NLKeyAppendFunction selectOptKeyAppendFunction(ValueType valueType);

    // Non-null row count for a COUNT. An ID chunk has no null rows, so countAllRows
    // is its handle (the row count); a nullable value chunk of this value type
    // counts only its present values. Used by nl.count_update.
    static size_t countAllRows(const Column* column);
    static NLCountFunction selectOptCountFunction(ValueType valueType);

    // The reset / fold / emit handlers for one aggregate, selected from the
    // reduction and a value type (the accumulator's for reset/result, the input's
    // for update). Throw for a value type the reduction cannot handle: sum/avg need
    // a numeric type, min/max an orderable one. Used by the nl.aggregate ops.
    static NLAggregateResetFunction selectAggregateReset(AggregateKind kind, ValueType accumulatorType);
    static NLAggregateUpdateFunction selectAggregateUpdate(AggregateKind kind, ValueType inputType);

    // The siblings of selectAggregateUpdate / selectGroupAggregateFold for a type-erased
    // column, whose cells carry their own tags rather than sharing one value type
    static NLAggregateUpdateFunction selectListElementAggregateUpdate(AggregateKind kind);
    static NLGroupAggregateFoldFunction selectListElementGroupAggregateFold(GroupAggregateKind kind);
    static NLAggregateResultFunction selectAggregateResult(AggregateKind kind, ValueType resultType);

    // The grouped counterparts, selected for one aggregate of a grouped
    // aggregation. grow initializes a new group to the reduction's identity; fold
    // reduces a chunk's rows into their groups; emit materializes a slice of groups.
    // They throw for a value type the reduction cannot handle, exactly as the scalar
    // selectors do (sum/avg need numeric, min/max orderable). count and
    // count_distinct use only the per-group tally, so their grow/emit ignore the
    // value type. count's fold has an all-rows form (count(*) over an ID chunk) and a
    // present-values form (count(x) over a nullable value chunk); count_distinct's
    // has an ID form and a present-values form the same way, both keyed on the
    // value's bytes - so, unlike count, an embedding column is rejected.
    static NLGroupAggregateGrowFunction selectGroupAggregateGrow(GroupAggregateKind kind, ValueType accumulatorType);
    static NLGroupAggregateFoldFunction selectGroupAggregateFold(GroupAggregateKind kind, ValueType inputType);
    static NLGroupAggregateFoldFunction selectGroupCountAllFold();
    static NLGroupAggregateFoldFunction selectGroupCountDistinctFold(ValueType inputType);
    static NLGroupAggregateFoldFunction selectGroupCountDistinctChunkFold(NLChunkKind kind);

    // The grouped count / count(DISTINCT) folds of a type-erased column of tagged
    // scalars, the column a heterogeneous UNWIND produces
    static NLGroupAggregateFoldFunction selectGroupCountListElementFold();
    static NLGroupAggregateFoldFunction selectGroupCountDistinctListElementFold();
    static NLGroupAggregateEmitFunction selectGroupAggregateEmit(GroupAggregateKind kind, ValueType resultType);

    // The append (onto a key buffer's tail) for an ID chunk of this kind / a
    // nullable value chunk of this value type. Used by nl.group_aggregate_update to
    // grow the key buffers with each new group's key values.
    static NLGroupKeyGatherFunction selectGroupKeyGather(NLChunkKind kind);
    static NLGroupKeyGatherFunction selectOptGroupKeyGather(ValueType valueType);

    // The collect fold for a column of this value type: appends each present value to
    // its group's list in the flat value buffer. Only the scalar value types are
    // supported (collect of embeddings is unsupported for now).
    static NLCollectFoldFunction selectCollectFold(ValueType valueType);
    static NLCollectFoldFunction selectCollectDistinctFold(ValueType valueType);

    // The unwind value-emit / collect list-emit for a column of this value type: the
    // drain-side siblings of selectCollectFold, baked from the same value type.
    static NLUnwindCollectValueEmitFunction selectUnwindCollectValueEmit(ValueType valueType);
    static NLCollectListEmitFunction selectCollectListEmit(ValueType valueType);

    // The fold and list-emit for an entity chunk of this kind, whose elements carry a
    // node or edge ID. An edge-type ID is no entity, so the kind is rejected.
    static void selectCollectEntityHandlers(NLChunkKind kind,
                                            bool distinctValues,
                                            NLCollectFoldFunction& fold,
                                            NLCollectListEmitFunction& listEmit);

    // The with-null property fetch handler for an ID type (NodeID/EdgeID) and a
    // value type (types::Double, ...). The translator picks the specialization
    // from the resolved property and stores it as the statement's handler; only
    // the explicitly instantiated (ID, T) pairs in NLExecutor.cpp are available.
    template <typename ID, typename T>
    static void runPropertyFetch(NLExecutionContext* context, NLFunctionData* data);

private:
    NLExecutionContext _ctxt;
    const NLProgram* _prog {nullptr};

    // Search the index the loop names and keep its neighbours on the loop data, in the
    // types the two chunks carry. Holds the index' reader lock for the search alone.
    static void searchVectorIndex(NLExecutionContext* context, NLVectorSearchLoopData* loopData);
};

}
