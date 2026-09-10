#pragma once

#include <stddef.h>

#include "columns/ColumnOperator.h"
#include "metadata/PropertyType.h"

#include "NLExecutionContext.h"
#include "NLProgram.h"

namespace db {

class GraphView;
class NLOutputSink;
class LocalMemory;
class NLSystemContext;
class NLVectorSearchLoopData;

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

    // The property-equality sibling of runScanNodesByLabelLoop: emit the nodes whose
    // property holds the loop data's literal, scanning each part's property column in place.
    static void runScanNodesByPropertyValueLoop(NLExecutionContext* context, NLFunctionData* data);

    // The fixed sibling of runScanNodesLoop: emit the loop data's constant node ID
    // list one chunk at a time - no graph walk - running the body over each slice.
    static void runConstScanNodesLoop(NLExecutionContext* context, NLFunctionData* data);

    // The literal-list sibling of runConstScanNodesLoop: stream the loop data's
    // ListView one chunk at a time into its value column - a nullable value column
    // for a homogeneous list, a ColumnVector<ListElementView> for a heterogeneous one -
    // running the body over each slice. A null limit leaves it unbounded.
    static void runUnwindConstLoop(NLExecutionContext* context, NLFunctionData* data);

    // The file sibling of runUnwindConstLoop: resolve the loop data's path against the
    // data directory and its header names against the file's header line, then stream the
    // records one chunk at a time into the field columns, running the body over each
    // slice. A null limit leaves it unbounded; a bounded loop stops reading the file as
    // soon as the budget is spent.
    static void runLoadCSVLoop(NLExecutionContext* context, NLFunctionData* data);

    // The neighbour sibling of runUnwindConstLoop: search the named vector index for the
    // loop data's query vector, then stream the neighbours it found one chunk at a time
    // into the ID and score columns, running the body over each slice. A null limit
    // leaves it unbounded.
    static void runVectorSearchLoop(NLExecutionContext* context, NLFunctionData* data);

    // The per-row sibling of runUnwindConstLoop: expand the loop data's source column,
    // emitting one row per element of each of its cells - none for a null - one chunk at
    // a time, with the carry set gathered by the source row each emitted row came from.
    static void runUnwindLoop(NLExecutionContext* context, NLFunctionData* data);

    static void runScanEdgesLoop(NLExecutionContext* context, NLFunctionData* data);
    static void runScanEdgesByTypeLoop(NLExecutionContext* context, NLFunctionData* data);
    static void runGetOutEdgesLoop(NLExecutionContext* context, NLFunctionData* data);
    static void runGetInEdgesLoop(NLExecutionContext* context, NLFunctionData* data);

    static void runGetEdgesLoop(NLExecutionContext* context, NLFunctionData* data);

    // The by-type edge hops: like runGetOutEdgesLoop / runGetInEdgesLoop, but the
    // chunk writer keeps only the edges of the loop data's resolved edge type. An
    // unmatchable type (a name absent from the schema) emits nothing.
    static void runGetOutEdgesByTypeLoop(NLExecutionContext* context, NLFunctionData* data);
    static void runGetInEdgesByTypeLoop(NLExecutionContext* context, NLFunctionData* data);

    static void runGetNodeLabelSet(NLExecutionContext* context, NLFunctionData* data);
    static void runGetEdgeTypes(NLExecutionContext* context, NLFunctionData* data);

    // Fills a boolean mask
    static void runCheckLabelConstraint(NLExecutionContext* context, NLFunctionData* data);
    static void runCheckEdgeTypeConstraint(NLExecutionContext* context, NLFunctionData* data);

    // Drive the pairs of a cross product, running the body once per chunk of them.
    static void runCrossProductLoop(NLExecutionContext* context, NLFunctionData* data);

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

    // Empty the buffers and matched flags of an OPTIONAL MATCH accumulator and lay its row
    // tag out over this step's input rows; runs each time its block runs.
    static void runOptionalReset(NLExecutionContext* context, NLFunctionData* data);

    // Append this step's chunk of every column the pattern contributes to its buffer, and
    // mark as matched each input row the row tag names. The sole matched-flag mutator.
    static void runOptionalCollect(NLExecutionContext* context, NLFunctionData* data);

    // The emit phase of an OPTIONAL MATCH: re-chunk the collected rows, running the body
    // per chunk, then sweep the matched flags and emit one null-padded row per input row
    // the pattern missed.
    static void runOptionalDrainLoop(NLExecutionContext* context, NLFunctionData* data);

    // Empty the buffers and the key index of a hash join's build side; runs each time
    // its block runs.
    static void runHashJoinReset(NLExecutionContext* context, NLFunctionData* data);

    // Append the current chunk of every build column to its buffer and index each row
    // under its key. Runs once per build-loop step, growing the buffers row-aligned; a
    // row whose key is null is buffered but left out of the index.
    static void runHashJoinCollect(NLExecutionContext* context, NLFunctionData* data);

    // Emit each probe row paired with the build rows its key matches: look each probe
    // row's key up in the index, then gather the probe columns and the build buffers by
    // the matched pairs into fresh output chunks. A null probe key matches nothing.
    static void runHashJoinProbeLoop(NLExecutionContext* context, NLFunctionData* data);

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

    static void runShortestPathReset(NLExecutionContext* context, NLFunctionData* data);

    static void runShortestPathUpdate(NLExecutionContext* context, NLFunctionData* data);

    static void runShortestPathLoop(NLExecutionContext* context, NLFunctionData* data);

    static void runCreateNode(NLExecutionContext* context, NLFunctionData* data);

    static void runCreateEdge(NLExecutionContext* context, NLFunctionData* data);

    // Bind one MERGE pattern per row of the op's input chunks, writing the pattern for
    // the rows the graph and this query's earlier writes do not already hold it for.
    static void runMerge(NLExecutionContext* context, NLFunctionData* data);

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
    // Read a node or edge column as a nullable column of its IDs' integers, an invalid ID
    // - what an OPTIONAL MATCH leaves - reading as the null. The entity sibling of
    // selectToNullable, which reads a scalar value column.
    static NLUnaryFn selectEntityToNullable(NLChunkKind kind, LocalMemory* memory, Column*& result);

    static NLUnaryFn selectToNullable(ValueType valueType, const Column* operand, LocalMemory* memory, Column*& result);

    // Write each row of a CASE (nl.case): the value of the first branch whose condition
    // holds, the default when none does, and an absent value when there is no default.
    static void runCase(NLExecutionContext* context, NLFunctionData* data);

    // Build one list per row (nl.make_list): row r takes the cell each element column
    // holds at r, in operand order, as one contiguous run of the query's list buffer.
    static void runMakeList(NLExecutionContext* context, NLFunctionData* data);

    // Size the CASE result to the step's rows, all absent
    static NLCaseResetFn selectCaseReset(ValueType valueType);

    // Read one branch's condition column: a mask, a nullable mask - where a null row is
    // not a match - or the null literal, which matches no row at all
    static NLCaseTestFn selectCaseTest(const Column* condition, bool nullable, bool untypedNull);

    // Copy one branch's value column into the result, which lowering resolved to the one
    // nullable value column every branch promotes into. A branch of null writes nothing:
    // the reset already left the row absent.
    static NLCaseWriteFn selectCaseWrite(ValueType valueType,
                                         const Column* value,
                                         bool nullable,
                                         bool untypedNull);

    // The entity siblings of the two above, for a selection whose branches are nodes or
    // edges: an entity column carries its null in the ID rather than in an optional, so
    // the reset writes the invalid ID and the write copies the branch's ID across
    static NLCaseResetFn selectEntityCaseReset(NLChunkKind kind);

    static NLCaseWriteFn selectEntityCaseWrite(NLChunkKind kind, bool untypedNull);

    // Lay a constant chunk's single value out over the driving relation's rows
    // (nl.broadcast_constant), so a fold that walks rows is handed the step's rows
    // rather than the one row a constant column is.
    static void runBroadcastConstant(NLExecutionContext* context, NLFunctionData* data);

    static void runLabels(NLExecutionContext* context, NLFunctionData* data);
    static void runEdgeTypes(NLExecutionContext* context, NLFunctionData* data);

    static void runUnaryFunction(NLExecutionContext* context, NLFunctionData* data);

    template <typename Functor>
    static NLUnaryFunctionKernel selectFunction(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);

    // A conversion reads whichever element the argument column holds, so the functor is
    // picked from that column rather than fixed by the op: the string form named here is
    // what a column of strings converts through.
    template <typename StringFunctor>
    static NLUnaryFunctionKernel selectConversion(const Column* input, bool inputNullable, LocalMemory* memory, Column*& result);

    // The owned-string members of the nullable handler families, for the chunk kind
    // labels() and type() produce: a nullable value whose rows own their characters
    // rather than borrowing them, so the value type alone does not pick the handler.
    static NLGatherFunction selectOptOwnedStringGather();
    static NLAppendFunction selectOptOwnedStringAppend();
    static NLCopyFunction selectOptOwnedStringCopy();
    static NLGroupKeyGatherFunction selectOptOwnedStringGroupKeyGather();
    static NLCompareFunction selectOptOwnedStringCompare();
    static NLKeyAppendFunction selectOptOwnedStringKeyAppend();
    static NLCountFunction selectOptOwnedStringCount();
    static NLBroadcastFunction selectOptOwnedStringBlockRepeat();
    static NLBroadcastFunction selectOptOwnedStringTile();

    // The mask members of the handler families, for a !storage.bool chunk: a ColumnMask -
    // what a label test, an edge type test and a merge produce - where an i1 value chunk
    // is a ColumnVector<CustomBool>.
    static NLGatherFunction selectMaskGather();
    static NLAppendFunction selectMaskAppend();
    static NLCopyFunction selectMaskCopy();
    static NLGroupKeyGatherFunction selectMaskGroupKeyGather();
    static NLCompareFunction selectMaskCompare();
    static NLKeyAppendFunction selectMaskKeyAppend();
    static NLBroadcastFunction selectMaskBlockRepeat();
    static NLBroadcastFunction selectMaskTile();
    static NLKeyAppendFunction selectMaskMergeKeyAppend(ValueType keyType);
    static NLUnaryFn selectMaskToNullable(LocalMemory* memory, Column*& result);

    static NLGatherFunction selectGatherFunction(NLChunkKind kind);

    // The null fill for a chunk of this kind: an invalid ID for an ID chunk, which is how
    // an entity an OPTIONAL MATCH did not match is spelled.
    static NLFillNullFunction selectFillNullFunction(NLChunkKind kind);

    // Gather for a nullable value chunk of this value type (sort emit re-chunk).
    static NLGatherFunction selectOptGatherFunction(ValueType valueType);

    // Gather for the count result chunk: one non-nullable uint64 tally per row.
    static NLGatherFunction selectCountGatherFunction();

    // The mask survivor collector for an nl.filter, chosen by the mask chunk's shape: a
    // nullable mask drops null rows as well as false ones, and a mask that is null itself
    // keeps nothing.
    static NLMaskSurvivorFunction selectMaskSurvivorFunction(bool nullable, bool untypedNull);

    // Append (onto a buffer tail) for an ID chunk of this kind / a nullable value
    // chunk of this value type. Used by nl.sort_collect.
    static NLAppendFunction selectAppendFunction(NLChunkKind kind);
    static NLAppendFunction selectOptAppendFunction(ValueType valueType);
    static NLAppendFunction selectCountAppendFunction();

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

    // The appenders that key one row of a merge's property value column. A merge keys
    // the value a row asks for against the value the graph holds, and those arrive in a
    // plain (or constant) column and a nullable one respectively, so these write the
    // present-value tag byte selectOptKeyAppendFunction' appenders write.
    //
    // @param keyType is the type the schema holds the property as, which the row's own
    // value is keyed in: the analyzer lets an integer constrain a double-typed property,
    // and the graph side keys the double it reads back.
    static NLKeyAppendFunction selectPlainMergeKeyAppendFunction(NLChunkKind kind, ValueType keyType);
    static NLKeyAppendFunction selectConstMergeKeyAppendFunction(ValueType valueType, ValueType keyType);
    static NLKeyAppendFunction selectOptMergeKeyAppendFunction(ValueType valueType, ValueType keyType);
    static NLKeyAppendFunction selectOptOwnedStringMergeKeyAppend(ValueType keyType);
    static NLKeyAppendFunction selectNullMergeKeyAppendFunction();
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

    static NLAppendFunction selectOptListElementAppendFunction();
    static NLGatherFunction selectOptListElementGatherFunction();
    static NLCompareFunction selectListElementCompareFunction();

    // Distinct row key and non-null tally for a list_element chunk, so a heterogeneous
    // unwind can be deduped and counted.
    static NLKeyAppendFunction selectListElementKeyAppendFunction();
    static NLCountFunction selectListElementCountFunction();

    // Key-buffer gather-append and emit-phase range copy for a list_element chunk, so a
    // heterogeneous unwind's cells can be a grouping key rather than only a counted column.
    static NLGroupKeyGatherFunction selectListElementGroupKeyGatherFunction();
    static NLCopyFunction selectListElementCopyFunction();

    static NLBroadcastFunction selectOptListElementBlockRepeatFunction();
    static NLBroadcastFunction selectOptListElementTileFunction();
    static NLCompareFunction selectOptListElementCompareFunction();
    static NLKeyAppendFunction selectOptListElementKeyAppendFunction();
    static NLCountFunction selectOptListElementCountFunction();
    static NLGroupKeyGatherFunction selectOptListElementGroupKeyGatherFunction();
    static NLCopyFunction selectOptListElementCopyFunction();

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

    // Hash and equality for a join key: an ID chunk of this kind, a nullable value chunk
    // of this value type, a plain numeric chunk, a type-erased cell. Used by
    // nl.hash_join_collect to chain a build row under its hash and by nl.hash_join_probe
    // to find and confirm the build rows a probe row matches. An embedding is a key here,
    // as `=` compares two vectors, though it is no DISTINCT key.
    static NLJoinKeyFunctions selectJoinKeyFunctions(NLChunkKind kind);
    static NLJoinKeyFunctions selectOptJoinKeyFunctions(ValueType valueType);
    static NLJoinKeyFunctions selectPlainJoinKeyFunctions(ValueType valueType);
    static NLJoinKeyFunctions selectListElementJoinKeyFunctions();

    // Per-row test of whether a join key can match. An ID chunk holds neither a null nor
    // a NaN, so every row of it matches; a nullable value chunk reads its present flag, a
    // type-erased cell its tag, and a double or embedding column also rejects a NaN, which
    // no key equals - itself included. Used by nl.hash_join_collect to leave a key
    // out of the index and by nl.hash_join_probe to leave a probe row unmatched.
    static NLKeyIsMatchableFunction everyKeyMatchable();
    static NLKeyIsMatchableFunction selectOptKeyMatchableFunction(ValueType valueType);
    static NLKeyIsMatchableFunction selectPlainKeyMatchableFunction(ValueType valueType);
    static NLKeyIsMatchableFunction selectListElementKeyMatchableFunction();

    // Non-null row count for a COUNT. An ID chunk has no null rows, so countAllRows
    // is its handle (the row count); a nullable value chunk of this value type
    // counts only its present values. Used by nl.count_update.
    static size_t countAllRows(const Column* column);
    static NLCountFunction selectOptCountFunction(ValueType valueType);

    // Non-null row count for a COUNT over a node, edge or edge-type ID chunk, whose null
    // is an invalid ID rather than a missing optional; null for a kind that has no such
    // row, which then keeps countAllRows. The grouped sibling folds the same tally per
    // group.
    static NLCountFunction selectIDCountFunction(NLChunkKind kind);
    static NLGroupAggregateFoldFunction selectGroupCountValidIDFold(NLChunkKind kind);

    // The reset / fold / emit handlers for one aggregate, selected from the
    // reduction and a value type (the accumulator's for reset/result, the input's
    // for update). Throw for a value type the reduction cannot handle: sum/avg need
    // a numeric type, min/max an orderable one. Used by the nl.aggregate ops.
    static NLAggregateResetFunction selectAggregateReset(AggregateKind kind, ValueType accumulatorType);
    static NLAggregateUpdateFunction selectAggregateUpdate(AggregateKind kind, ValueType inputType);

    // The sibling reading a type-erased input column: sum and avg reduce its numeric
    // cells into the f64 accumulator, whatever tags they carry.
    static NLAggregateUpdateFunction selectTaggedAggregateUpdate(AggregateKind kind);
    static NLAggregateUpdateFunction selectOptTaggedAggregateUpdate(AggregateKind kind);
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

    // The sibling reading a type-erased input column: sum and avg reduce its numeric
    // cells into the f64 accumulator, whatever tags they carry.
    static NLGroupAggregateFoldFunction selectTaggedGroupAggregateFold(GroupAggregateKind kind);
    static NLGroupAggregateFoldFunction selectOptTaggedGroupAggregateFold(GroupAggregateKind kind);
    static NLGroupAggregateFoldFunction selectGroupCountAllFold();
    static NLGroupAggregateFoldFunction selectGroupCountDistinctFold(ValueType inputType);
    static NLGroupAggregateFoldFunction selectGroupCountDistinctChunkFold(NLChunkKind kind);

    // The grouped count / count(DISTINCT) folds of a type-erased column of tagged
    // scalars, the column a heterogeneous UNWIND produces
    static NLGroupAggregateFoldFunction selectGroupCountListElementFold();
    static NLGroupAggregateFoldFunction selectGroupCountOptListElementFold();
    static NLGroupAggregateFoldFunction selectGroupCountDistinctListElementFold();
    static NLGroupAggregateFoldFunction selectGroupCountDistinctOptListElementFold();
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

    // The row counts an nl.unwind reads its source column with: a list column drains its
    // elements, a type-erased column drains a cell tagged as a list and drops one tagged
    // null, a nullable value column drops its nulls, and a column holding a value in every
    // row spreads each of them to the single row it is.
    static NLUnwindElementCountFunction selectListUnwindElementCount();
    static NLUnwindElementCountFunction selectTaggedUnwindElementCount();
    static NLUnwindElementCountFunction selectOptUnwindElementCount(ValueType valueType);
    static NLUnwindElementCountFunction selectValueUnwindElementCount();

    // The drains filling an nl.unwind's element chunk from a column whose cells hold more
    // than the element: a drained list fills the column its own element type names, and
    // only one whose elements share no type fills the type-erased column. A scalar column
    // needs none - its cells are the elements, gathered through the carry set.
    static NLUnwindElementEmitFunction selectListUnwindElementEmit();
    static NLUnwindElementEmitFunction selectListUnwindValueEmit(ValueType valueType);
    static NLUnwindElementEmitFunction selectListUnwindNodeEmit();
    static NLUnwindElementEmitFunction selectListUnwindEdgeEmit();
    static NLUnwindElementEmitFunction selectListUnwindListEmit();
    static NLUnwindElementEmitFunction selectTaggedUnwindElementEmit();
    static NLCollectListEmitFunction selectCollectListEmit(ValueType valueType);

    // The reads an nl.make_list takes one element out of a column with: a nullable value
    // column gives the value it holds or a tagged null, an entity the ID it holds or that
    // same null where an OPTIONAL MATCH left the ID invalid, a nested list the cell it
    // holds in every row, a type-erased column the cell under the tag it already carries,
    // and a column owning its characters a copy of them.
    static NLListItemReadFunction selectValueListItemRead(ValueType valueType);
    static NLListItemReadFunction selectNodeListItemRead();
    static NLListItemReadFunction selectEdgeListItemRead();
    static NLListItemReadFunction selectNestedListItemRead();
    static NLListItemReadFunction selectTaggedListItemRead(bool nullable);
    static NLListItemReadFunction selectOwnedStringListItemRead(bool nullable);

    // The fold and list-emit for an entity chunk of this kind, whose elements carry a
    // node or edge ID. An edge-type ID is no entity, so the kind is rejected.
    static void selectCollectEntityHandlers(NLChunkKind kind,
                                            bool distinctValues,
                                            NLCollectFoldFunction& fold,
                                            NLCollectListEmitFunction& listEmit);

    // The handlers a collect of a list column reads: the cells nest into a list of lists.
    static void selectCollectListHandlers(bool distinctValues,
                                          NLCollectFoldFunction& fold,
                                          NLCollectListEmitFunction& listEmit);

    // The handlers a collect of a type-erased column reads: each cell keeps the type its
    // tag names, and a cell tagged null is dropped as Cypher's collect drops a null.
    static void selectCollectTaggedHandlers(bool distinctValues,
                                            NLCollectFoldFunction& fold,
                                            NLCollectListEmitFunction& listEmit);

    static void selectCollectOptTaggedHandlers(bool distinctValues,
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

    template <SupportedType T>
    static void runScanNodesByPropertyValueLoopAs(NLExecutionContext* context, NLScanByPropertyValueLoopData* loopData);
};

}
