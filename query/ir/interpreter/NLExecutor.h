#pragma once

#include <stddef.h>

#include "metadata/PropertyType.h"

#include "NLProgram.h"

namespace db {

class GraphView;
class NLOutputSink;

class NLExecutionContext {
public:
    NLExecutionContext(const GraphView* view,
                       NLOutputSink* sink,
                       size_t chunkSize)
        : _view(view),
        _sink(sink),
        _chunkSize(chunkSize)
    {
    }

    const GraphView* getView() const { return _view; }
    NLOutputSink* getSink() const { return _sink; }
    size_t getChunkSize() const { return _chunkSize; }

private:
    const GraphView* _view {nullptr};
    NLOutputSink* _sink {nullptr};
    size_t _chunkSize {0};
};

// Executes a translated NLProgram against a graph view
class NLExecutor {
public:
    NLExecutor(const GraphView* view,
                  const NLProgram* prog,
                  NLOutputSink* sink);
    ~NLExecutor();

    void run();

    static void runScanNodesLoop(NLExecutionContext* context, NLFunctionData* data);
    static void runGetOutEdgesLoop(NLExecutionContext* context, NLFunctionData* data);
    static void runGetInEdgesLoop(NLExecutionContext* context, NLFunctionData* data);
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

    // Zero the tally of a COUNT; runs each time its block runs.
    static void runCountReset(NLExecutionContext* context, NLFunctionData* data);

    // Add this step's chunk's non-null row count to the tally. The sole tally
    // mutator.
    static void runCountUpdate(NLExecutionContext* context, NLFunctionData* data);

    // The emit step of a COUNT: materialize the final tally as the output chunk's
    // single unsigned i64 row. Runs once, after the producing loop; nl.output emits
    // the chunk at function scope.
    static void runCountResult(NLExecutionContext* context, NLFunctionData* data);

    static void runOutput(NLExecutionContext* context, NLFunctionData* data);
    static NLGatherFunction selectGatherFunction(NLChunkKind kind);

    // Gather for a nullable value chunk of this value type (sort emit re-chunk).
    static NLGatherFunction selectOptGatherFunction(ValueType valueType);

    // Append (onto a buffer tail) for an ID chunk of this kind / a nullable value
    // chunk of this value type. Used by nl.sort_collect.
    static NLAppendFunction selectAppendFunction(NLChunkKind kind);
    static NLAppendFunction selectOptAppendFunction(ValueType valueType);

    // The 3-way row comparator for an ID key column of this kind / a nullable
    // value key column of this value type. The value-type selector throws for a
    // value type with no order (an embedding), which cannot be a sort key.
    static NLCompareFunction selectCompareFunction(NLChunkKind kind);
    static NLCompareFunction selectOptCompareFunction(ValueType valueType);

    // Block-repeat for an ID chunk of this kind (outer column).
    static NLBroadcastFunction selectBlockRepeatFunction(NLChunkKind kind);

    // Tile for an ID chunk of this kind (inner column).
    static NLBroadcastFunction selectTileFunction(NLChunkKind kind);

    // Block-repeat for a nullable value chunk of this value type (outer column).
    static NLBroadcastFunction selectOptBlockRepeatFunction(ValueType valueType);

    // Tile for a nullable value chunk of this value type (inner column).
    static NLBroadcastFunction selectOptTileFunction(ValueType valueType);

    // Range copy for an ID chunk of this kind (skip suffix copy).
    static NLCopyFunction selectCopyFunction(NLChunkKind kind);

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

    // The with-null property fetch handler for an ID type (NodeID/EdgeID) and a
    // value type (types::Double, ...). The translator picks the specialization
    // from the resolved property and stores it as the statement's handler; only
    // the explicitly instantiated (ID, T) pairs in NLExecutor.cpp are available.
    template <typename ID, typename T>
    static void runPropertyFetch(NLExecutionContext* context, NLFunctionData* data);

private:
    NLExecutionContext _ctxt;
    const NLProgram* _prog {nullptr};
};

}
