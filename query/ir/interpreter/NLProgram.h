#pragma once

#include <stddef.h>
#include <algorithm>
#include <memory>
#include <vector>

#include "ID.h"
#include "columns/ColumnEdgeTypes.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "iterators/ChunkConfig.h"

namespace db {

class NLExecutionContext;
class NLFunctionData;

// Translation resolves every chunk SSA value 
// to a concrete ColumnVector type through this kind
enum class NLChunkKind {
    NodeID,
    EdgeID,
    EdgeTypeID,
};

// Type of function pointers implementing operations
using NLHandlerFunction = void (*)(NLExecutionContext* context, NLFunctionData* data);

// Function descriptor representing translated statements
// It consists of a function pointer and function-specific data
class NLFunctionDescriptor {
public:
    NLFunctionDescriptor(NLHandlerFunction function,
                         NLFunctionData* data)
        : _function(function),
        _data(data)
    {
    }

    NLHandlerFunction getFunction() const { return _function; }
    NLFunctionData* getData() const { return _data; }

private:
    NLHandlerFunction _function {nullptr};
    NLFunctionData* _data {nullptr};
};

// Base class of function-specific data
class NLFunctionData {
public:
    virtual ~NLFunctionData() = default;
};

// Runtime state of one LIMIT: the remaining row budget and how many rows the
// current innermost step should emit. nl.limit resets it, nl.limit_update is the
// sole mutator (called once per innermost step before nl.output), and the loops
// and nl.output only read it. emitThisStep is what output emits; remaining is
// what the loops test to early-exit.
class NLLimitState {
public:
    void reset(size_t budget) {
        _remaining = budget;
        _emitThisStep = 0;
    }

    size_t remaining() const { return _remaining; }
    size_t emitThisStep() const { return _emitThisStep; }

    // Called once per innermost step by nl.limit_update, before nl.output: emit
    // min(rowsAvailable, remaining) this step and charge it against the budget.
    void update(size_t rowsAvailable) {
        _emitThisStep = std::min(rowsAvailable, _remaining);
        _remaining -= _emitThisStep;
    }

private:
    size_t _remaining {0};
    size_t _emitThisStep {0};
};

// Holds the translated statements of a program or loop body
class NLStmtContainer {
public:
    using Stmts = std::vector<NLFunctionDescriptor>;

    const Stmts& stmts() const { return _stmts; }

    void addStmt(const NLFunctionDescriptor& stmt) {
        _stmts.push_back(stmt);
    }

private:
    Stmts _stmts;
};

// Type of handles per column type that writes the input rows selected 
// by the indices column into output
using NLGatherFunction = void (*)(const Column* input,
                                  const ColumnVector<size_t>* indices,
                                  Column* output);

// Wraps a "carried" column from previous operations
class NLCarriedColumn {
public:
    NLCarriedColumn(const Column* input,
                    Column* output,
                    NLGatherFunction gather)
        : _input(input),
        _output(output),
        _gather(gather)
    {
    }

    const Column* getInput() const { return _input; }
    Column* getOutput() const { return _output; }
    NLGatherFunction getGatherFunc() const { return _gather; }

private:
    // Carried column pre-filtering
    const Column* _input {nullptr};

    // Output column after filtering
    Column* _output {nullptr};

    // Gather function to be used to process indices
    NLGatherFunction _gather {nullptr};
};

// nl.scan_nodes loop data
class NLScanLoopData : public NLFunctionData {
public:
    NLScanLoopData(ColumnNodeIDs* nodeIDs)
        : _nodeIDs(nodeIDs)
    {
    }

    ColumnNodeIDs* getNodeIDs() const { return _nodeIDs; }

    // The governing limit counter, or null for an unbounded loop. The loop
    // driver stops once it reaches zero.
    NLLimitState* getLimit() const { return _limit; }
    void setLimit(NLLimitState* limit) { _limit = limit; }

    NLStmtContainer* getStmts() { return &_stmts; }
    const NLStmtContainer* getStmts() const { return &_stmts; }

private:
    ColumnNodeIDs* _nodeIDs {nullptr};
    NLLimitState* _limit {nullptr};
    NLStmtContainer _stmts;
};

// nl.get_out_edges and nl.get_in_edges loop data
// The state is the same for get_out_edges/get_in_edges
class NLEdgeLoopData : public NLFunctionData {
public:
    using CarriedColumns = std::vector<NLCarriedColumn>;

    NLEdgeLoopData(const ColumnNodeIDs* input,
                   ColumnNodeIDs* sources,
                   ColumnEdgeIDs* edgeIDs,
                   ColumnEdgeTypes* edgeTypes,
                   ColumnNodeIDs* targets)
        : _inputNodeIDs(input),
        _sources(sources),
        _edgeIDs(edgeIDs),
        _edgeTypes(edgeTypes),
        _targets(targets)
    {
    }

    const ColumnNodeIDs* getInput() const { return _inputNodeIDs; }

    ColumnNodeIDs* getSources() const { return _sources; }
    ColumnEdgeIDs* getEdgeIDs() const { return _edgeIDs; }
    ColumnEdgeTypes* getEdgeTypes() const { return _edgeTypes; }
    ColumnNodeIDs* getTargets() const { return _targets; }

    ColumnVector<size_t>* getIndices() { return &_indices; }

    const CarriedColumns& carriedColumns() const { return _carriedColumns; }

    // The governing limit counter, or null for an unbounded loop. The loop
    // driver stops once it reaches zero.
    NLLimitState* getLimit() const { return _limit; }
    void setLimit(NLLimitState* limit) { _limit = limit; }

    NLStmtContainer* getStmts() { return &_stmts; }
    const NLStmtContainer* getStmts() const { return &_stmts; }

    void addCarriedColumn(const NLCarriedColumn& carried) {
        _carriedColumns.push_back(carried);
    }

private:
    const ColumnNodeIDs* _inputNodeIDs {nullptr};
    NLLimitState* _limit {nullptr};

    // The four fixed chunks of an edge iterator step, in loop-variable order
    ColumnNodeIDs* _sources {nullptr};
    ColumnEdgeIDs* _edgeIDs {nullptr};
    ColumnEdgeTypes* _edgeTypes {nullptr};
    ColumnNodeIDs* _targets {nullptr};

    CarriedColumns _carriedColumns;
    NLStmtContainer _stmts;

    // Scratch for the writer's row-to-input-row map, which drives the gathers
    ColumnVector<size_t> _indices;
};

// nl.get_node_properties / nl.get_edge_properties data: a with-null property
// read that maps the input ID column to a nullable value column, one value per
// input row (missing values are null, no row dropped). The node-vs-edge ID type
// and the value type are baked into the chosen handler; the PropertyTypeID was
// resolved from the name against the schema during translation.
class NLPropertyFetchData : public NLFunctionData {
public:
    NLPropertyFetchData(const Column* input, Column* output, PropertyTypeID propertyTypeID)
        : _input(input),
        _output(output),
        _propertyTypeID(propertyTypeID)
    {
    }

    const Column* getInput() const { return _input; }
    Column* getOutput() const { return _output; }
    PropertyTypeID getPropertyTypeID() const { return _propertyTypeID; }

private:
    const Column* _input {nullptr};
    Column* _output {nullptr};
    PropertyTypeID _propertyTypeID;
};

// A cross product emits N*M rows (every outer row paired with every inner row),
// but an input column holds only its N or M values, so its values must be
// repeated to fill an N*M-row output column. That repetition is the broadcast.
// With outer [a0,a1] (N=2) and inner [b0,b1,b2] (M=3) the result is:
//   outer -> [a0,a0,a0, a1,a1,a1]   each outer row repeated M times (block-repeat)
//   inner -> [b0,b1,b2, b0,b1,b2]   whole inner chunk repeated N times (tile)
// so row k across all columns is one (outer, inner) pair. `factor` is M for an
// outer column, N for an inner one; both directions share this signature.
using NLBroadcastFunction = void (*)(const Column* input, size_t factor, Column* output);

// One column used as operand of nl.cross_product: its input chunk, the output
// chunk to fill, and the broadcast that fills one from the other.
class NLCrossColumn {
public:
    NLCrossColumn(const Column* input,
                  Column* output,
                  NLBroadcastFunction broadcast)
        : _input(input),
        _output(output),
        _broadcast(broadcast)
    {
    }

    const Column* getInput() const { return _input; }
    Column* getOutput() const { return _output; }
    NLBroadcastFunction getBroadcast() const { return _broadcast; }

private:
    const Column* _input {nullptr};
    Column* _output {nullptr};
    NLBroadcastFunction _broadcast {nullptr};
};

// nl.cross_product data: the outer and inner columns of a cartesian product.
// N (outer rows) and M (inner rows) are read at run time from the first column
// of each group. Each outer column is block-repeated x M and each inner column
// is tiled x N, so the product has N*M rows.
class NLCrossProductData : public NLFunctionData {
public:
    using Columns = std::vector<NLCrossColumn>;

    const Columns& outerColumns() const { return _outerColumns; }
    const Columns& innerColumns() const { return _innerColumns; }

    void addOuterColumn(const NLCrossColumn& column) {
        _outerColumns.push_back(column);
    }

    void addInnerColumn(const NLCrossColumn& column) {
        _innerColumns.push_back(column);
    }

private:
    Columns _outerColumns;
    Columns _innerColumns;
};

// nl.limit data: resets a counter to its budget each time the block it lives in
// runs - once at function scope for a top-level LIMIT, or per enclosing step for
// a nested one.
class NLLimitInitData : public NLFunctionData {
public:
    NLLimitInitData(NLLimitState* state, size_t count)
        : _state(state),
        _count(count)
    {
    }

    NLLimitState* getState() const { return _state; }
    size_t getCount() const { return _count; }

private:
    NLLimitState* _state {nullptr};
    size_t _count {0};
};

// nl.limit_update data: the counter to charge and the representative column
// whose row count is charged against it (the same column nl.output emits).
class NLLimitUpdateData : public NLFunctionData {
public:
    NLLimitUpdateData(NLLimitState* state, const Column* rows)
        : _state(state),
        _rows(rows)
    {
    }

    NLLimitState* getState() const { return _state; }
    const Column* getRows() const { return _rows; }

private:
    NLLimitState* _state {nullptr};
    const Column* _rows {nullptr};
};

// nl.output data
class NLOutputData : public NLFunctionData {
public:
    using OutputColumns = std::vector<const Column*>;

    const OutputColumns& outputs() const { return _columns; }

    void addOutputColumn(const Column* col) {
        _columns.push_back(col);
    }

    // The governing limit counter, or null for an unbounded output. When set,
    // output emits only its emitThisStep() prefix; it never mutates the counter.
    NLLimitState* getLimit() const { return _limit; }
    void setLimit(NLLimitState* limit) { _limit = limit; }

private:
    std::vector<const Column*> _columns;
    NLLimitState* _limit {nullptr};
};

class NLProgram {
public:
    NLProgram();
    ~NLProgram();

    NLProgram(const NLProgram&) = delete;
    NLProgram& operator=(const NLProgram&) = delete;

    template <typename DataType, typename... Args>
    DataType* allocFunctionData(Args... args) {
        auto data = std::make_unique<DataType>(args...);
        DataType* dataPtr = data.get();
        _functionData.push_back(std::move(data));
        return dataPtr;
    }

    // Allocate one LIMIT's runtime counter, owned by the program; the
    // statements that reset, charge and read it hold a borrowed pointer.
    NLLimitState* allocLimitState() {
        auto state = std::make_unique<NLLimitState>();
        NLLimitState* statePtr = state.get();
        _limitStates.push_back(std::move(state));
        return statePtr;
    }

    NLStmtContainer* getStmts() { return &_stmts; }
    const NLStmtContainer* getStmts() const { return &_stmts; }

    size_t getChunkSize() const { return _chunkSize; }
    void setChunkSize(size_t chunkSize) { _chunkSize = chunkSize; }

private:
    size_t _chunkSize {ChunkConfig::CHUNK_SIZE};
    std::vector<std::unique_ptr<NLFunctionData>> _functionData;
    std::vector<std::unique_ptr<NLLimitState>> _limitStates;
    NLStmtContainer _stmts;
};

}
