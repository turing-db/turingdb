#pragma once

#include <stddef.h>
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

    NLStmtContainer* getStmts() { return &_stmts; }
    const NLStmtContainer* getStmts() const { return &_stmts; }

private:
    ColumnNodeIDs* _nodeIDs {nullptr};
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

    NLStmtContainer* getStmts() { return &_stmts; }
    const NLStmtContainer* getStmts() const { return &_stmts; }

    void addCarriedColumn(const NLCarriedColumn& carried) {
        _carriedColumns.push_back(carried);
    }

private:
    const ColumnNodeIDs* _inputNodeIDs {nullptr};

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

// nl.output data
class NLOutputData : public NLFunctionData {
public:
    using OutputColumns = std::vector<const Column*>;

    const OutputColumns& outputs() const { return _columns; }

    void addOutputColumn(const Column* col) {
        _columns.push_back(col);
    }

private:
    std::vector<const Column*> _columns;
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

    NLStmtContainer* getStmts() { return &_stmts; }
    const NLStmtContainer* getStmts() const { return &_stmts; }

    size_t getChunkSize() const { return _chunkSize; }
    void setChunkSize(size_t chunkSize) { _chunkSize = chunkSize; }

private:
    size_t _chunkSize {ChunkConfig::CHUNK_SIZE};
    std::vector<std::unique_ptr<NLFunctionData>> _functionData;
    NLStmtContainer _stmts;
};

}
