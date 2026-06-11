#pragma once

#include <stddef.h>
#include <memory>
#include <vector>

#include "columns/ColumnEdgeTypes.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "iterators/ChunkConfig.h"

namespace db {

struct NLExecutionContext;
struct NLFunctionData;

// The element type of a chunk slot, mirroring the !nl.chunk element types of
// the nl dialect. Translation resolves every chunk SSA value to a concrete
// ColumnVector through this kind; the interpreter never consults a type again.
enum class NLChunkKind {
    NodeID,
    EdgeID,
    EdgeTypeID,
};

// A handler is an ordinary function composed by plain indirect calls: the
// interpreter is subroutine-threaded, so loop control stays a native C++ loop
// inside the handler and nesting maps onto the C call stack
// (see docs/subroutine_interpreter.md).
using NLHandlerFunction = void (*)(NLExecutionContext& context, NLFunctionData* data);

// One translated statement: the handler to call and the payload it runs on.
struct NLFunctionDescriptor {
    NLHandlerFunction _function {nullptr};
    NLFunctionData* _data {nullptr};
};

// Base of the per-descriptor payloads, owned by the NLProgram. Handlers know
// the concrete payload type of their descriptor and static_cast to it.
struct NLFunctionData {
    virtual ~NLFunctionData();
};

// Copies the input rows selected by the writer's indices column into the
// output chunk. Selected per NLChunkKind at translation time.
using NLGatherFunction = void (*)(const Column& input,
                                  const ColumnVector<size_t>& indices,
                                  Column& output);

// One columns_to_filter entry of an edge fetch: the chunk of the enclosing
// loop to filter down to the rows surviving the traversal, and the loop
// variable slot the filtered chunk lands in.
struct NLCarriedColumn {
    const Column* _input {nullptr};
    Column* _output {nullptr};
    NLGatherFunction _gather {nullptr};
};

// nl.for over an nl.scan_nodes iterator.
struct NLScanLoopData : public NLFunctionData {
    ColumnNodeIDs* _nodeIDs {nullptr};
    std::vector<NLFunctionDescriptor> _body;
};

// nl.for over an nl.get_out_edges or nl.get_in_edges iterator. The source op
// emits no descriptor of its own: its configuration is folded in here, so the
// handler drives the storage chunk writer directly.
struct NLEdgeLoopData : public NLFunctionData {
    const ColumnNodeIDs* _inputNodeIDs {nullptr};

    // The four fixed chunks of an edge iterator step, in loop-variable order
    ColumnNodeIDs* _sources {nullptr};
    ColumnEdgeIDs* _edgeIDs {nullptr};
    ColumnEdgeTypes* _edgeTypes {nullptr};
    ColumnNodeIDs* _targets {nullptr};

    std::vector<NLCarriedColumn> _carriedColumns;
    std::vector<NLFunctionDescriptor> _body;

    // Scratch for the writer's row-to-input-row map, which drives the gathers
    ColumnVector<size_t> _indices;
};

// nl.output: hand the current chunks to the sink, one output column per chunk.
struct NLOutputData : public NLFunctionData {
    std::vector<const Column*> _columns;
};

// A translated nl program: the descriptor tree plus all the state it points
// into. Slots are preallocated at translation time, so execution performs no
// allocation. The program owns its slot state, which means one NLProgram
// instance supports one execution at a time.
class NLProgram {
public:
    NLProgram();
    ~NLProgram();

    NLProgram(const NLProgram&) = delete;
    NLProgram& operator=(const NLProgram&) = delete;

    // Allocates the backing column of one chunk SSA value, reserved to the
    // chunk size. Set the chunk size before translating.
    Column* addChunkSlot(NLChunkKind kind);

    template <typename DataType>
    DataType* addFunctionData() {
        auto data = std::make_unique<DataType>();
        DataType* dataPointer = data.get();
        _functionData.push_back(std::move(data));
        return dataPointer;
    }

    std::vector<NLFunctionDescriptor>& getTopLevel() { return _topLevel; }
    const std::vector<NLFunctionDescriptor>& getTopLevel() const { return _topLevel; }

    size_t getChunkSize() const { return _chunkSize; }
    void setChunkSize(size_t chunkSize) { _chunkSize = chunkSize; }

private:
    std::vector<std::unique_ptr<Column>> _chunkSlots;
    std::vector<std::unique_ptr<NLFunctionData>> _functionData;
    std::vector<NLFunctionDescriptor> _topLevel;
    size_t _chunkSize {ChunkConfig::CHUNK_SIZE};
};

}
