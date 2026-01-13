#pragma once

#include <math.h>
#include <optional>
#include <string_view>

#include "Processor.h"
#include "WriteProcessorTypes.h"

#include "interfaces/PipelineBlockInputInterface.h"
#include "interfaces/PipelineBlockOutputInterface.h"
#include "dataframe/ColumnTag.h"
#include "interfaces/PipelineInputInterface.h"
#include "metadata/LabelSet.h"

#include "FatalException.h"

namespace db {

class Dataframe;
class MetadataBuilder;
class CommitWriteBuffer;

}

namespace db {

class PipelineV2;
class ExprProgram;

class WriteProcessor final : public Processor {
public:
    using DeletedNodes = std::vector<ColumnTag>;
    using DeletedEdges = std::vector<ColumnTag>;
    using PendingNodes = std::vector<WriteProcessorTypes::PendingNode>;
    using PendingEdges = std::vector<WriteProcessorTypes::PendingEdge>;

    static WriteProcessor* create(PipelineV2* pipeline,
                                  ExprProgram* exprProg,
                                  bool hasInput = false);

    void prepare(ExecutionContext* ctxt) final;
    void reset() final;
    void execute() final;

    void setPendingNodes(const PendingNodes& nodes) { _pendingNodes = nodes; }
    void setPendingEdges(const PendingEdges& edges) { _pendingEdges = edges; }

    void setDeletedNodes(const DeletedNodes& nodes) { _deletedNodes = nodes; }
    void setDeletedEdges(const DeletedEdges& edges) { _deletedEdges = edges; }

    PipelineBlockInputInterface& input() {
        if (!_input) {
            throw FatalException("Attempted to get null input of WriteProcessor.");
        }
        return _input.value();
    }

    PipelineBlockOutputInterface& output() { return _output; }

    std::string describe() const final {
        return "WriteProcessor";
    }

private:
    // WriteProcessor may have an input, but it does not require one.
    // @ref WriteProcessor::create may default initialise a block input, otherwise the
    // default state of the processor is no input (nullopt)
    std::optional<PipelineBlockInputInterface> _input;
    PipelineBlockOutputInterface _output;

    MetadataBuilder* _metadataBuilder {nullptr};
    CommitWriteBuffer* _writeBuffer {nullptr};

    DeletedNodes _deletedNodes;
    DeletedEdges _deletedEdges;

    PendingNodes _pendingNodes;
    PendingEdges _pendingEdges;

    ExprProgram* _exprProgram {nullptr};

    bool _writtenRowsThisCycle {false};

    /**
     * @brief Set up logic called at the start of each call to @ref execute.
     * @detail Clears all columns for pending nodes/edges. These columns should be empty
     * at each call so that their size never excedes a chunk. Also checks that each column
     * for pending nodes/edges are present in the output DF.
     */
    void setup();

    /**
     * @brief Adds nodes and edges which are marked to be deleted to the @ref
     * _writeBuffer.
     * @warn Currently always deletes any edges which are incident to a deleted node.
     */
    void performDeletions();

    /**
     * @brief Adds pending nodes and edges to @ref _writeBuffer, one copy for each input
     * row. Populates the dataframe of @ref _output with estimated IDs for each node/edge
     * (where "estimation" is an estimation of what each node/edge ID will be once
     * committed).
     */
    void performCreations();

    /**
     * @warn NOT IMPLEMENTED
     */
    void performUpdates() = delete;

    /**
     * @brief Adds @param numIters copies of each element of @ref _pendingNodes to @ref
     * _writeBuffer. Fills @ref _output dataframe with the index in @ref
     * _writeBuffer::_pendingNodes for which each node can be found.
     */
    void createNodes(size_t numIters);

    /**
     * @brief Adds @param numIters copies of each element of @ref _pendingEdges to
     * @ref _writeBuffer. Fills @ref _output dataframe with the index in @ref
     * _writeBuffer::_pendingEdges for which each node can be found.
     * @warn Requires @ref createNodes to have been called prior
     */
    void createEdges(size_t numIters);

    /**
     * @brief Transforms each column in @ref _output dataframe which has a tag belonging
     * to an element of @ref _pendingNodes or @ref _pendingEdges into an estimation of the
     * committed Node/EdgeID for each row in that column.
     */
    void postProcessTempIDs();

    /**
    * @brief Helper function to generate a LabelSet from a collection of node label names.
    * @warn Calls @ref _metadataBuilder::getOrCreateLabel as a side effect.
    */
    LabelSet getLabelSet(std::span<const std::string_view> labels);

    WriteProcessor(ExprProgram* exprProg);
    ~WriteProcessor() final = default;
};

}
