#pragma once

#include <cmath>
#include <optional>
#include <string_view>

#include "FatalException.h"
#include "Processor.h"
#include "WriteProcessorTypes.h"

#include "interfaces/PipelineBlockInputInterface.h"
#include "interfaces/PipelineBlockOutputInterface.h"
#include "dataframe/ColumnTag.h"
#include "interfaces/PipelineInputInterface.h"
#include "metadata/LabelSet.h"

namespace db {

class Dataframe;
class MetadataBuilder;
class CommitWriteBuffer;

}

namespace db::v2 {

class PipelineV2;

class WriteProcessor final : public Processor {
public:
    using DeletedNodes = std::vector<ColumnTag>;
    using DeletedEdges = std::vector<ColumnTag>;
    using PendingNodes = std::vector<WriteProcessorTypes::PendingNode>;
    using PendingEdges = std::vector<WriteProcessorTypes::PendingEdge>;
    using MaybeInput = std::optional<PipelineBlockInputInterface>;

    static WriteProcessor* create(PipelineV2* pipeline, bool hasInput = false);

    void prepare(ExecutionContext* ctxt) final;
    void reset() final;
    void execute() final;

    void setPendingNodes(const PendingNodes& nodes) { _pendingNodes = nodes; }
    void setPendingEdges(const PendingEdges& edges) { _pendingEdges = edges; }

    void setDeletedNodes(const DeletedNodes& nodes) { _deletedNodes = nodes; }
    void setDeletedEdges(const DeletedEdges& edges) { _deletedEdges = edges; }

    PipelineBlockInputInterface& input() {
        if (!_input) {
            throw FatalException("Attempted to get null input of WriteProcessor");
        }
        return _input.value();
    }

    PipelineBlockOutputInterface& output() { return _output; }

    std::string describe() const final {
        return "WriteProcessor";
    }

private:
    MaybeInput _input {PipelineBlockInputInterface {}};
    PipelineBlockOutputInterface _output;

    MetadataBuilder* _metadataBuilder {nullptr};
    CommitWriteBuffer* _writeBuffer {nullptr};

    DeletedNodes _deletedNodes;
    DeletedEdges _deletedEdges;

    PendingNodes _pendingNodes;
    PendingEdges _pendingEdges;

    void performDeletions();
    void performCreations();
    void performUpdates();

    void createNodes(size_t numIters);
    void createEdges(size_t numIters);
    void postProcessFakeIDs();

    LabelSet getLabelSet(std::span<const std::string_view> labels);

    WriteProcessor() = default;
    ~WriteProcessor() final = default;
};

}
