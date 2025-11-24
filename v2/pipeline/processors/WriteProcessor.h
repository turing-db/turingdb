#pragma once

#include "Processor.h"
#include "WriteProcessorTypes.h"

#include "interfaces/PipelineBlockInputInterface.h"
#include "interfaces/PipelineBlockOutputInterface.h"
#include "dataframe/ColumnTag.h"

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

    static WriteProcessor* create(PipelineV2* pipeline);

    void prepare(ExecutionContext* ctxt) final;
    void reset() final;
    void execute() final;

    void setDeletedNodes(const DeletedNodes& nodes) { _deletedNodes = nodes; }
    void setDeletedEdges(const DeletedEdges& edges) { _deletedEdges = edges; }

    PipelineBlockInputInterface& input() { return _input; }
    PipelineBlockOutputInterface& output() { return _output; }

    std::string describe() const final {
        return "WriteProcessor";
    }

private:
    PipelineBlockInputInterface _input;
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

    WriteProcessor() = default;
    ~WriteProcessor() final = default;
};

}
