#pragma once

#include <variant>

#include "Processor.h"

#include "interfaces/PipelineBlockInputInterface.h"
#include "interfaces/PipelineBlockOutputInterface.h"

#include "metadata/PropertyType.h"
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
    using PendingNodeOffset = size_t;
    using IncidentNode = std::variant<PendingNodeOffset, ColumnTag>;

    PipelineBlockInputInterface _input;
    PipelineBlockOutputInterface _output;

    MetadataBuilder* _metadataBuilder;
    CommitWriteBuffer* _writeBuffer;

    DeletedNodes _deletedNodes;
    DeletedEdges _deletedEdges;

    struct PropertyConstraint {
        std::string_view _propName;
        ValueType _type;
        ColumnTag _tag;
    };

    struct PropertyConstraints {
        std::vector<PropertyConstraint> _properties;
    };

    struct PendingNode {
        std::vector<std::string_view> _labels;
        PropertyConstraints* _properties;
    };

    struct PendingEdge {
        IncidentNode _srcTag;
        IncidentNode _tgtTag;
        std::string_view _edgeType;
        PropertyConstraints* _properties;
    };

    void performDeletions();
    void performCreates();
    void performUpdates();

    WriteProcessor() = default;
    ~WriteProcessor() final = default;
};
}
