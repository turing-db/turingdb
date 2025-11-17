#pragma once

#include <string_view>

#include "PipelineInputInterface.h"
#include "PipelineOutputInterface.h"

namespace db {
class NamedColumn;
}

namespace db::v2 {

class PipelineInputPort;
class PipelineOutputPort;
class PipelineNodeInputInterface;
class PipelineBlockInputInterface;
class PipelineEdgeInputInterface;

class PipelineEdgeOutputInterface : public PipelineOutputInterface {
public:
    constexpr PipelineInterfaceKind getKind() const override { return PipelineInterfaceKind::EDGE; }

    void setEdges(NamedColumn* edgeIDs,
                  NamedColumn* edgeTypes,
                  NamedColumn* otherNodes) {
        _edgeIDs = edgeIDs;
        _edgeTypes = edgeTypes;
        _otherNodes = otherNodes;
    }

    void setIndices(NamedColumn* indices) { _indices = indices; }

    NamedColumn* getIndices() const { return _indices; }

    NamedColumn* getEdgeIDs() const { return _edgeIDs; }
    NamedColumn* getOtherNodes() const { return _otherNodes; }
    NamedColumn* getEdgeTypes() const { return _edgeTypes; }

    void connectTo(PipelineEdgeInputInterface& input) override;
    void connectTo(PipelineNodeInputInterface& input) override;
    void connectTo(PipelineBlockInputInterface& input) override;
    void rename(const std::string_view& name) override;
    void renameOtherIDs(const std::string_view& name);

private:
    NamedColumn* _indices {nullptr};
    NamedColumn* _edgeIDs {nullptr};
    NamedColumn* _otherNodes {nullptr};
    NamedColumn* _edgeTypes {nullptr};
};

}
