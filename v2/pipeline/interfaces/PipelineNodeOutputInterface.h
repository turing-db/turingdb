#pragma once

#include <string_view>

#include "PipelineInputInterface.h"
#include "PipelineOutputInterface.h"

namespace db {
class NamedColumn;
}

namespace db::v2 {

class PipelineNodeOutputInterface : public PipelineOutputInterface {
public:
    constexpr PipelineInterfaceKind getKind() const override { return PipelineInterfaceKind::NODE; }

    void setNodeIDs(NamedColumn* nodeIDs) { _nodeIDs = nodeIDs; }
    void setIndices(NamedColumn* indices) { _indices = indices; }

    NamedColumn* getIndices() const { return _indices; }

    NamedColumn* getNodeIDs() const { return _nodeIDs; }

    void rename(const std::string_view& name) override;
    void connectTo(PipelineNodeInputInterface& input) override;
    void connectTo(PipelineBlockInputInterface& input) override;

private:
    NamedColumn* _indices {nullptr};
    NamedColumn* _nodeIDs {nullptr};
};

}
