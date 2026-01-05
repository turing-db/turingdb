#pragma once

#include "interfaces/PipelineInputInterface.h"

namespace db {
class NamedColumn;
}

namespace db::v2 {

class PipelineNodeInputInterface : public PipelineInputInterface {
public:
    constexpr PipelineInterfaceKind getKind() const override { return PipelineInterfaceKind::NODE; }

    NamedColumn* getNodeIDs() const { return _nodeIDs; }

    void setNodeIDs(NamedColumn* nodeIDs) { _nodeIDs = nodeIDs; }

private:
    NamedColumn* _nodeIDs {nullptr};
};


}
