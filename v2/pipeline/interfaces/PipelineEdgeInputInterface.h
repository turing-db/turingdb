#pragma once

#include "PipelineInputInterface.h"

namespace db {
class NamedColumn;
}

namespace db::v2 {

class PipelineEdgeInputInterface : public PipelineInputInterface {
public:
    constexpr PipelineInterfaceKind getKind() const override { return PipelineInterfaceKind::EDGE; }

    void setEdges(NamedColumn* edgeIDs,
                  NamedColumn* otherNodes,
                  NamedColumn* edgeTypes) {
        _edgeIDs = edgeIDs;
        _otherNodes = otherNodes;
        _edgeTypes = edgeTypes;
    }

    NamedColumn* getEdgeIDs() const { return _edgeIDs; }
    NamedColumn* getOtherNodes() const { return _otherNodes; }
    NamedColumn* getEdgeTypes() const { return _edgeTypes; }

private:
    NamedColumn* _edgeIDs {nullptr};
    NamedColumn* _otherNodes {nullptr};
    NamedColumn* _edgeTypes {nullptr};
};

}
