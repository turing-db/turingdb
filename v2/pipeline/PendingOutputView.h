#pragma once

#include <string_view>

#include "interfaces/PipelineEdgeOutputInterface.h"

namespace db {
class Dataframe;
}

namespace db::v2 {

class PendingOutputView {
public:
    PipelineOutputInterface* getInterface() const { return _iface; }
    Dataframe* getDataframe() const { return _iface->getDataframe(); }

    void setInterface(PipelineOutputInterface* iface) { 
        _iface = iface;
        _edgesProjectedOnOtherIDs = false;
    }

    void connectTo(PipelineNodeInputInterface& input) {
        _iface->connectTo(input);
    }

    void connectTo(PipelineBlockInputInterface& input) {
        _iface->connectTo(input);
    }

    void connectTo(PipelineEdgeInputInterface& input) {
        _iface->connectTo(input);
    }

    void projectEdgesOnOtherIDs() {
        _edgesProjectedOnOtherIDs = true;
    }

    bool isEdgesProjectedOnOtherIDs() const {
        return _edgesProjectedOnOtherIDs;
    }

    void rename(std::string_view name) {
        if (auto* edgeIface = dynamic_cast<PipelineEdgeOutputInterface*>(_iface)) {
            if (_edgesProjectedOnOtherIDs) {
                edgeIface->renameOtherIDs(name);
            } else {
                edgeIface->rename(name);
            }
        } else {
            _iface->rename(name);
        }
    }

private:
    bool _edgesProjectedOnOtherIDs {false};
    PipelineOutputInterface* _iface {nullptr};
};

}
