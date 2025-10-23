#pragma once

#include <variant>

namespace db::v2 {

class MaterializeData;
class PipelineOutputInterface;

class PlanGraphStream {
public:
    enum class Mode {
        EMPTY,
        NODE_STREAM,
        EDGE_STREAM
    };

    PlanGraphStream() = default;
    ~PlanGraphStream() = default;

    PipelineOutputInterface* getInterface() const { return _currentOut; }

    // Stream mode
    bool isNodeStream() const { return _mode == Mode::NODE_STREAM; }
    bool isEdgeStream() const { return _mode == Mode::EDGE_STREAM; }

    void setNodes(PipelineOutputInterface& nodes) {
        _mode = Mode::NODE_STREAM;
        _currentOut = &nodes;
    }
    
    void setEdges(PipelineOutputInterface& edges) {
        _mode = Mode::EDGE_STREAM;
        _currentOut = &edges;
    }

    // MaterializeData
    MaterializeData* getMaterializeData() const { return _matData; }

    void setMaterializeData(MaterializeData* data) { _matData = data; }
    void closeMaterializeData() { _matData = nullptr; }

private:
    Mode _mode {Mode::EMPTY};
    MaterializeData* _matData {nullptr};
    PipelineOutputInterface* _currentOut {nullptr};
};

}
