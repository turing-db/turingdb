#pragma once

namespace db::v2 {

class PlanGraphStream {
public:
    enum class Mode {
        EMPTY,
        NODES,
        EDGES
    };

    PlanGraphStream() = default;
    ~PlanGraphStream() = default;

    bool isNodeStream() const { return _mode == Mode::NODES; }
    bool isEdgeStream() const { return _mode == Mode::EDGES; }

private:
    Mode _mode {Mode::EMPTY};
};

}
