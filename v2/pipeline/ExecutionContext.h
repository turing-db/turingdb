#pragma once

namespace db {
class GraphView;
}

namespace db::v2 {

class ExecutionContext {
public:
    ExecutionContext(const GraphView& graphView)
        : _graphView(graphView)
    {
    }

    const GraphView& getGraphView() const { return _graphView; }

private:
    const GraphView& _graphView;
};
}
