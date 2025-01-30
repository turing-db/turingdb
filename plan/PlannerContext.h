#pragma once

namespace db {

class GraphView;
class LocalMemory;

class PlannerContext {
public:
    PlannerContext(const GraphView& dbView,
                   LocalMemory* mem)
        : _dbView(dbView),
        _mem(mem)
    {
    }

    const GraphView& getGraphView() const { return _dbView; }

    LocalMemory* getMemory() const { return _mem; }

private:
    const GraphView& _dbView;
    LocalMemory* _mem {nullptr};
};

}
