#pragma once

namespace db {

class GraphView;
class MemoryManager;

class PlannerContext {
public:
    PlannerContext(const GraphView& dbView,
                   MemoryManager* mem)
        : _dbView(dbView),
        _mem(mem)
    {
    }

    const GraphView& getGraphView() const { return _dbView; }

    MemoryManager* getMemoryManager() const { return _mem; }

private:
    const GraphView& _dbView;
    MemoryManager* _mem {nullptr};
};

}
