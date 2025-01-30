#pragma once

namespace db {

class GraphView;
class SystemManager;

class ExecutionContext {
public:
    explicit ExecutionContext(SystemManager* sysMan, const GraphView& dbView)
        : _sysMan(sysMan),
        _dbView(dbView)
    {
    }

    SystemManager* getSystemManager() const { return _sysMan; }
    const GraphView& getGraphView() const { return _dbView; }

private:
    SystemManager* _sysMan {nullptr};
    const GraphView& _dbView;
};

}
