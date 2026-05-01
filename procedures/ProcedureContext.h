#pragma once

#include <stddef.h>

namespace db {

class Graph;
class GraphView;
class Transaction;
class ProcedureManager;

class ProcedureContext {
public:
    ProcedureContext() = default;

    Graph* getGraph() const { return _graph; }
    const GraphView* getGraphView() const { return _graphView; }
    Transaction* getTransaction() const { return _tx; }
    const ProcedureManager* getProcedures() const { return _procedures; }
    size_t getChunkSize() const { return _chunkSize; }

    void setGraph(Graph* graph) { _graph = graph; }
    void setGraphView(const GraphView* graphView) { _graphView = graphView; }
    void setTransaction(Transaction* tx) { _tx = tx; }
    void setProcedures(const ProcedureManager* procedures) { _procedures = procedures; }
    void setChunkSize(size_t chunkSize) { _chunkSize = chunkSize; }

private:
    Graph* _graph {nullptr};
    const GraphView* _graphView {nullptr};
    Transaction* _tx {nullptr};
    const ProcedureManager* _procedures {nullptr};
    size_t _chunkSize {0};
};

}
