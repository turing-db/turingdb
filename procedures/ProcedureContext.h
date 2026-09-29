#pragma once

#include <stddef.h>

// Full definition needed: `ListBuffer<>` relies on its default template
// argument, which may only be declared once per TU — forward-declaring it here
// with the default collides with list/ListBuffer.h in any TU that includes both.
#include "list/ListBuffer.h"

namespace db {

class Graph;
class GraphView;
class Transaction;
class ProcedureManager;
class StringBuffer;

class ProcedureContext {
public:
    ProcedureContext() = default;

    Graph* getGraph() const { return _graph; }
    const GraphView* getGraphView() const { return _graphView; }
    Transaction* getTransaction() const { return _tx; }
    const ProcedureManager* getProcedures() const { return _procedures; }
    size_t getChunkSize() const { return _chunkSize; }
    QueryListBuffer* getListBuffer() const { return _listBuffer; }
    StringBuffer* getStringBuffer() const { return _stringBuffer; }

    void setGraph(Graph* graph) { _graph = graph; }
    void setGraphView(const GraphView* graphView) { _graphView = graphView; }
    void setTransaction(Transaction* tx) { _tx = tx; }
    void setProcedures(const ProcedureManager* procedures) { _procedures = procedures; }
    void setChunkSize(size_t chunkSize) { _chunkSize = chunkSize; }
    void setListBuffer(ListBuffer<>* listBuffer) { _listBuffer = listBuffer; }
    void setStringBuffer(StringBuffer* stringBuffer) { _stringBuffer = stringBuffer; }

private:
    Graph* _graph {nullptr};
    const GraphView* _graphView {nullptr};
    Transaction* _tx {nullptr};
    const ProcedureManager* _procedures {nullptr};
    size_t _chunkSize {0};
    QueryListBuffer* _listBuffer {nullptr};
    StringBuffer* _stringBuffer {nullptr};
};

}
