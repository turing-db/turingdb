#pragma once

#include <string>

#include "ExecutionContext.h"
#include "columns/ColumnVector.h"
#include "views/GraphView.h"

namespace db {

class ExecutionContext;

class HistoryStep {
public:
    struct Tag {};

    explicit HistoryStep(ColumnVector<std::string>* log);
    ~HistoryStep();

    void prepare(ExecutionContext* ctxt) {
        _writeTx = ctxt->getWriteTransaction();
        _view = &ctxt->getGraphView();
    }

    bool isFinished() const { return true; }

    void reset() {}

    void execute();

    void describe(std::string& descr) const;

private:
    const GraphView* _view {nullptr};
    WriteTransaction* _writeTx {nullptr};
    ColumnVector<std::string>* _log {nullptr};
};

} 
