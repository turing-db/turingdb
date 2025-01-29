#pragma once

#include "ExecutionContext.h"
#include "ColumnIDs.h"

namespace db {

class ExecutionContext;

class GetLabelSetIDStep {
public:
    struct Tag {};

    GetLabelSetIDStep(const ColumnIDs* nodeIDs, ColumnVector<LabelSetID>* labelsetIDs);
    ~GetLabelSetIDStep();

    void prepare(ExecutionContext* ctxt) {
        _view = &ctxt->getGraphView();
    }

    void reset() {}

    inline bool isFinished() const { return true; }

    void execute();

private:
    const ColumnIDs* _nodeIDs {nullptr};
    ColumnVector<LabelSetID>* _labelsetIDs {nullptr};
    const GraphView* _view {nullptr};
};

}
