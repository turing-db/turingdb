#pragma once

#include <string>

#include "ExecutionContext.h"
#include "columns/ColumnIDs.h"

namespace db {

class ExecutionContext;

class GetLabelSetIDStep {
public:
    struct Tag {};

    GetLabelSetIDStep(const ColumnNodeIDs* nodeIDs, ColumnVector<LabelSetID>* labelsetIDs);
    ~GetLabelSetIDStep();

    void prepare(ExecutionContext* ctxt) {
        _view = &ctxt->getGraphView();
    }

    void reset() {}

    inline bool isFinished() const { return true; }

    void execute();

    void describe(std::string& descr) const;

private:
    const ColumnNodeIDs* _nodeIDs {nullptr};
    ColumnVector<LabelSetID>* _labelsetIDs {nullptr};
    const GraphView* _view {nullptr};
};

}
