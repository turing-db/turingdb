#pragma once

#include <string>

#include "ExecutionContext.h"
#include "columns/ColumnVector.h"
#include "views/GraphView.h"
#include "reader/GraphReader.h"

namespace db {

class ExecutionContext;

class CallLabelStep {
public:
    struct Tag {};

    explicit CallLabelStep(ColumnVector<LabelID>* id,
                           ColumnVector<std::string>* labelName);
    ~CallLabelStep();

    void prepare(ExecutionContext* ctxt) {
        _view = &ctxt->getGraphView();
    }

    bool isFinished() const { return true; }

    void reset() {}

    void execute();

    void describe(std::string& descr) const;

private:
    const GraphView* _view {nullptr};
    ColumnVector<LabelID>* _id {nullptr};
    ColumnVector<std::string>* _labelName {nullptr};
};

}
