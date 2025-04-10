#pragma once

#include <string>

#include "ExecutionContext.h"
#include "columns/ColumnVector.h"
#include "views/GraphView.h"
#include "reader/GraphReader.h"

namespace db {

class ExecutionContext;

class CallPropertyStep {
public:
    struct Tag {};

    explicit CallPropertyStep(ColumnVector<PropertyTypeID>* _id,
                              ColumnVector<std::string>* _propName,
                              ColumnVector<std::string>* _propType);
    ~CallPropertyStep();

    void prepare(ExecutionContext* ctxt) {
        _view = &ctxt->getGraphView();
    }

    bool isFinished() const { return true; }

    void reset() {}

    void execute();

    void describe(std::string& descr) const;

private:
    const GraphView* _view {nullptr};
    ColumnVector<PropertyTypeID>* _id {nullptr};
    ColumnVector<std::string>* _propName {nullptr};
    ColumnVector<std::string>* _propType {nullptr};
};

}
