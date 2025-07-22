#pragma once

#include "DataPartSpan.h"
#include "ID.h"
#include "views/GraphView.h"
#include <memory>

namespace db {

class ExecutionContext;
template <typename T>
class ColumnVector;
class GraphView;

class ScanNodesStringApproxStep {
public:
    struct Tag {};

    ScanNodesStringApproxStep(ColumnVector<NodeID>* nodes, const GraphView& view, PropertyTypeID propID,
                              std::string_view strQuery);
    ScanNodesStringApproxStep(ScanNodesStringApproxStep && other) = default;
    ~ScanNodesStringApproxStep();

    void prepare(ExecutionContext* ctxt);

    void reset();

    inline bool isFinished() const {
        return _done;
    }

    void execute();

    void describe(std::string& descr) const;

private:
    bool _done {false};
    std::unique_ptr<DataPartSpan> _dps;
    ColumnVector<NodeID>* _nodes {nullptr};
    const GraphView& _view;
    PropertyTypeID _pId {0};
    const std::string _strQuery;
};
}
