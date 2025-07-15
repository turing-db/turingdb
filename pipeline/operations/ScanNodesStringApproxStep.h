#pragma once

#include "DataPartSpan.h"
#include "metadata/PropertyType.h"
#include <memory>

namespace db {

class ColumnNodeIDs;
template <typename T>
class ColumnVector;
class ExecutionContext;

class ScanNodesStringApproxStep {
public:
    ScanNodesStringApproxStep(ColumnNodeIDs* nodes,
                             ColumnVector<types::String>* propValues);
    ScanNodesStringApproxStep(ScanNodesStringApproxStep && other) = default;
    ~ScanNodesStringApproxStep();

    void prepare(ExecutionContext* ctxt);

    inline void reset();

    inline bool isFinished() const;

    inline void execute();

    void describe(std::string& descr) const;

private:
    std::unique_ptr<DataPartSpan> _dps;
    ColumnNodeIDs* _nodes {nullptr};
    ColumnVector<types::String>* _propValues {nullptr};
};
}
