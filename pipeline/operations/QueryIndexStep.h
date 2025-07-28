#pragma once

#include <sstream>

#include "ID.h"
#include "ScanNodesStringApproxStep.h"
#include "indexes/StringIndexUtils.h"
#include "columns/ColumnSet.h"
#include "views/GraphView.h"

namespace db {

class ExecutionContext;

template <typename IDT>
    requires(Identifier<IDT>)
class QueryIndexStep {
public:
    struct Tag {};

    QueryIndexStep(ColumnSet<IDT>* outSet, const GraphView& view, PropertyTypeID propID,
                   const std::string& strQuery)
        : _set(outSet),
          _view(view),
          _pId(propID),
          _strQuery(strQuery)
    {
    }

    QueryIndexStep(QueryIndexStep<IDT>&& other) = default;

    ~QueryIndexStep()
    {
    }

    void prepare(ExecutionContext* ctxt) {
    }

    void reset() {
        _set->clear();
    }

    inline bool isFinished() const {
        return _done;
    }

    void execute() {
        std::vector<IDT> matchesVec {};

        StringIndexUtils::getMatches(matchesVec, _view, _pId, _strQuery);

        for (const auto& match : matchesVec) {
            _set->insert(match);
        }

        _done = true;
    }

    void describe(std::string& descr) const {
        std::stringstream ss;
        ss << "QueryIndexStep";
        if constexpr (std::same_as<IDT, NodeID>) {
            ss << " type(IDT)= NodeID";
        }
        else if constexpr (std::same_as<IDT, EdgeID>) {
            ss << " type(IDT)= EdgeID";
        }
        ss << " set=" << std::hex << _set;
        ss << " propID=" << std::hex << _pId;
        ss << " query=" << std::hex << _strQuery;
    }

private:
    ColumnSet<IDT>* _set;
    const GraphView& _view;
    PropertyTypeID _pId;
    const std::string& _strQuery;
    bool _done {false};
};

using QueryNodeIndexStep = QueryIndexStep<NodeID>;
using QueryEdgeIndexStep = QueryIndexStep<EdgeID>;
}
