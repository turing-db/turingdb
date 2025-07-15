#include "ScanNodesStringApproxStep.h"

#include "DataPart.h"
#include "ExecutionContext.h"
#include "Profiler.h"
#include "columns/ColumnVector.h"
#include "indexes/StringIndex.h"
#include "views/GraphView.h"

#include <memory>
#include <sstream>

using namespace db;
using Step = ScanNodesStringApproxStep;


Step::ScanNodesStringApproxStep(ColumnVector<NodeID>* nodes, PropertyTypeID propID,
                                std::string_view strQuery)
    : _nodes(nodes),
      _pId(propID),
      _strQuery(strQuery)
{
}

Step::~ScanNodesStringApproxStep() {
}

void Step::describe(std::string& descr) const {
    std::stringstream ss;
    ss << "ScanNodesByPropertyStep";
    ss << " nodes=" << std::hex << _nodes;
    ss << " query=" << std::hex << _strQuery;
    descr.assign(ss.str());
    
}

void Step::prepare(ExecutionContext* ctxt) {
    // XXX: Does this need be uniqueptr?
    _dps = std::make_unique<DataPartSpan>(ctxt->getGraphView().dataparts());
}

void Step::reset() {
    
}

inline bool Step::isFinished() const {
    return _dps != nullptr;
}

void Step::execute() {
    Profile profile {"ScanNodesStringApproxStep::execute"};

    // For each datapart
    for (DataPartIterator it = _dps->begin(); it != _dps->end(); it++) {
        // Query its index for each string
        const auto& nodeStringIndex = it->get()->getNodeStrPropIndex();
        const auto& strIndex = nodeStringIndex.at(_pId);

        // Accumulates matching node IDs into _nodes column vec
        strIndex->query<NodeID>(_nodes->getRaw(), _strQuery);
    }
    _dps.reset();
}
