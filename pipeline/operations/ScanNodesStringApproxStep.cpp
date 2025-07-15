#include "ScanNodesStringApproxStep.h"
#include "DataPart.h"
#include "ExecutionContext.h"
#include "Profiler.h"
#include "views/GraphView.h"

#include <memory>
#include <sstream>

using namespace db;
using Step = ScanNodesStringApproxStep;


Step::ScanNodesStringApproxStep(
    ColumnNodeIDs* nodes, ColumnVector<types::String>* propValues)
    : _nodes(nodes),
      _propValues(propValues)
{
}

Step::~ScanNodesStringApproxStep() {
}

void Step::describe(std::string& descr) const {
    std::stringstream ss;
    ss << "ScanNodesByPropertyStep";
    ss << " nodes=" << std::hex << _nodes;
    ss << " props=" << std::hex << _propValues;
    descr.assign(ss.str());
    
}

void Step::prepare(ExecutionContext* ctxt) {
    // XXX: Does this need be uniqueptr?
    _dps = std::make_unique<DataPartSpan>(ctxt->getGraphView().dataparts());
}

void Step::execute() {
    Profile profile {"ScanNodesStringApproxStep::execute"};
    
    // For each datapart
    for (DataPartIterator it = _dps->begin(); it != _dps->end(); it++) {
        // Query its index for each string
        //auto& nodeStringIndex = it->get()->getNodeStrPropIndex();

        // accumulate into NodeIDs
    }

}
