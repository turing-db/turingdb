#include "ScanNodesStringApproxStep.h"

#include <sstream>

#include "DataPart.h"
#include "ID.h"
#include "ExecutionContext.h"
#include "PipelineException.h"
#include "Profiler.h"
#include "TuringException.h"
#include "columns/ColumnVector.h"
#include "indexes/StringIndexUtils.h"
#include "iterators/TombstoneFilter.h"
#include "views/GraphView.h"

using namespace db;
using Step = ScanNodesStringApproxStep;


Step::ScanNodesStringApproxStep(ColumnVector<NodeID>* nodes, const GraphView& view,
                                PropertyTypeID propID, std::string_view strQuery)
    : _nodes(nodes),
      _view(view),
      _pId(propID),
      _strQuery(strQuery)
{
}

Step::~ScanNodesStringApproxStep()
{
}

void Step::describe(std::string& descr) const {
    std::stringstream ss;
    ss << "ScanNodesByPropertyStep";
    ss << " nodes=" << std::hex << _nodes;
    ss << " propID=" << std::hex << _pId;
    ss << " query=" << std::hex << _strQuery;
    descr.assign(ss.str());
}

void Step::prepare(ExecutionContext* ctxt) {
}

void Step::reset() {
}

void Step::execute() {
    Profile profile {"ScanNodesStringApproxStep::execute"};
    // Fill _nodes with the matches of all datapart's indexes
    try {
        StringIndexUtils::getMatches<NodeID>(_nodes->getRaw(), _view, _pId, _strQuery);
        if (_view.tombstones().hasNodes()) {
            TombstoneFilter filter(_view.tombstones());
            filter.populateRanges(_nodes);
            filter.filter(_nodes);
        }
    } catch (TuringException& e) {
        throw PipelineException(e.what());
    }

    _done = true;
}
