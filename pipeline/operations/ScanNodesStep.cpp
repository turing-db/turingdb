#include "ScanNodesStep.h"

#include <sstream>

using namespace db;

ScanNodesStep::ScanNodesStep(ColumnNodeIDs* nodes)
    : _nodes(nodes)
{
}

ScanNodesStep::~ScanNodesStep() {
}

void ScanNodesStep::describe(std::string& descr) const {
    std::stringstream ss;
    ss << "ScanNodesStep";
    ss << " nodes=" << std::hex << _nodes;
    descr.assign(ss.str());
}
