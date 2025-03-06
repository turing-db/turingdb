#include "ScanNodesByLabelStep.h"

#include <sstream>

using namespace db;

ScanNodesByLabelStep::ScanNodesByLabelStep(ColumnIDs* nodes, const LabelSet* labelSet)
    : _nodes(nodes),
    _labelSet(labelSet)
{
}

ScanNodesByLabelStep::~ScanNodesByLabelStep() {
}

void ScanNodesByLabelStep::describe(std::string& descr) const {
    std::stringstream ss;
    ss << "ScanNodesByLabelStep";
    ss << " nodes=" << std::hex << _nodes;
    ss << " labelSet=" << std::hex << _labelSet;
    descr.assign(ss.str());
}
