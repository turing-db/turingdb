#include "ScanNodesByLabelStep.h"

#include <sstream>

using namespace db;

ScanNodesByLabelStep::ScanNodesByLabelStep(ColumnNodeIDs* nodes, const LabelSetHandle& labelSet)
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

    std::vector<LabelID> labels;
    _labelSet.decompose(labels);
    ss << fmt::format(" labelSet={}", fmt::join(labels, ", "));

    descr.assign(ss.str());
}
