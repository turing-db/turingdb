#include "ScanOutEdgesByLabelStep.h"

#include <sstream>

using namespace db;

ScanOutEdgesByLabelStep::ScanOutEdgesByLabelStep(const EdgeWriteInfo& edgeWriteInfo, const LabelSet* labelSet)
    : _edgeWriteInfo(edgeWriteInfo),
    _labelSet(labelSet)
{
}

ScanOutEdgesByLabelStep::~ScanOutEdgesByLabelStep() {
}

void ScanOutEdgesByLabelStep::describe(std::string& descr) const {
    std::stringstream ss;
    ss << "ScanOutEdgesByLabelStep";
    ss << " indices=" << std::hex << _edgeWriteInfo._indices;
    ss << " sourceNodes=" << std::hex << _edgeWriteInfo._sourceNodes;
    ss << " edges=" << std::hex << _edgeWriteInfo._edges;
    ss << " targetNodes=" << std::hex << _edgeWriteInfo._targetNodes;
    ss << " edgeTypes=" << std::hex << _edgeWriteInfo._edgeTypes;
    ss << " labelSet=" << std::hex << _labelSet;
    descr.assign(ss.str());
}
