#include "ScanInEdgesByLabelStep.h"

#include <sstream>
#include <spdlog/fmt/ranges.h>

using namespace db;

ScanInEdgesByLabelStep::ScanInEdgesByLabelStep(const EdgeWriteInfo& edgeWriteInfo, const LabelSetHandle& labelSet)
    : _edgeWriteInfo(edgeWriteInfo),
    _labelSet(labelSet)
{
}

ScanInEdgesByLabelStep::~ScanInEdgesByLabelStep() {
}

void ScanInEdgesByLabelStep::describe(std::string& descr) const {
    std::stringstream ss;
    ss << "ScanInEdgesByLabelStep";
    ss << " indices=" << std::hex << _edgeWriteInfo._indices;
    ss << " sourceNodes=" << std::hex << _edgeWriteInfo._sourceNodes;
    ss << " edges=" << std::hex << _edgeWriteInfo._edges;
    ss << " targetNodes=" << std::hex << _edgeWriteInfo._targetNodes;
    ss << " edgeTypes=" << std::hex << _edgeWriteInfo._edgeTypes;

    std::vector<LabelID> labels;
    _labelSet.decompose(labels);
    ss << fmt::format(" labelSet={}", fmt::join(labels, ", "));

    descr.assign(ss.str());
}
