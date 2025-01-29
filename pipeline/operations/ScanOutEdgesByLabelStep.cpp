#include "ScanOutEdgesByLabelStep.h"

using namespace db;

ScanOutEdgesByLabelStep::ScanOutEdgesByLabelStep(const EdgeWriteInfo& edgeWriteInfo, const LabelSet* labelSet)
    : _edgeWriteInfo(edgeWriteInfo),
    _labelSet(labelSet)
{
}

ScanOutEdgesByLabelStep::~ScanOutEdgesByLabelStep() {
}
