#include "ScanInEdgesByLabelStep.h"

using namespace db;

ScanInEdgesByLabelStep::ScanInEdgesByLabelStep(const EdgeWriteInfo& edgeWriteInfo, const LabelSet* labelSet)
    : _edgeWriteInfo(edgeWriteInfo),
    _labelSet(labelSet)
{
}

ScanInEdgesByLabelStep::~ScanInEdgesByLabelStep() {
}
