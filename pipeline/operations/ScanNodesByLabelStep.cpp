#include "ScanNodesByLabelStep.h"

using namespace db;

ScanNodesByLabelStep::ScanNodesByLabelStep(ColumnIDs* nodes, const LabelSet* labelSet)
    : _nodes(nodes),
    _labelSet(labelSet)
{
}

ScanNodesByLabelStep::~ScanNodesByLabelStep() {
}
