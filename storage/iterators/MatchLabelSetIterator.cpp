#include "MatchLabelSetIterator.h"

#include "NodeContainer.h"
#include "DataPart.h"

using namespace db;

MatchLabelSetIterator::MatchLabelSetIterator(const GraphView& view, const LabelSet* labelSet)
    : Iterator(view), 
    _targetLabelSet(labelSet)
{
    for (; _partIt.isValid(); _partIt.next()) {
        const DataPart* part = _partIt.get();
        const NodeContainer& nodes = part->nodes();
        const auto& labelsetIndexer = nodes.getLabelSetIndexer();
        _labelSetIt = labelsetIndexer.matchIterate(_targetLabelSet);

        if (_labelSetIt.isValid()) {
            break;
        }
    }
}

MatchLabelSetIterator::~MatchLabelSetIterator() {
}

void MatchLabelSetIterator::next() {
    _labelSetIt.next();
    while (!_labelSetIt.isValid()) {
        _partIt.next();
        if (!isValid()) {
            return;
        }

        const NodeContainer& nodes = _partIt.get()->nodes();
        const auto& labelsetIndexer = nodes.getLabelSetIndexer();
        _labelSetIt = labelsetIndexer.matchIterate(_targetLabelSet);
    }
}
