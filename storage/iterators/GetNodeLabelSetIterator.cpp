#include "GetNodeLabelSetIterator.h"

#include <algorithm>

#include "datapart/DataPart.h"
#include "datapart/NodeContainer.h"
#include "metadata/LabelSetHandle.h"

#include "BioAssert.h"

using namespace db;

GetNodeLabelSetIterator::GetNodeLabelSetIterator(const GraphView& view,
                                                 const ColumnNodeIDs* inputNodeIDs)
    : Iterator(view),
    _inputNodeIDs(inputNodeIDs),
    _nodeIt(inputNodeIDs->cbegin())
{
}

GetNodeLabelSetIterator::~GetNodeLabelSetIterator() {
}

void GetNodeLabelSetIterator::next() {
    ++_nodeIt;
}

LabelSetID GetNodeLabelSetIterator::get() const {
    const NodeID nodeID = *_nodeIt;

    for (const auto& part : _view.dataparts()) {
        if (part->hasNode(nodeID)) {
            return part->nodes().getNodeLabelSet(nodeID).getID();
        }
    }

    return LabelSetID {};
}

GetNodeLabelSetChunkWriter::GetNodeLabelSetChunkWriter(const GraphView& view,
                                                       const ColumnNodeIDs* inputNodeIDs)
    : GetNodeLabelSetIterator(view, inputNodeIDs)
{
}

void GetNodeLabelSetChunkWriter::fill(size_t maxCount) {
    bioassert(_labelSetIDs, "GetNodeLabelSetChunkWriter must be initialized with a valid column");

    const size_t remainingInput = static_cast<size_t>(
        std::distance(_nodeIt, _inputNodeIDs->cend()));
    const size_t count = std::min(maxCount, remainingInput);

    _labelSetIDs->resize(count);
    std::generate(_labelSetIDs->begin(), _labelSetIDs->end(), [this]() {
        const LabelSetID id = get();
        next();
        return id;
    });
}
