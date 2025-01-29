#include "GetLabelSetIDStep.h"

#include "GraphView.h"
#include "GraphReader.h"

using namespace db;

GetLabelSetIDStep::GetLabelSetIDStep(const ColumnIDs* nodeIDs,
                                     ColumnVector<LabelSetID>* labelsetIDs)
    : _nodeIDs(nodeIDs),
    _labelsetIDs(labelsetIDs)
{
}

GetLabelSetIDStep::~GetLabelSetIDStep() {
}

void GetLabelSetIDStep::execute() {
    const auto& nodeIDs = *_nodeIDs;
    ColumnVector<LabelSetID>& labelsetIDs = *_labelsetIDs;
    const auto reader = _view->read();

    labelsetIDs.clear();
    labelsetIDs.reserve(nodeIDs.size());
    for (EntityID nodeID : nodeIDs) {
        labelsetIDs.emplace_back(reader.getNodeLabelSetID(nodeID));
    }
}
