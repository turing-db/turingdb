#include "GetLabelSetIDStep.h"

#include <sstream>

#include "views/GraphView.h"
#include "reader/GraphReader.h"

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
        labelsetIDs.emplace_back(reader.getNodeLabelSet(nodeID).getID());
    }
}

void GetLabelSetIDStep::describe(std::string& descr) const {
    std::stringstream ss;
    ss << "GetLabelSetIDStep";
    ss << " nodeIDs=" << std::hex << _nodeIDs;
    ss << " labelsetIDs=" << std::hex << _labelsetIDs;
    descr = ss.str();
}
