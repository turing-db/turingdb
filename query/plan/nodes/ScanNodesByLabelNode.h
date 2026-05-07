#pragma once

#include "PlanGraphNode.h"

#include "metadata/LabelSet.h"

namespace db {

class ScanNodesByLabelNode : public PlanGraphNode {
public:
    ScanNodesByLabelNode(PlanGraphNodeID id, const LabelSet& labelset)
        : PlanGraphNode(id, PlanGraphOpcode::SCAN_NODES_BY_LABEL),
        _labelset(labelset)
    {
    }

    const LabelSet& getLabelSet() const { return _labelset; }

private:
    LabelSet _labelset;
};

}
