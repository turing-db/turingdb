#pragma once

#include <string_view>

#include "PlanGraphNode.h"

namespace db {

class LoadCommitNode : public PlanGraphNode {
public:
    LoadCommitNode(PlanGraphNodeID id, std::string_view hashStr)
        : PlanGraphNode(id, PlanGraphOpcode::LOAD_COMMIT),
        _hashStr(hashStr)
    {
    }

    std::string_view getHashStr() const { return _hashStr; }

private:
    std::string_view _hashStr;
};

}
