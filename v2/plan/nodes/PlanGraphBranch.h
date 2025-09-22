#pragma once

#include <cstdint>
namespace db::v2 {

class PlanGraphNode;

class PlanGraphBranch {
public:
    void setTip(PlanGraphNode* tip) { _tip = tip; }
    void setBranchId(uint16_t branchId) { _branchId = branchId; }

    PlanGraphNode* tip() { return _tip; }
    const PlanGraphNode* tip() const { return _tip; }
    uint16_t branchId() const { return _branchId; }

private:
    PlanGraphNode* _tip {nullptr};
    uint16_t _branchId {0};
};

}
