#pragma once

#include <cstdint>
#include <span>
#include <vector>

namespace db::v2 {

class PlanGraphNode;

class PlanGraphBranch {
public:
    void setTip(PlanGraphNode* tip) { _tip = tip; }
    void setBranchId(uint16_t branchId) { _branchId = branchId; }
    void setIslandId(uint16_t islandId) { _islandId = islandId; }

    const PlanGraphNode* getTip() const { return _tip; }
    PlanGraphNode* getTip() { return _tip; }
    uint16_t getBranchId() const { return _branchId; }
    uint16_t getIslandId() const { return _islandId; }

    std::span<const PlanGraphBranch* const> children() const {
        return _children;
    }

    void addChild(PlanGraphBranch* child) {
        _children.emplace_back(child);
    }

private:
    PlanGraphNode* _tip {nullptr};
    std::vector<PlanGraphBranch*> _children;
    uint16_t _branchId {0};
    uint16_t _islandId {0};
};

}
