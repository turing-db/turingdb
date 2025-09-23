#pragma once

#include <queue>
#include <unordered_set>

#include "nodes/PlanGraphNode.h"

namespace db::v2 {

class PlanGraphTopology {
public:
    enum class PathToDependency {
        SameVar,
        BackwardPath,
        UndirectedPath,
        NoPath
    };

    static PathToDependency getShortestPath(const PlanGraphNode* origin, const PlanGraphNode* target) {
        if (target == origin) {
            return PathToDependency::SameVar;
        }

        std::queue<const PlanGraphNode*> phase1;
        std::queue<const PlanGraphNode*> phase2;
        std::unordered_set<const PlanGraphNode*> visited;

        phase1.push(origin);
        visited.insert(origin);

        while (!phase1.empty()) {
            const PlanGraphNode* node = phase1.front();
            phase1.pop();

            if (node == target) {
                return PathToDependency::BackwardPath;
            }

            for (const auto& in : node->inputs()) {
                if (!visited.insert(in).second) {
                    continue; // Already visited
                }

                phase1.push(in);
            }

            for (const auto& out : node->outputs()) {
                if (!visited.insert(out).second) {
                    continue; // Already visited
                }

                phase2.push(out);
            }
        }

        while (!phase2.empty()) {
            const PlanGraphNode* node = phase2.front();
            phase2.pop();

            if (node == target) {
                return PathToDependency::UndirectedPath;
            }

            for (const auto& out : node->outputs()) {
                if (!visited.insert(out).second) {
                    continue; // Already visited
                }

                phase2.push(out);
            }

            for (const auto& in : node->inputs()) {
                if (!visited.insert(in).second) {
                    continue; // Already visited
                }

                phase2.push(in);
            }
        }

        return PathToDependency::NoPath;
    }

    static PlanGraphNode* getBranchTip(PlanGraphNode* origin) {
        std::queue<PlanGraphNode*> q;

        // Should not be needed since we should have loops
        std::unordered_set<PlanGraphNode*> visited;

        q.push(origin);
        visited.insert(origin);

        while (!q.empty()) {
            PlanGraphNode* node = q.front();
            q.pop();

            const auto& outputs = node->outputs();

            if (outputs.empty()) {
                return node;
            }

            for (const auto& out : node->outputs()) {
                if (!visited.insert(out).second) {
                    continue; // Already visited
                }

                q.push(out);
            }
        }

        return nullptr; // Should not happen
    }
};

}
