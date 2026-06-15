#include "VariableDependencyGraphTraversal.h"

#include <algorithm>
#include <stack>

#include "DependencyEdge.h"
#include "VariableDependency.h"
#include "VariableDependencyGraph.h"


using namespace db;

void VariableDependencyGraphTraversal::getTraversal(const VariableDependencyGraph* graph,
                                                    std::vector<const VariableDependency*>& traversal) {
    traversal.clear();

    std::unordered_set<const VariableDependency*> visited;

    const auto degree = [](const VariableDependency& a, const VariableDependency& b) {
        return a.edges().size() < b.edges().size();
    };
    const auto maxIt = std::ranges::max_element(graph->vars(), degree);

    const VariableDependency* root = &*maxIt;

    std::stack<const VariableDependency*> stack;
    stack.push(root);

    while (!stack.empty()) {
        const VariableDependency* cur = stack.top();
        stack.pop();

        const bool alreadySeen = visited.contains(cur);

        bool canTraverse = true;

        for (const DependencyEdge* e : cur->incoming()) {
            if (e->isMetaEdge() && !alreadySeen) {
                canTraverse = false;
                break;
            }
        }

        if (!canTraverse) {
            visited.insert(cur);
            continue;
        }

        for (const DependencyEdge* e : cur->outgoing()) {
            const VariableDependency* tgt = e->tgt();
            stack.push(tgt);
        }

        visited.insert(cur);
        traversal.push_back(cur);
    }
}
