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

    std::stack<const VariableDependency*> stack;
    for (const VariableDependency& v : graph->vars()) {
        if (visited.contains(&v)) {
            continue;
        }

        stack.push(&v);

        while (!stack.empty()) {
            const VariableDependency* cur = stack.top();
            stack.pop();

            const bool alreadySeen = visited.contains(cur);

            if (alreadySeen) {
                continue;
            }

            const auto seenMetaInputs = [&visited](const DependencyEdge* e) {
                return !e->isMetaEdge() || visited.contains(e->src());
            };

            const bool canTraverse = std::ranges::all_of(cur->incoming(), seenMetaInputs);

            if (!canTraverse) {
                continue;
            }

            for (const DependencyEdge* e : cur->outgoing()) {
                const VariableDependency* tgt = e->tgt();
                stack.push(tgt);
            }
            for (const DependencyEdge* e : cur->incoming()) {
                const VariableDependency* src = e->src();
                stack.push(src);
            }

            visited.insert(cur);
            traversal.push_back(cur);
        }
    }
}
