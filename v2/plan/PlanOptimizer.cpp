#include "PlanOptimizer.h"

#include <iostream>
#include <queue>

#include "PlanGraph.h"
#include "VarDecl.h"

using namespace db;

PlanOptimizer::PlanOptimizer(PlanGraph& planGraph)
    : _planGraph(planGraph)
{
}

PlanOptimizer::~PlanOptimizer() {
}

void PlanOptimizer::optimize() {
    _planGraph.getRoots(_roots);

    removeUnusedVars();
}

void PlanOptimizer::removeUnusedVars() {
    std::queue<PlanGraphNode*> nodeQueue;

    for (PlanGraphNode* root : _roots) {
        nodeQueue.push(root);
    }

    while (!nodeQueue.empty()) {
        PlanGraphNode* node = nodeQueue.front();
        nodeQueue.pop();

        if (node->getOpcode() == PlanGraphOpcode::VAR) {
            auto* varNode = static_cast<VarNode*>(node);
            auto* varDecl = varNode->getVarDecl();
            if (varDecl->isUsed()) {
                std::cout << "Keeping used var: " << varDecl->getName() << std::endl;
            } else {
                std::cout << "Removing unused var: " << varDecl->getName() << std::endl;
            }
        }

        for (PlanGraphNode* out : node->outputs()) {
            nodeQueue.push(out);
        }
    }
}
