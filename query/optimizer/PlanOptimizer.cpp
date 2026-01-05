#include "PlanOptimizer.h"

#include "PlanGraph.h"
#include "nodes/ScanNodesNode.h"
#include "nodes/FilterNode.h"
#include "nodes/ScanNodesByLabelNode.h"

using namespace db::v2;

PlanOptimizer::PlanOptimizer(PlanGraph* plan)
    : _plan(plan)
{
}

PlanOptimizer::~PlanOptimizer() {
}

void PlanOptimizer::optimize() {
    // Do some very simple plan rewriting

    rewriteScanByLabels();

    _plan->removeIsolatedNodes();
}

void PlanOptimizer::rewriteScanByLabels() {
    std::vector<PlanGraphNode*> roots;
    _plan->getRoots(roots);

    for (PlanGraphNode* root : roots) {
        // === Check rewrite rule precondition === 
        // We are looking for pairs:
        // [root] ScanNodesNode --> NodeFilterNode (label, no predicates)
        //
        // We don't rewrite if ScanNodesNode has multiple successors
        ScanNodesNode* scanNodes = dynamic_cast<ScanNodesNode*>(root);
        if (!scanNodes) {
            continue;
        } 

        const auto& scanNodesOutputs = scanNodes->outputs();
        if (scanNodesOutputs.size() != 1) {
            continue;
        }

        NodeFilterNode* filterNode = dynamic_cast<NodeFilterNode*>(scanNodesOutputs[0]);
        if (!filterNode) {
            continue;
        }

        // The filter node must have a label constraint and no predicates
        const LabelSet& labelset = filterNode->getLabelConstraints();
        if (labelset.empty() || !filterNode->getPredicates().empty()) {
            continue;
        }
        
        // === Rewrite ===
        
        // Create ScanNodesByLabel
        ScanNodesByLabelNode* scanNodesByLabel = _plan->create<ScanNodesByLabelNode>(labelset);

        // Connect ScanNodesByLabel to the successors of the filter node
        for (PlanGraphNode* filterNodeNext : filterNode->outputs()) {
            scanNodesByLabel->connectOut(filterNodeNext);
        }
        
        scanNodes->clearOutputs();
        filterNode->clearOutputs();
    }
}
