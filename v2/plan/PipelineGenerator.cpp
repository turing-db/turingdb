#include "PipelineGenerator.h"

#include <stack>

#include "PlanGraph.h"

#include "processors/ScanNodesProcessor.h"
#include "processors/GetOutEdgesProcessor.h"

#include "columns/ColumnIDs.h"

#include "LocalMemory.h"
#include "PlannerException.h"

using namespace db::v2;
using namespace db;

namespace {

template <typename ColumnType>
ColumnType* addColumnInBuffer(LocalMemory* mem, PipelineBuffer* buffer) {
    auto* col = mem->alloc<ColumnType>();
    buffer->getBlock().addColumn(col);
    return col;
}

}

void PipelineGenerator::generate() {
    std::stack<PlanGraphNode*> nodeStack;

    // Insert root nodes
    std::vector<PlanGraphNode*> rootNodes;
    _graph->getRoots(rootNodes);
    for (const auto& node : rootNodes) {
        nodeStack.push(node);
        node->getGenerationState().setDiscovered();
    }

    // Translate nodes in a DFS manner
    while (!nodeStack.empty()) {
        PlanGraphNode* node = nodeStack.top();

        PlanGraphNode::GenerationState& nodeState = node->getGenerationState();
        if (nodeState.isTranslated()) {
            // The node is already translated
            // so it means that we are coming back to the node on the DFS backtrack path
            // All its outputs have been translated already
            nodeStack.pop();
            continue;
        } else {
            // First time we encounter this node
            // Translate the node into a pipeline processor
            translateNode(node);
            nodeState.setTranslated();
        }

        for (PlanGraphNode* nextNode : node->outputs()) {
            PlanGraphNode::GenerationState& nextNodeState = nextNode->getGenerationState();
            if (!nextNodeState.isDiscovered()) {
                nodeStack.push(nextNode);
                nextNodeState.setDiscovered();
            }
        }
    }
}

void PipelineGenerator::translateNode(PlanGraphNode* node) {
    switch (node->getOpcode()) {
        case PlanGraphOpcode::VAR: {
            // Do nothing
        }
        break;

        case PlanGraphOpcode::SCAN_NODES:
            translateScanNodes(node);
        break;

        case PlanGraphOpcode::GET_OUT_EDGES:
            translateGetOutEdges(node);
        break;
        
        case PlanGraphOpcode::FILTER_NODE:
        case PlanGraphOpcode::FILTER_EDGE:
        case PlanGraphOpcode::GET_IN_EDGES:
        case PlanGraphOpcode::GET_EDGES:
        case PlanGraphOpcode::GET_EDGE_TARGET:
        case PlanGraphOpcode::CREATE_NODE:
        case PlanGraphOpcode::CREATE_EDGE:
        case PlanGraphOpcode::CREATE_GRAPH:
        case PlanGraphOpcode::PROJECT_RESULTS:
        case PlanGraphOpcode::CARTESIAN_PRODUCT:
        case PlanGraphOpcode::JOIN:
        case PlanGraphOpcode::MATERIALIZE:
        case PlanGraphOpcode::PRODUCE_RESULTS:
        default:
            throw PlannerException("Unsupported plan graph opcode: "
                + std::string(PlanGraphOpcodeDescription::value(node->getOpcode())));
        break;
    }
}

void PipelineGenerator::translateScanNodes(PlanGraphNode* node) {
    ScanNodesProcessor* scanNodes = ScanNodesProcessor::create(_pipeline);
    addColumnInBuffer<ColumnNodeIDs>(_mem, scanNodes->outNodeIDs()->getBuffer());
}

void PipelineGenerator::translateGetOutEdges(PlanGraphNode* node) {
    //GetOutEdgesProcessor* getOutEdges = GetOutEdgesProcessor::create(_pipeline);
}
