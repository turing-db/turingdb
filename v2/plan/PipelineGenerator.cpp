#include "PipelineGenerator.h"

#include <stack>
#include <spdlog/fmt/fmt.h>

#include "PlanGraph.h"
#include "nodes/VarNode.h"
#include "nodes/ScanNodesNode.h"
#include "nodes/GetOutEdgesNode.h"
#include "nodes/GetEdgeTargetNode.h"
#include "nodes/MaterializeNode.h"
#include "nodes/FilterNode.h"
#include "nodes/ProduceResultsNode.h"

#include "PlanGraphStream.h"

#include "PipelinePort.h"

#include "processors/MaterializeData.h"
#include "processors/ScanNodesProcessor.h"
#include "processors/GetOutEdgesProcessor.h"
#include "processors/MaterializeProcessor.h"
#include "processors/LambdaProcessor.h"

#include "columns/Block.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnIndices.h"

#include "decl/VarDecl.h"

#include "LocalMemory.h"
#include "PlannerException.h"

using namespace db::v2;
using namespace db;

namespace {

struct TranslateToken {
    PlanGraphNode* node {nullptr};
    PlanGraphStream stream;
};

using TranslateTokenStack = std::stack<TranslateToken>;

template <typename ColumnType>
NamedColumn* allocColumnInBuffer(PipelineBuffer* buffer,
                                 LocalMemory* mem,
                                 std::string_view name) {
    ColumnType* col = mem->alloc<ColumnType>();
    PipelineBlock& block = buffer->getBlock();
    return NamedColumn::create(block, col, name);
}

}

void PipelineGenerator::generate() {
    TranslateTokenStack nodeStack;

    // Insert root nodes
    std::vector<PlanGraphNode*> rootNodes;
    _graph->getRoots(rootNodes);

    for (const auto& node : rootNodes) {
        nodeStack.emplace(node, PlanGraphStream());
    }

    // Translate nodes in a DFS manner
    while (!nodeStack.empty()) {
        auto [node, stream] = nodeStack.top();
        nodeStack.pop();

        // Create materialize data here, TO IMPROVE LATER
        if (!stream.getMaterializeData()) {
            stream.setMaterializeData(MaterializeData::create(_pipeline, _mem));
        }

        translateNode(node, stream);

        for (PlanGraphNode* nextNode : node->outputs()) {
            nodeStack.emplace(nextNode, stream);
        }
    }
}

void PipelineGenerator::translateNode(PlanGraphNode* node, PlanGraphStream& stream) {
    switch (node->getOpcode()) {
        case PlanGraphOpcode::VAR:
            translateVarNode(static_cast<VarNode*>(node), stream);
        break;

        case PlanGraphOpcode::SCAN_NODES:
            translateScanNodesNode(static_cast<ScanNodesNode*>(node), stream);
        break;

        case PlanGraphOpcode::GET_OUT_EDGES:
            translateGetOutEdgesNode(static_cast<GetOutEdgesNode*>(node), stream);
        break;

        case PlanGraphOpcode::GET_EDGE_TARGET:
            translateGetEdgeTargetNode(static_cast<GetEdgeTargetNode*>(node), stream);
        break;

        case PlanGraphOpcode::MATERIALIZE:
            translateMaterializeNode(static_cast<MaterializeNode*>(node), stream);
        break;

        case PlanGraphOpcode::FILTER_NODE:
            translateFilterNode(static_cast<NodeFilterNode*>(node), stream);
        break;

        case PlanGraphOpcode::FILTER_EDGE:
            translateFilterEdgeNode(static_cast<EdgeFilterNode*>(node), stream);
        break;

        case PlanGraphOpcode::PRODUCE_RESULTS:
            translateProduceResultsNode(static_cast<ProduceResultsNode*>(node), stream);
        break;

        case PlanGraphOpcode::GET_IN_EDGES:
        case PlanGraphOpcode::GET_EDGES: 
        case PlanGraphOpcode::CREATE_NODE:
        case PlanGraphOpcode::CREATE_EDGE:
        case PlanGraphOpcode::CREATE_GRAPH:
        case PlanGraphOpcode::PROJECT_RESULTS:
        case PlanGraphOpcode::CARTESIAN_PRODUCT:
        case PlanGraphOpcode::JOIN:
        case PlanGraphOpcode::_SIZE:
        case PlanGraphOpcode::UNKNOWN:
            throw PlannerException(fmt::format("Unknown plan graph node opcode: {}",
                PlanGraphOpcodeDescription::value(node->getOpcode())));
    }
}

void PipelineGenerator::translateVarNode(VarNode* node,
                                         PlanGraphStream& stream) {
    NamedColumn* prevCol = stream.getInterface()->getColumn();
    prevCol->setName(node->getVarDecl()->getName());
}

void PipelineGenerator::translateScanNodesNode(ScanNodesNode* node,
                                               PlanGraphStream& stream) {
    ScanNodesProcessor* scanNodesProc = ScanNodesProcessor::create(_pipeline);
    stream.setNodes(scanNodesProc->outNodeIDs());
}

void PipelineGenerator::translateGetOutEdgesNode(GetOutEdgesNode* node,
                                                 PlanGraphStream& stream) {
    GetOutEdgesProcessor* getOutEdgesProc = GetOutEdgesProcessor::create(_pipeline);

    connectNodes(stream, getOutEdgesProc->inNodeIDs());
}

void PipelineGenerator::translateGetEdgeTargetNode(GetEdgeTargetNode* node,
                                                   PlanGraphStream& stream) {
}

void PipelineGenerator::translateMaterializeNode(MaterializeNode* node,
                                                 PlanGraphStream& stream) {
}

void PipelineGenerator::translateFilterNode(NodeFilterNode* node,
                                            PlanGraphStream& stream) {
}

void PipelineGenerator::translateFilterEdgeNode(EdgeFilterNode* node,
                                                PlanGraphStream& stream) {
}

void PipelineGenerator::translateProduceResultsNode(ProduceResultsNode* node,
                                                    PlanGraphStream& stream) {
}

std::string_view PipelineGenerator::generateColumnName() {
    
}

void PipelineGenerator::connectNodes(PlanGraphStream& stream,
                                     PipelineInputInterface* targetIface) {
    if (!stream.isNodeStream()) {
        throw PlannerException("Expected a node stream");
    }

    // Connect to target port
    PipelineOutputInterface* srcIface = stream.getInterface();
    PipelineOutputPort* outPort = srcIface->getPort();
    outPort->connectTo(targetIface->getPort());

    // Generate column name
    const std::string_view colName = generateColumnName();

    // Allocate column
    NamedColumn* nodesCol = allocColumnInBuffer<ColumnNodeIDs>(outPort->getBuffer(), _mem, colName);
    srcIface->setColumn(nodesCol);
    targetIface->setColumn(nodesCol);
}
