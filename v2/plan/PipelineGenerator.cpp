#include "PipelineGenerator.h"

#include <spdlog/fmt/fmt.h>

#include "PipelinePort.h"
#include "PlanGraph.h"
#include "PlanGraphStream.h"
#include "TranslateToken.h"

#include "PipelineBuffer.h"

#include "processors/ScanNodesProcessor.h"
#include "processors/GetOutEdgesProcessor.h"

#include "columns/Block.h"
#include "columns/ColumnIDs.h"

#include "LocalMemory.h"
#include "PlannerException.h"

using namespace db::v2;
using namespace db;

namespace {

/*
template <typename ColumnType>
PipelineOutputPort* addColumnInPort(LocalMemory* mem, PipelineOutputPort* outPort) {
    auto* col = mem->alloc<ColumnType>();
    PipelineBuffer* buffer = outPort->getBuffer();
    buffer->getBlock().addColumn(col);
    return outPort;
}
*/

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

        translateNode(node, stream);

        for (PlanGraphNode* nextNode : node->outputs()) {
            nodeStack.emplace(nextNode, stream);
        }
    }
}

void PipelineGenerator::translateNode(PlanGraphNode* node, PlanGraphStream& stream) {
    switch (node->getOpcode()) {
        case PlanGraphOpcode::VAR:
            // Do nothing
        break;
        case PlanGraphOpcode::SCAN_NODES: {
            ScanNodesProcessor* proc = ScanNodesProcessor::create(_pipeline);
            PipelineOutputPort* outNodes = proc->outNodeIDs();
            stream.set(PlanGraphStream::NodeStream{outNodes});
        }
        break;
        case PlanGraphOpcode::GET_OUT_EDGES: {
            // Require a node stream
            if (!stream.isNodeStream()) {
                throw PlannerException(fmt::format("GET_OUT_EDGES node requires a node stream"));
            }
            GetOutEdgesProcessor* proc = GetOutEdgesProcessor::create(_pipeline);
            PipelineOutputPort* outEdgeIDs = proc->outEdgeIDs();
            PipelineOutputPort* outTargetIDs = proc->outTargetNodes();
            stream.set(PlanGraphStream::EdgeStream{outEdgeIDs, outTargetIDs});
        }
        break;
        case PlanGraphOpcode::GET_EDGE_TARGET: {
            // Require an edge stream
            if (!stream.isEdgeStream()) {
                throw PlannerException(fmt::format("GET_EDGE_TARGET node requires an edge stream"));
            }

            PipelineOutputPort* targetIDs = stream.getEdgeStream().targetIDs;
            stream.set(PlanGraphStream::NodeStream{targetIDs});
        }
        break;
        case PlanGraphOpcode::MATERIALIZE: {
            // Do nothing
        }
        break;
        case PlanGraphOpcode::FILTER_NODE: {
            // Require a node stream
            if (!stream.isNodeStream()) {
                throw PlannerException(fmt::format("FILTER_NODE node requires a node stream"));
            }
        }
        break;
        case PlanGraphOpcode::FILTER_EDGE: {
            // Require an edge stream
            if (!stream.isEdgeStream()) {
                throw PlannerException(fmt::format("FILTER_EDGE node requires an edge stream"));
            }
        }
        break;
        case PlanGraphOpcode::PRODUCE_RESULTS: {
            // Do nothing
        }
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
