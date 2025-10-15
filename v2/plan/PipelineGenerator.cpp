#include "PipelineGenerator.h"

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
#include "TranslateToken.h"

#include "PipelinePort.h"

#include "processors/MaterializeData.h"
#include "processors/ScanNodesProcessor.h"
#include "processors/GetOutEdgesProcessor.h"
#include "processors/MaterializeProcessor.h"
#include "processors/LambdaProcessor.h"

#include "columns/ColumnIDs.h"
#include "columns/ColumnIndices.h"

#include "LocalMemory.h"
#include "PlannerException.h"

using namespace db::v2;
using namespace db;

namespace {

template <typename ColumnType>
ColumnType* addColumnToBlock(Block& block, LocalMemory* mem) {
    ColumnType* col = mem->alloc<ColumnType>();
    block.addColumn(col);
    return col;
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

void PipelineGenerator::translateVarNode(VarNode* node, PlanGraphStream& stream) {
    // Do nothing
}

void PipelineGenerator::translateScanNodesNode(ScanNodesNode* node, PlanGraphStream& stream) {
    ScanNodesProcessor* proc = ScanNodesProcessor::create(_pipeline);
    PipelineOutputPort* outNodes = proc->outNodeIDs();

    stream.setStream(PlanGraphStream::NodeStream{outNodes});
}

void PipelineGenerator::translateGetOutEdgesNode(GetOutEdgesNode* node, PlanGraphStream& stream) {
    // Require a node stream
    if (!stream.isNodeStream()) {
        throw PlannerException(fmt::format("GET_OUT_EDGES node requires a node stream"));
    }

    GetOutEdgesProcessor* proc = GetOutEdgesProcessor::create(_pipeline);
    connectNodeStream(stream, proc->inNodeIDs());
    
    PipelineOutputPort* outIndices = proc->outIndices();
    ColumnIndices* indicesCol = addColumnToBlock<ColumnIndices>(outIndices->getBuffer()->getBlock(), _mem);
    stream.getMaterializeData()->createStep(indicesCol);

    PipelineOutputPort* outEdgeIDs = proc->outEdgeIDs();
    PipelineOutputPort* outTargetIDs = proc->outTargetNodes();
    stream.setStream(PlanGraphStream::EdgeStream{outEdgeIDs, outTargetIDs});
}

void PipelineGenerator::translateGetEdgeTargetNode(GetEdgeTargetNode* node, PlanGraphStream& stream) {
    // Require an edge stream
    if (!stream.isEdgeStream()) {
        throw PlannerException(fmt::format("GET_EDGE_TARGET node requires an edge stream"));
    }

    PipelineOutputPort* targetIDs = stream.getEdgeStream().targetIDs;
    stream.setStream(PlanGraphStream::NodeStream{targetIDs});
}

void PipelineGenerator::translateMaterializeNode(MaterializeNode* node, PlanGraphStream& stream) {
    MaterializeData* matData = stream.getMaterializeData();
    MaterializeProcessor* proc = MaterializeProcessor::create(_pipeline, matData);

    if (stream.isNodeStream()) {
        connectNodeStream(stream, proc->input());
    } else if (stream.isEdgeStream()) {
        connectEdgeTargetIDStream(stream, proc->input());
    } else {
        throw PlannerException(fmt::format("MATERIALIZE node requires a node or edge stream"));
    }

    PipelineOutputPort* outNodeIDs = proc->output();
    // TODO: needs to be the post materialize column
    stream.setStream(PlanGraphStream::NodeStream{outNodeIDs});
    stream.closeMaterializeData();
}

void PipelineGenerator::translateFilterNode(NodeFilterNode* node, PlanGraphStream& stream) {
    // Require a node stream
    if (!stream.isNodeStream()) {
        throw PlannerException(fmt::format("FILTER_NODE node requires a node stream"));
    }
}

void PipelineGenerator::translateFilterEdgeNode(EdgeFilterNode* node, PlanGraphStream& stream) {
    // Require an edge stream
    if (!stream.isEdgeStream()) {
        throw PlannerException(fmt::format("FILTER_EDGE node requires an edge stream"));
    }
}

void PipelineGenerator::translateProduceResultsNode(ProduceResultsNode* node, PlanGraphStream& stream) {
    MaterializeProcessor* materializeProc = MaterializeProcessor::create(_pipeline, stream.getMaterializeData());
    const bool isNodeStream = stream.isNodeStream();
    if (isNodeStream) {
        connectNodeStream(stream, materializeProc->input());
    } else {
        connectEdgeTargetIDStream(stream, materializeProc->input());
    }

    auto callback = [this](const Block& block, LambdaProcessor::Operation operation) {
        _callback(block);
    };

    LambdaProcessor* lambdaProc = LambdaProcessor::create(_pipeline, callback);
    materializeProc->output()->connectTo(lambdaProc->input());

    stream.closeMaterializeData();
}

void PipelineGenerator::connectNodeStream(PlanGraphStream& stream, PipelineInputPort* target) {
    // Connect the output port generating the stream to target port
    PlanGraphStream::NodeStream& nodeStream = stream.getNodeStream();
    PipelineOutputPort* nodeIDsPort = nodeStream.nodeIDs;
    nodeIDsPort->connectTo(target);

    // Allocate column in the bufer of the output port generating the stream
    // because it is used
    PipelineBuffer* nodeIDsBuffer = nodeIDsPort->getBuffer();
    ColumnNodeIDs* nodeIDs = addColumnToBlock<ColumnNodeIDs>(nodeIDsBuffer->getBlock(), _mem);

    // Add the column to the materialize data
    stream.getMaterializeData()->addToStep(nodeIDs);
}

void PipelineGenerator::connectEdgeStream(PlanGraphStream& stream,
                                          PipelineInputPort* edgeIDsDest,
                                          PipelineInputPort* targetIDsDest) {
    // Connect the output ports generating the stream to target ports
    PlanGraphStream::EdgeStream& edgeStream = stream.getEdgeStream();
    PipelineOutputPort* edgeIDsPort = edgeStream.edgeIDs;
    PipelineOutputPort* targetIDsPort = edgeStream.targetIDs;
    edgeIDsPort->connectTo(edgeIDsDest);
    targetIDsPort->connectTo(targetIDsDest);

    // Allocate an edgeID and a targetID column in the buffer of the output ports generating the stream
    PipelineBuffer* edgeIDsBuffer = edgeIDsPort->getBuffer();
    PipelineBuffer* targetIDsBuffer = targetIDsPort->getBuffer();
    ColumnEdgeIDs* edgeIDs = addColumnToBlock<ColumnEdgeIDs>(edgeIDsBuffer->getBlock(), _mem);
    ColumnNodeIDs* targetIDs = addColumnToBlock<ColumnNodeIDs>(targetIDsBuffer->getBlock(), _mem);

    // Add columns to the materialize data
    MaterializeData* matData = stream.getMaterializeData();
    matData->addToStep(edgeIDs);
    matData->addToStep(targetIDs);
}

void PipelineGenerator::connectEdgeIDStream(PlanGraphStream& stream, PipelineInputPort* edgeIDsDest) {
    // Connect the edgeID output port of the stream to the target port
    PlanGraphStream::EdgeStream& edgeStream = stream.getEdgeStream();
    PipelineOutputPort* edgeIDsPort = edgeStream.edgeIDs;
    edgeIDsPort->connectTo(edgeIDsDest);

    // Allocate an edgeID column in the buffer of the used output port
    PipelineBuffer* edgeIDsBuffer = edgeIDsPort->getBuffer();
    ColumnEdgeIDs* edgeIDs = addColumnToBlock<ColumnEdgeIDs>(edgeIDsBuffer->getBlock(), _mem);

    // Add the column to the materialize data
    stream.getMaterializeData()->addToStep(edgeIDs);
}

void PipelineGenerator::connectEdgeTargetIDStream(PlanGraphStream& stream, PipelineInputPort* targetIDsDest) {
    // Connect the targetID output port of the stream to the target port
    PlanGraphStream::EdgeStream& edgeStream = stream.getEdgeStream();
    PipelineOutputPort* targetIDsPort = edgeStream.targetIDs;
    targetIDsPort->connectTo(targetIDsDest);

    // Allocate a targetID column in the buffer of the used output port
    PipelineBuffer* targetIDsBuffer = targetIDsPort->getBuffer();
    ColumnNodeIDs* targetIDs = addColumnToBlock<ColumnNodeIDs>(targetIDsBuffer->getBlock(), _mem);
    stream.getMaterializeData()->addToStep(targetIDs);
}
