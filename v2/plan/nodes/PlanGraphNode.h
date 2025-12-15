#pragma once

#include "EnumToString.h"

#include <vector>

namespace db::v2 {

enum class PlanGraphOpcode {
    UNKNOWN,
    VAR,
    SCAN_NODES,
    SCAN_NODES_BY_LABEL,
    FILTER_NODE,
    FILTER_EDGE,
    GET_OUT_EDGES,
    GET_IN_EDGES,
    GET_EDGES,
    GET_EDGE_TARGET,
    GET_PROPERTY,
    GET_PROPERTY_WITH_NULL,
    GET_ENTITY_TYPE,
    CREATE_GRAPH,
    PROJECT_RESULTS,
    CARTESIAN_PRODUCT,
    JOIN,
    WRITE,
    FUNC_EVAL,
    AGGREGATE_EVAL,
    PROCEDURE_EVAL,
    ORDER_BY,
    SKIP,
    LIMIT,
    PRODUCE_RESULTS,
    LOAD_GRAPH,
    LOAD_GML,
    LOAD_NEO4J,
    LIST_GRAPH,
    CHANGE,
    _SIZE
};

using PlanGraphOpcodeDescription = EnumToString<PlanGraphOpcode>::Create<
    EnumStringPair<PlanGraphOpcode::UNKNOWN, "UNKNOWN">,
    EnumStringPair<PlanGraphOpcode::VAR, "VAR">,
    EnumStringPair<PlanGraphOpcode::SCAN_NODES, "SCAN_NODES">,
    EnumStringPair<PlanGraphOpcode::SCAN_NODES_BY_LABEL, "SCAN_NODES_BY_LABEL">,
    EnumStringPair<PlanGraphOpcode::FILTER_NODE, "FILTER_NODE">,
    EnumStringPair<PlanGraphOpcode::FILTER_EDGE, "FILTER_EDGE">,
    EnumStringPair<PlanGraphOpcode::GET_OUT_EDGES, "GET_OUT_EDGES">,
    EnumStringPair<PlanGraphOpcode::GET_IN_EDGES, "GET_IN_EDGES">,
    EnumStringPair<PlanGraphOpcode::GET_EDGES, "GET_EDGES">,
    EnumStringPair<PlanGraphOpcode::GET_EDGE_TARGET, "GET_EDGE_TARGET">,
    EnumStringPair<PlanGraphOpcode::GET_PROPERTY, "GET_PROPERTY">,
    EnumStringPair<PlanGraphOpcode::GET_PROPERTY_WITH_NULL, "GET_PROPERTY_WITH_NULL">,
    EnumStringPair<PlanGraphOpcode::GET_ENTITY_TYPE, "GET_ENTITY_TYPE">,
    EnumStringPair<PlanGraphOpcode::CREATE_GRAPH, "CREATE_GRAPH">,
    EnumStringPair<PlanGraphOpcode::PROJECT_RESULTS, "PROJECT_RESULTS">,
    EnumStringPair<PlanGraphOpcode::CARTESIAN_PRODUCT, "CARTESIAN_PRODUCT">,
    EnumStringPair<PlanGraphOpcode::JOIN, "JOIN">,
    EnumStringPair<PlanGraphOpcode::WRITE, "WRITE">,
    EnumStringPair<PlanGraphOpcode::FUNC_EVAL, "FUNC_EVAL">,
    EnumStringPair<PlanGraphOpcode::AGGREGATE_EVAL, "AGGREGATE_EVAL">,
    EnumStringPair<PlanGraphOpcode::PROCEDURE_EVAL, "PROCEDURE_EVAL">,
    EnumStringPair<PlanGraphOpcode::ORDER_BY, "ORDER_BY">,
    EnumStringPair<PlanGraphOpcode::SKIP, "SKIP">,
    EnumStringPair<PlanGraphOpcode::LIMIT, "LIMIT">,
    EnumStringPair<PlanGraphOpcode::PRODUCE_RESULTS, "PRODUCE_RESULTS">,
    EnumStringPair<PlanGraphOpcode::LOAD_GRAPH, "LOAD_GRAPH">,
    EnumStringPair<PlanGraphOpcode::LOAD_GML, "LOAD_GML">,
    EnumStringPair<PlanGraphOpcode::LOAD_NEO4J, "LOAD_NEO4J">,
    EnumStringPair<PlanGraphOpcode::LIST_GRAPH, "LIST_GRAPH">,
    EnumStringPair<PlanGraphOpcode::CHANGE, "CHANGE">>;

class PlanGraphNode {
public:
    using Nodes = std::vector<PlanGraphNode*>;

    virtual ~PlanGraphNode() = default;

    PlanGraphOpcode getOpcode() const { return _opcode; }

    const Nodes& inputs() const { return _inputs; }

    const Nodes& outputs() const { return _outputs; }

    bool isRoot() const { return _inputs.empty(); }

    bool isBinary() const { return _inputs.size() == 2; }

    void connectOut(PlanGraphNode* succ) {
        _outputs.emplace_back(succ);
        succ->_inputs.emplace_back(this);
    }

    void clearInputs() {
        // Remove self from the outputs of our inputs
        for (PlanGraphNode* input : _inputs) {
            auto& outputs = input->_outputs;

            for (auto it = outputs.begin(); it != outputs.end(); ++it) {
                if (*it == this) {
                    outputs.erase(it);
                    break;
                }
            }
        }

        // Clear inputs
        _inputs.clear();
    }

    void clearOutputs() {
        // Remove self from the inputs of our outputs
        for (PlanGraphNode* output : _outputs) {
            auto& inputs = output->_inputs;

            for (auto it = inputs.begin(); it != inputs.end(); ++it) {
                if (*it == this) {
                    inputs.erase(it);
                    break;
                }
            }
        }

        // Clear outputs
        _outputs.clear();
    }

protected:
    explicit PlanGraphNode(PlanGraphOpcode opcode)
        : _opcode(opcode)
    {
    }

private:
    PlanGraphOpcode _opcode {PlanGraphOpcode::UNKNOWN};
    Nodes _inputs;
    Nodes _outputs;
};
}
