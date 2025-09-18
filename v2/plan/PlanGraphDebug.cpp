#include "PlanGraphDebug.h"

#include "EvalExprNode.h"
#include "GetNodeLabelSetNode.h"
#include "GetPropertyNode.h"
#include "PlannerException.h"
#include "PropertyMapExprNode.h"
#include "views/GraphView.h"

#include "PlanGraph.h"

#include "decl/VarDecl.h"

#include "metadata/LabelSet.h"

#include "BioAssert.h"

using namespace db;
using namespace db::v2;

void PlanGraphDebug::dumpMermaid(std::ostream& output, const GraphView& view, const PlanGraph& planGraph) {
    const auto& metadata = view.metadata();
    const auto& edgeTypeMap = metadata.edgeTypes();
    const auto& labelMap = metadata.labels();
    const auto& propTypeMap = metadata.propTypes();

    output << "---\n";
    output << "config:\n";
    output << "  layout: dagre\n";
    output << "---\n";
    output << "erDiagram\n";

    for (const auto& node : planGraph._nodes) {
        // Writing node definition
        output << fmt::format("    {} {{\n", fmt::ptr(node.get()));
        output << fmt::format("        opcode {}\n", PlanGraphOpcodeDescription::value(node->getOpcode()));

        switch (node->getOpcode()) {
            case PlanGraphOpcode::VAR: {
                const auto* n = dynamic_cast<VarNode*>(node.get());
                bioassert(n->getVarDecl());
                output << fmt::format("        name _{}\n", n->getVarDecl()->getName());
            } break;
            case PlanGraphOpcode::SCAN_NODES: {
            } break;
            case PlanGraphOpcode::SCAN_NODES_BY_LABEL: {
                const auto* n = dynamic_cast<ScanNodesByLabelNode*>(node.get());
                bioassert(n->getLabelSet());
                std::vector<LabelID> labels;
                n->getLabelSet()->decompose(labels);
                for (const auto& label : labels) {
                    output << fmt::format("        label {}\n", labelMap.getName(label).value());
                }
            } break;
            case PlanGraphOpcode::FILTER_NODE_LABEL: {
                const auto* n = dynamic_cast<FilterNodeLabelNode*>(node.get());
                bioassert(n->getLabelSet());
                std::vector<LabelID> labels;
                n->getLabelSet()->decompose(labels);
                for (const auto& label : labels) {
                    output << "        label " << labelMap.getName(label).value() << "\n";
                }
            } break;
            case PlanGraphOpcode::FILTER_EDGE_TYPE: {
                const auto* n = dynamic_cast<const FilterEdgeTypeNode*>(node.get());
                output << "        edge_type " << edgeTypeMap.getName(n->getEdgeTypeID()).value() << "\n";
            } break;
            case PlanGraphOpcode::CREATE_GRAPH: {
                const auto* n = dynamic_cast<CreateGraphNode*>(node.get());
                bioassert(n->getGraphName());
                output << "        graph " << n->getGraphName() << "\n";
            } break;

            case PlanGraphOpcode::PROPERTY_MAP_EXPR: {
                const auto* n = dynamic_cast<PropertyMapExprNode*>(node.get());
                for (const auto& [propType, expr] : n->getExprs()) {
                    std::optional name = propTypeMap.getName(propType);
                    if (!name) {
                        name = std::to_string(propType);
                    }

                    output << "        prop " << name.value() << "\n";
                }
            }

            case PlanGraphOpcode::UNKNOWN:
            case PlanGraphOpcode::FILTER:
            case PlanGraphOpcode::FILTER_NODE_EXPR:
            case PlanGraphOpcode::FILTER_EDGE_EXPR:
            case PlanGraphOpcode::GET_NODE_LABEL_SET:
            case PlanGraphOpcode::GET_PROPERTY:
            case PlanGraphOpcode::EVAL_EXPR:
            case PlanGraphOpcode::GET_EDGES:
            case PlanGraphOpcode::GET_OUT_EDGES:
            case PlanGraphOpcode::GET_IN_EDGES:
            case PlanGraphOpcode::GET_EDGE_TARGET:
            case PlanGraphOpcode::CREATE_NODE:
            case PlanGraphOpcode::CREATE_EDGE: {
            } break;

            case PlanGraphOpcode::_SIZE: {
                throw PlannerException("Fatal error: unknown opcode");
            } break;
        }

        output << "    }\n";

        // Writing connections
        for (const PlanGraphNode* out : node->outputs()) {
            output << fmt::format("    {} ||--o{{ {} : _\n", fmt::ptr(node.get()), fmt::ptr(out));
        }
    }
}
