#include "PlanGraphDebug.h"

#include "nodes/FilterNode.h"
#include "nodes/VarNode.h"
#include "nodes/CreateGraphNode.h"

#include "PlannerException.h"
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
    output << "  layout: hierarchical\n";
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
            case PlanGraphOpcode::CREATE_GRAPH: {
                const auto* n = dynamic_cast<CreateGraphNode*>(node.get());
                bioassert(n->getGraphName());
                output << "        graph " << n->getGraphName() << "\n";
            } break;

            case PlanGraphOpcode::FILTER_NODE: {
                const auto* n = dynamic_cast<FilterNodeNode*>(node.get());
                bioassert(n->getLabelConstraints());
                std::vector<LabelID> labels;
                n->getLabelConstraints().decompose(labels);

                for (const auto& label : labels) {
                    output << "        label " << labelMap.getName(label).value() << "\n";
                }

                for (const auto& [propType, expr, op] : n->getPropertyConstraints()) {
                    std::optional name = propTypeMap.getName(propType);
                    if (!name) {
                        name = std::to_string(propType);
                    }

                    output << "        prop " << name.value() << "\n";
                }
            } break;

            case PlanGraphOpcode::FILTER_EDGE: {
                const auto* n = dynamic_cast<FilterEdgeNode*>(node.get());
                bioassert(n->getEdgeTypeConstraints());

                for (const auto& edgeType : n->getEdgeTypeConstraints()) {
                    output << "        edge_type " << edgeTypeMap.getName(edgeType).value() << "\n";
                }

                for (const auto& [propType, expr, op] : n->getPropertyConstraints()) {
                    std::optional name = propTypeMap.getName(propType);
                    if (!name) {
                        name = std::to_string(propType);
                    }

                    output << "        prop " << name.value() << "\n";
                }
            } break;

            case PlanGraphOpcode::UNKNOWN:
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
            output << fmt::format("    {} ||--o{{ {} : \" \" \n", fmt::ptr(node.get()), fmt::ptr(out));
        }
    }
}
