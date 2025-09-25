#include "PlanGraphDebug.h"

#include "nodes/FilterNode.h"
#include "nodes/VarNode.h"
#include "nodes/CreateGraphNode.h"

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
        output << fmt::format("    {}[{}] {{\n", fmt::ptr(node.get()), PlanGraphOpcodeDescription::value(node->getOpcode()));

        switch (node->getOpcode()) {
            case PlanGraphOpcode::VAR: {
                const auto* n = dynamic_cast<VarNode*>(node.get());
                bioassert(n->getVarDecl());
                output << fmt::format("        name _{}\n", n->getVarDecl()->getName());
                output << fmt::format("        order _{}\n", n->getDeclOdrer());
            } break;
            case PlanGraphOpcode::SCAN_NODES: {
            } break;
            case PlanGraphOpcode::CREATE_GRAPH: {
                const auto* n = dynamic_cast<CreateGraphNode*>(node.get());
                output << "        graph " << n->getGraphName() << "\n";
            } break;

            case PlanGraphOpcode::FILTER_NODE: {
                const auto* n = dynamic_cast<FilterNodeNode*>(node.get());
                std::vector<LabelID> labels;
                n->getLabelConstraints().decompose(labels);

                for (const auto& label : labels) {
                    output << "        label " << labelMap.getName(label).value() << "\n";
                }

                for (const auto& constraint : n->getPropertyConstraints()) {
                    const auto& [var, propType, expr, deps] = *constraint;

                    std::optional name = propTypeMap.getName(propType);
                    if (!name) {
                        name = std::to_string(propType);
                    }

                    output << fmt::format("        prop {}_{}\n", var->getVarDecl()->getName(), name.value());

                    for (const auto& dep : deps.getDependencies()) {
                        output << "        dep _" << dep._var->getVarDecl()->getName();
                        if (std::holds_alternative<ExprDependencies::LabelDependency>(dep._dep)) {
                            output << "_labels\n";
                        } else if (const auto* p = std::get_if<ExprDependencies::PropertyDependency>(&dep._dep)) {
                            std::optional name = propTypeMap.getName(p->_propTypeID);
                            if (!name) {
                                name = std::to_string(p->_propTypeID);
                            }
                            output << "_" << name.value() << "\n";
                        }
                    }
                }

                for (const auto& pred : n->getWherePredicates()) {
                    output << "        predicate _" << fmt::ptr(pred) << "\n";

                    for (const auto& dep : pred->getDependencies()) {
                        output << "        dep _" << dep._var->getVarDecl()->getName();
                        if (std::holds_alternative<ExprDependencies::LabelDependency>(dep._dep)) {
                            output << "_labels\n";
                        } else if (const auto* p = std::get_if<ExprDependencies::PropertyDependency>(&dep._dep)) {
                            std::optional name = propTypeMap.getName(p->_propTypeID);
                            if (!name) {
                                name = std::to_string(p->_propTypeID);
                            }
                            output << "_" << name.value() << "\n";
                        }
                    }
                }
            } break;

            case PlanGraphOpcode::FILTER_EDGE: {
                const auto* n = dynamic_cast<FilterEdgeNode*>(node.get());
                for (const auto& edgeType : n->getEdgeTypeConstraints()) {
                    output << "        edge_type " << edgeTypeMap.getName(edgeType).value() << "\n";
                }

                for (const auto& constraint : n->getPropertyConstraints()) {
                    const auto& [var, propType, expr, deps] = *constraint;

                    std::optional name = propTypeMap.getName(propType);
                    if (!name) {
                        name = std::to_string(propType);
                    }

                    output << fmt::format("        prop {}_{}\n", var->getVarDecl()->getName(), name.value());

                    for (const auto& dep : deps.getDependencies()) {
                        output << "        dep _" << dep._var->getVarDecl()->getName();
                        if (std::holds_alternative<ExprDependencies::LabelDependency>(dep._dep)) {
                            output << "_labels\n";
                        } else if (const auto* p = std::get_if<ExprDependencies::PropertyDependency>(&dep._dep)) {
                            std::optional name = propTypeMap.getName(p->_propTypeID);
                            if (!name) {
                                name = std::to_string(p->_propTypeID);
                            }
                            output << "_" << name.value() << "\n";
                        }
                    }
                }
            } break;

            default: {
            } break;
        }

        output << "    }\n";

        // Writing connections
        for (const PlanGraphNode* out : node->outputs()) {
            output << fmt::format("    {} ||--o{{ {} : \" \"\n", fmt::ptr(node.get()), fmt::ptr(out));
        }
    }
}
