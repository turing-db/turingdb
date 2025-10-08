#include "PlanGraphDebug.h"

#include "decl/PatternData.h"
#include "nodes/FilterNode.h"
#include "nodes/VarNode.h"
#include "nodes/CreateGraphNode.h"

#include "nodes/WriteNode.h"
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

    output << R"(
%%{ init: {"theme": "default",
           "themeVariables": { "wrap": "false" },
           "flowchart": { "curve": "linear",
                          "markdownAutoWrap":"false",
                          "wrappingWidth": "1000" }
           }
}%%
)";
    output << "flowchart TD\n";

    for (const auto& node : planGraph._nodes) {
        // Writing node definition
        output << fmt::format("    {}[\"`\n", fmt::ptr(node.get()));
        output << fmt::format("        __{}__\n\n", PlanGraphOpcodeDescription::value(node->getOpcode()));

        switch (node->getOpcode()) {
            case PlanGraphOpcode::VAR: {
                const auto* n = dynamic_cast<VarNode*>(node.get());
                bioassert(n->getVarDecl());
                output << fmt::format("        __name__: {}\n", n->getVarDecl()->getName());
                output << fmt::format("        __name__: {}\n", n->getVarDecl()->getName());
                output << fmt::format("        __order__: {}\n", n->getDeclOrder());
            } break;
            case PlanGraphOpcode::SCAN_NODES: {
            } break;
            case PlanGraphOpcode::CREATE_GRAPH: {
                const auto* n = dynamic_cast<CreateGraphNode*>(node.get());
                output << "        __graph__: " << n->getGraphName() << "\n";
            } break;

            case PlanGraphOpcode::FILTER_NODE: {
                const auto* n = dynamic_cast<NodeFilterNode*>(node.get());
                std::vector<LabelID> labels;
                n->getLabelConstraints().decompose(labels);

                for (const auto& label : labels) {
                    output << "        __label__: " << labelMap.getName(label).value() << "\n";
                }

                for (const auto& constraint : n->getPropertyConstraints()) {
                    const auto& [var, propType, expr, deps] = *constraint;

                    std::optional name = propTypeMap.getName(propType);
                    if (!name) {
                        name = std::to_string(propType);
                    }

                    output << fmt::format("        __prop__ _{}_: {}\n", var->getVarDecl()->getName(), name.value());

                    for (const auto& dep : deps.getDependencies()) {
                        output << "        __dep__: " << dep._var->getVarDecl()->getName();
                        if (std::holds_alternative<ExprDependencies::LabelDependency>(dep._dep)) {
                            output << " __labels__\n";
                        } else if (const auto* p = std::get_if<ExprDependencies::PropertyDependency>(&dep._dep)) {
                            output << "." << p->_propertyType << "\n";
                        }
                    }
                }

                for (const auto& pred : n->getWherePredicates()) {
                    output << "        __predicate__: " << fmt::ptr(pred) << "\n";

                    for (const auto& dep : pred->getDependencies()) {
                        output << "        __dep__: " << dep._var->getVarDecl()->getName();
                        if (std::holds_alternative<ExprDependencies::LabelDependency>(dep._dep)) {
                            output << ": __labels__\n";
                        } else if (const auto* p = std::get_if<ExprDependencies::PropertyDependency>(&dep._dep)) {
                            output << "." << p->_propertyType << "\n";
                        }
                    }
                }
            } break;

            case PlanGraphOpcode::FILTER_EDGE: {
                const auto* n = dynamic_cast<EdgeFilterNode*>(node.get());
                for (const auto& edgeType : n->getEdgeTypeConstraints()) {
                    output << "        __edge_type__: " << edgeTypeMap.getName(edgeType).value() << "\n";
                }

                for (const auto& constraint : n->getPropertyConstraints()) {
                    const auto& [var, propType, expr, deps] = *constraint;

                    std::optional name = propTypeMap.getName(propType);
                    if (!name) {
                        name = std::to_string(propType);
                    }

                    output << fmt::format("        __prop__ _{}_: {}\n", var->getVarDecl()->getName(), name.value());

                    for (const auto& dep : deps.getDependencies()) {
                        output << "        __dep__: " << dep._var->getVarDecl()->getName();
                        if (std::holds_alternative<ExprDependencies::LabelDependency>(dep._dep)) {
                            output << "__labels__\n";
                        } else if (const auto* p = std::get_if<ExprDependencies::PropertyDependency>(&dep._dep)) {
                            output << "." << p->_propertyType << "\n";
                        }
                    }
                }
            } break;

            case PlanGraphOpcode::WRITE: {
                const auto* n = dynamic_cast<WriteNode*>(node.get());

                size_t i = 0;
                for (const auto& node : n->pendingNodes()) {
                    output << "        __node__ (" << i;

                    std::span labels = node._data->labelConstraints();
                    for (const auto& label : labels) {
                        output << ":" << label << "";
                    }

                    if (!node._data->exprConstraints().empty()) {
                        output << " {";
                        size_t i = 0;
                        for (const auto& [propName, vt, expr] : node._data->exprConstraints()) {
                            if (i++ > 0) {
                                output << ", ";
                            }

                            output << fmt::format("{} ({}): {}",
                                                  propName,
                                                  ValueTypeName::value(vt),
                                                  fmt::ptr(expr));
                        }
                        output << " }";
                    }
                    output << ")\n";
                    i++;
                }

                i = 0;
                for (const auto& edge : n->pendingEdges()) {
                    const auto& data = edge._data;

                    output << "        __edge__ ";

                    if (edge._src.isInput()) {
                        output << "(" << edge._src.asInput()->getName() << ")";
                    } else {
                        output << "(" << edge._src.asPendingNodeOffset() << ")";
                    }

                    output << "-[" << i;                                  // Edge ID
                    output << ":" << data->edgeTypeConstraints().front(); // Edge type

                    if (!data->exprConstraints().empty()) {
                        output << " {";
                        size_t i = 0;
                        for (const auto& [propName, vt, expr] : data->exprConstraints()) {
                            if (i++ > 0) {
                                output << ", ";
                            }

                            output << fmt::format("{} ({}): {}",
                                                  propName,
                                                  ValueTypeName::value(vt),
                                                  fmt::ptr(expr));
                        }
                        output << " }";
                    }

                    output << "]->";
                    if (edge._tgt.isInput()) {
                        output << "(" << edge._tgt.asInput()->getName() << ")\n";
                    } else {
                        output << "(" << edge._tgt.asPendingNodeOffset() << ")\n";
                    }
                    i++;
                }
            } break;

            default: {
            } break;
        }

        output << "    `\"]\n";

        // Writing connections
        for (const PlanGraphNode* out : node->outputs()) {
            output << fmt::format("    {}-->{}\n", fmt::ptr(node.get()), fmt::ptr(out));
        }
    }
}
