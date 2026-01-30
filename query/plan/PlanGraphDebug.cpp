#include "PlanGraphDebug.h"

#include "nodes/AggregateEvalNode.h"
#include "nodes/ChangeNode.h"
#include "nodes/FilterNode.h"
#include "nodes/FuncEvalNode.h"
#include "nodes/GetEntityTypeNode.h"
#include "nodes/GetPropertyNode.h"
#include "nodes/GetPropertyWithNullNode.h"
#include "nodes/LoadJsonlNode.h"
#include "nodes/OrderByNode.h"
#include "nodes/ProcedureEvalNode.h"
#include "nodes/VarNode.h"
#include "nodes/CreateGraphNode.h"
#include "nodes/WriteNode.h"
#include "nodes/ScanNodesByLabelNode.h"
#include "nodes/LoadGraphNode.h"
#include "nodes/LoadGMLNode.h"
#include "nodes/LoadNeo4jNode.h"

#include "stmt/OrderByItem.h"
#include "views/GraphView.h"
#include "FunctionSignature.h"
#include "YieldClause.h"
#include "YieldItems.h"
#include "PlanGraph.h"
#include "Predicate.h"
#include "Symbol.h"
#include "SymbolChain.h"
#include "decl/PatternData.h"
#include "decl/VarDecl.h"

#include "metadata/LabelSet.h"

using namespace db;

namespace {

void outputDependency(std::ostream& output, const ExprDependencies::VarDependency& dep) {
    if (const auto* expr = dynamic_cast<const EntityTypeExpr*>(dep._expr)) {
        const auto* types = expr->getTypes();

        output << "        __dep__ _" << expr->getEntityVarDecl()->getName() << "_";

        for (const auto& type : *types) {
            output << ":_" << type->getName() << "_\n";
        }
    } else if (const auto* expr = dynamic_cast<const PropertyExpr*>(dep._expr)) {
        output << "        __dep__ _" << expr->getEntityVarDecl()->getName() << "_";

        fmt::println(output, "._{}_ ({})",
                     expr->getPropName(),
                     EvaluatedTypeName::value(expr->getType()));
    }
}

void outputPredicate(std::ostream& output, const Predicate* pred) {
    output << "        __has predicate__" << "\n";

    for (const auto& dep : pred->getDependencies().getVarDeps()) {
        outputDependency(output, dep);
    }
}

}

void PlanGraphDebug::dumpMermaidConfig(std::ostream& output) {
    output << R"(
%%{ init: {"theme": "default",
           "themeVariables": { "wrap": "false" },
           "flowchart": { "curve": "linear",
                          "markdownAutoWrap":"false",
                          "wrappingWidth": "1000" }
           }
}%%
)";
}

void PlanGraphDebug::dumpMermaidContent(std::ostream& output, const GraphView& view, const PlanGraph& planGraph) {
    const auto& metadata = view.metadata();
    const auto& edgeTypeMap = metadata.edgeTypes();
    const auto& labelMap = metadata.labels();

    std::unordered_map<const PlanGraphNode*, size_t> nodeOrder;

    output << "flowchart TD\n";

    for (size_t i = 0; i < planGraph._nodes.size(); i++) {
        const auto& node = planGraph._nodes[i];
        nodeOrder[node.get()] = i;

        // Writing node definition
        output << fmt::format("    {}[\"`\n", i);
        output << fmt::format("        __{}__\n", PlanGraphOpcodeDescription::value(node->getOpcode()));

        switch (node->getOpcode()) {
            case PlanGraphOpcode::VAR: {
                const auto* n = dynamic_cast<VarNode*>(node.get());
                output << fmt::format("        __name__: {}\n", n->getVarDecl()->getName());
            } break;

            case PlanGraphOpcode::SCAN_NODES: {
            } break;

            case PlanGraphOpcode::SCAN_NODES_BY_LABEL: {
                const auto* scanNodesByLabel = dynamic_cast<ScanNodesByLabelNode*>(node.get());
                std::vector<LabelID> labels;
                scanNodesByLabel->getLabelSet().decompose(labels);

                for (const auto& label : labels) {
                    output << "        __label__: " << labelMap.getName(label).value() << "\n";
                }
            }
            break;

            case PlanGraphOpcode::LOAD_GRAPH: {
                const auto* n = dynamic_cast<LoadGraphNode*>(node.get());
                output << "        __graph__: " << n->getGraphName() << "\n";
            } break;

            case PlanGraphOpcode::LOAD_GML: {
                const auto* n = dynamic_cast<LoadGMLNode*>(node.get());
                output << "        __graph__: " << n->getGraphName() << "\n";
                output << "        __filepath__: " << n->getFilePath().get() << "\n";
            } break;

            case PlanGraphOpcode::LOAD_NEO4J: {
                const auto* n = dynamic_cast<LoadNeo4jNode*>(node.get());
                output << "        __graph__: " << n->getGraphName() << "\n";
                output << "        __filepath__: " << n->getFilePath().get() << "\n";
            } break;

            case PlanGraphOpcode::LOAD_JSONL: {
                const auto* n = dynamic_cast<LoadJsonlNode*>(node.get());
                output << "        __graph__: " << n->getGraphName() << "\n";
                output << "        __filepath__: " << n->getFilePath().get() << "\n";
            } break;

            case PlanGraphOpcode::CREATE_GRAPH: {
                const auto* n = dynamic_cast<CreateGraphNode*>(node.get());
                output << "        __graph__: " << n->getGraphName() << "\n";
            } break;

            case PlanGraphOpcode::ORDER_BY: {
                const auto* n = dynamic_cast<OrderByNode*>(node.get());
                for (const auto& item : n->items()) {
                    output << "        __item__: ";
                    const OrderByType type = item->getType();
                    output << (type == OrderByType::ASC ? "ASC\n" : "DESC\n");
                }
            } break;

            case PlanGraphOpcode::FILTER_NODE: {
                const auto* n = dynamic_cast<NodeFilterNode*>(node.get());
                std::vector<LabelID> labels;
                n->getLabelConstraints().decompose(labels);

                for (const auto& label : labels) {
                    output << "        __label__: " << labelMap.getName(label).value() << "\n";
                }

                for (const auto& pred : n->getPredicates()) {
                    outputPredicate(output, pred);
                }
            } break;

            case PlanGraphOpcode::FILTER_EDGE: {
                const auto* e = dynamic_cast<EdgeFilterNode*>(node.get());
                for (const auto& edgeType : e->getEdgeTypeConstraints()) {
                    output << "        __edge_type__: " << edgeTypeMap.getName(edgeType).value() << "\n";
                }

                for (const auto& pred : e->getPredicates()) {
                    outputPredicate(output, pred);
                }
            } break;

            case PlanGraphOpcode::AGGREGATE_EVAL: {
                const auto* n = dynamic_cast<AggregateEvalNode*>(node.get());
                for (const auto& func : n->getFuncs()) {
                    const FunctionSignature* signature = func->getFunctionInvocation()->getSignature();
                    output << "        __aggregate_func__: " << signature->_fullName << "\n";
                }
                if (!n->getGroupByKeys().empty()) {
                    output << "        __has grouping keys__: " << n->getGroupByKeys().size() << "\n";
                }
            } break;

            case PlanGraphOpcode::FUNC_EVAL: {
                const auto* n = dynamic_cast<FuncEvalNode*>(node.get());
                for (const auto& func : n->getFuncs()) {
                    const FunctionSignature* signature = func->getFunctionInvocation()->getSignature();
                    output << "        __func__: " << signature->_fullName << "\n";
                }
            } break;

            case PlanGraphOpcode::WRITE: {
                const auto* n = dynamic_cast<WriteNode*>(node.get());

                size_t j = 0;
                for (const auto& node : n->pendingNodes()) {
                    output << "        __node__ (" << j;

                    const std::span labels = node._data->labelConstraints();
                    for (const auto& label : labels) {
                        output << ":" << label << "";
                    }

                    if (!node._data->exprConstraints().empty()) {
                        output << " {";
                        size_t k = 0;
                        for (const auto& [propName, vt, expr] : node._data->exprConstraints()) {
                            if (k++ > 0) {
                                output << ", ";
                            }

                            output << fmt::format("{} ({}): *expr*",
                                                  propName,
                                                  ValueTypeName::value(vt));
                        }
                        output << " }";
                    }
                    output << ")\n";
                    j++;
                }

                j = 0;
                for (const auto& edge : n->pendingEdges()) {
                    const auto& data = edge._data;

                    output << "        __edge__ ";

                    if (!n->hasPendingNode(edge._src)) {
                        output << "(" << edge._src->getName() << ")";
                    } else {
                        output << "(" << n->getPendingNodeOffset(edge._src) << ")";
                    }

                    output << "-[" << j;                                  // Edge ID
                    output << ":" << data->edgeTypeConstraints().front(); // Edge type

                    if (!data->exprConstraints().empty()) {
                        output << " {";
                        size_t k = 0;
                        for (const auto& [propName, vt, expr] : data->exprConstraints()) {
                            if (k++ > 0) {
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
                    if (!n->hasPendingNode(edge._tgt)) {
                        output << "(" << edge._tgt->getName() << ")\n";
                    } else {
                        output << "(" << n->getPendingNodeOffset(edge._tgt) << ")\n";
                    }
                    j++;
                }

                j = 0;
                for (const auto& node : n->toDeleteNodes()) {
                    output << "        __delete_node__ " << node->getName() << "\n";
                    j++;
                }

                j = 0;
                for (const auto& edge : n->toDeleteEdges()) {
                    output << "        __delete_edge__ " << edge->getName() << "\n";
                    j++;
                }

                j = 0;
                for (const auto& node : n->nodeUpdates()) {
                    output << "        __node_update__ " << node._propTypeName << "\n";
                    j++;
                }

                j = 0;
                for (const auto& edge : n->edgeUpdates()) {
                    output << "        __edge_update__ " << edge._propTypeName << "\n";
                    j++;
                }
            } break;

            case PlanGraphOpcode::GET_PROPERTY: {
                const auto* n = dynamic_cast<GetPropertyNode*>(node.get());
                output << "        __var__ " << n->getEntityVarDecl()->getName() << "\n";
                output << "        __prop__ " << n->getPropName() << "\n";
            } break;

            case PlanGraphOpcode::GET_PROPERTY_WITH_NULL: {
                const auto* n = dynamic_cast<GetPropertyWithNullNode*>(node.get());
                output << "        __var__ " << n->getEntityVarDecl()->getName() << "\n";
                output << "        __prop__ " << n->getPropName() << "\n";
            } break;

            case PlanGraphOpcode::GET_ENTITY_TYPE: {
                const auto* n = dynamic_cast<GetEntityTypeNode*>(node.get());
                const VarDecl* decl = n->getEntityVarDecl();
                output << "        __type__ " << ((decl->getType() == EvaluatedType::NodePattern) ? "node" : "edge") << "\n";
                output << "        __var__ " << decl->getName() << "\n";
            } break;

            case PlanGraphOpcode::PROCEDURE_EVAL: {
                const auto* n = dynamic_cast<ProcedureEvalNode*>(node.get());
                const auto* invocation = n->getFuncExpr()->getFunctionInvocation();
                const auto* signature = invocation->getSignature();
                output << "        __func__ " << signature->_fullName << "\n";

                const auto* yield = n->getYield();
                if (!yield) {
                    break;
                    break;
                }

                const YieldItems* yieldItems = yield->getItems();
                if (!yieldItems) {
                    output << "        __yield__: *all*\n";
                } else {
                    output << "        __yield__\n";
                    for (const auto* item : *yield->getItems()) {
                        output << "        __yield_item__: " << item->getSymbol()->getName() << "\n";
                    }
                }

                const auto* args = invocation->getArguments();
                if (!args || args->empty()) {
                    break;
                }

                output << "        __args__\n";
                for (const auto* arg : *args) {
                    output << "        __arg__: " << (arg->getName().empty() ? "unnamed" : arg->getName()) << "\n";
                }
            } break;

            case PlanGraphOpcode::CHANGE: {
                const auto* n = dynamic_cast<ChangeNode*>(node.get());
                switch (n->getOp()) {
                    case ChangeOp::NEW: {
                        output << "        __op__: new\n";
                    } break;
                    case ChangeOp::SUBMIT: {
                        output << "        __op__: submit\n";
                    } break;
                    case ChangeOp::DELETE: {
                        output << "        __op__: delete\n";
                    } break;
                    case ChangeOp::LIST: {
                        output << "        __op__: list\n";
                    } break;
                }
            } break;

            default: {
            } break;
        }

        output << "    `\"]\n";
    }

    for (const auto& node : planGraph._nodes) {
        // Writing connections
        const size_t a = nodeOrder.at(node.get());

        for (const PlanGraphNode* out : node->outputs()) {
            const size_t b = nodeOrder.at(out);
            output << fmt::format("    {}-->{}\n", a, b);
        }
    }
}

void PlanGraphDebug::dumpMermaid(std::ostream& output, const GraphView& view, const PlanGraph& planGraph) {
    dumpMermaidConfig(output);
    dumpMermaidContent(output, view, planGraph);
}
