#include "PlanGraphDebug.h"

#include <range/v3/view/enumerate.hpp>

#include "expr/Expr.h"
#include "nodes/AggregateEvalNode.h"
#include "nodes/ChangeNode.h"
#include "nodes/FilterNode.h"
#include "nodes/FuncEvalNode.h"
#include "nodes/GetEntityTypeNode.h"
#include "nodes/GetPropertyNode.h"
#include "nodes/GetPropertyWithNullNode.h"
#include "nodes/JoinNode.h"
#include "nodes/LoadJsonlNode.h"
#include "nodes/OrderByNode.h"
#include "nodes/ProcedureEvalNode.h"
#include "nodes/UnwindNode.h"
#include "nodes/VarNode.h"
#include "nodes/CreateGraphNode.h"
#include "nodes/WriteNode.h"
#include "nodes/ScanNodesByLabelNode.h"
#include "nodes/LoadGraphNode.h"
#include "nodes/LoadGMLNode.h"
#include "nodes/LoadParquetNode.h"
#include "nodes/ConstScanNode.h"
#include "nodes/ConstWriteSourceNode.h"
#include "nodes/ExprEvalNode.h"

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
#include "expr/BinaryExpr.h"
#include "expr/EntityTypeExpr.h"
#include "expr/ExprChain.h"
#include "expr/FunctionInvocationExpr.h"
#include "expr/LiteralExpr.h"
#include "expr/PropertyExpr.h"
#include "expr/StringExpr.h"
#include "expr/SymbolExpr.h"
#include "expr/UnaryExpr.h"
#include "FunctionInvocation.h"
#include "Literal.h"

#include "columns/ColumnIDs.h"
#include "metadata/LabelSet.h"

using namespace db;

namespace rg = ranges;
namespace rv = rg::views;

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

void dumpExpr(std::ostream& out, const Expr* expr) {
    switch (expr->getKind()) {
        case Expr::Kind::BINARY: {
            const auto* bin = static_cast<const BinaryExpr*>(expr);
            const auto op = bin->getOperator();
            const bool isLogical = (op == BinaryOperator::And || op == BinaryOperator::Or);

            if (isLogical) out << "(";
            dumpExpr(out, bin->getLHS());
            out << " " << BinaryOperatorDescription::value(op) << " ";
            dumpExpr(out, bin->getRHS());
            if (isLogical) out << ")";
        } break;

        case Expr::Kind::UNARY: {
            const auto* un = static_cast<const UnaryExpr*>(expr);
            out << UnaryOperatorDescription::value(un->getOperator()) << " ";
            dumpExpr(out, un->getSubExpr());
        } break;

        case Expr::Kind::STRING: {
            const auto* str = static_cast<const StringExpr*>(expr);
            dumpExpr(out, str->getLHS());
            out << " " << StringOperatorDescription::value(str->getStringOperator()) << " ";
            dumpExpr(out, str->getRHS());
        } break;

        case Expr::Kind::SYMBOL: {
            const auto* sym = static_cast<const SymbolExpr*>(expr);
            out << sym->getSymbol()->getName();
        } break;

        case Expr::Kind::PROPERTY: {
            const auto* prop = static_cast<const PropertyExpr*>(expr);
            out << prop->getEntityVarDecl()->getName() << "." << prop->getPropName();
        } break;

        case Expr::Kind::LITERAL: {
            const auto* lit = static_cast<const LiteralExpr*>(expr);
            const Literal* literal = lit->getLiteral();

            switch (literal->getKind()) {
                case Literal::Kind::NULL_LITERAL:
                    out << "NULL";
                    break;
                case Literal::Kind::BOOL:
                    out << (static_cast<const BoolLiteral*>(literal)->getValue() ? "true" : "false");
                    break;
                case Literal::Kind::INTEGER:
                    fmt::print(out, "{}", static_cast<const IntegerLiteral*>(literal)->getValue());
                    break;
                case Literal::Kind::DOUBLE:
                    fmt::print(out, "{}", static_cast<const DoubleLiteral*>(literal)->getValue());
                    break;
                case Literal::Kind::STRING:
                    out << "'" << static_cast<const StringLiteral*>(literal)->getValue() << "'";
                    break;
                case Literal::Kind::EMBEDDING: {
                    const auto data = static_cast<const EmbeddingLiteral*>(literal)->getValue();
                    out << "[";
                    for (size_t i = 0; i < data.size(); i++) {
                        if (i > 0) out << ", ";
                        out << data[i];
                    }
                    out << "]";
                } break;

                case Literal::Kind::LIST: {
                    const auto* data = static_cast<const ListLiteral*>(literal);
                    out << "[";
                    for (bool first {true}; const Expr* item : data->items()) {
                        if (!first) {
                            out << ", ";
                        }
                        dumpExpr(out, item);
                        first = false;
                    }
                    out << "]";
                }
                break;

                default:
                    out << "?";
                    break;
            }
        } break;

        case Expr::Kind::FUNCTION_INVOCATION: {
            const auto* fn = static_cast<const FunctionInvocationExpr*>(expr);
            const auto* sig = fn->getFunctionInvocation()->getSignature();
            out << sig->getFullName() << "(...)";
        } break;

        default:
            out << "?";
            break;
    }
}

void outputPredicate(std::ostream& output, const Predicate* pred) {
    output << "        __predicate__ ";
    dumpExpr(output, pred->getExpr());
    output << "\n";

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

    for (const auto& [i, node] : rv::enumerate(planGraph.nodes())) {
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

            case PlanGraphOpcode::CONST_SCAN: {
                const auto* n = dynamic_cast<ConstScanNode*>(node.get());
                const auto* col = dynamic_cast<const ColumnNodeIDs*>(n->values());
                if (col) {
                    output << "        __values__ [";
                    for (size_t j = 0; j < col->size(); j++) {
                        if (j > 0) output << ", ";
                        fmt::print(output, "{}", (*col)[j].getValue());
                    }
                    output << "]\n";
                }
            } break;

            case PlanGraphOpcode::CONST_WRITE_SOURCE: {
                const auto* n = dynamic_cast<ConstWriteSourceNode*>(node.get());
                const auto* col = dynamic_cast<const ColumnNodeIDs*>(n->nodeIDs());
                if (col) {
                    fmt::print(output, "        __nodeIDs__ n={}\n", col->size());
                }
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

            case PlanGraphOpcode::LOAD_PARQUET: {
                const auto* n = dynamic_cast<LoadParquetNode*>(node.get());
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
                    const std::string_view varName =
                        item->getExpr()->getExprVarDecl()->getName();
                    const std::string_view ord = (type == OrderByType::ASC ? "ASC" : "DESC");
                    output << varName << ", " << ord << '\n';
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

            case PlanGraphOpcode::FILTER_DATAFRAME: {
                const auto* n = dynamic_cast<DataframeFilterNode*>(node.get());
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
                    output << "        __aggregate_func__: " << signature->getFullName() << "\n";
                }
                if (!n->getGroupByKeys().empty()) {
                    output << "        __has grouping keys__: " << n->getGroupByKeys().size() << "\n";
                }
            } break;

            case PlanGraphOpcode::FUNC_EVAL: {
                const auto* n = dynamic_cast<FuncEvalNode*>(node.get());
                for (const auto& func : n->getFuncs()) {
                    const FunctionSignature* signature = func->getFunctionInvocation()->getSignature();
                    output << "        __func__: " << signature->getFullName() << "\n";
                }
            } break;

            case PlanGraphOpcode::WRITE: {
                const auto* n = dynamic_cast<WriteNode*>(node.get());

                size_t j = 0;
                for (const auto& pendingNode : n->pendingNodes()) {
                    output << "        __node__ (" << j;

                    const std::span labels = pendingNode._data->labelConstraints();
                    for (const auto& label : labels) {
                        output << ":" << label << "";
                    }

                    if (!pendingNode._data->exprConstraints().empty()) {
                        output << " {";
                        size_t k = 0;
                        for (const auto& [propName, vt, expr] : pendingNode._data->exprConstraints()) {
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
                for (const auto& delNode : n->toDeleteNodes()) {
                    output << "        __delete_node__ " << delNode->getName() << "\n";
                    j++;
                }

                j = 0;
                for (const auto& delEdge : n->toDeleteEdges()) {
                    output << "        __delete_edge__ " << delEdge->getName() << "\n";
                    j++;
                }

                j = 0;
                for (const auto& nodeUpdate : n->nodeUpdates()) {
                    output << "        __node_update__ " << nodeUpdate._decl->getName()
                           << "." << nodeUpdate._propTypeName << "\n";
                    j++;
                }

                j = 0;
                for (const auto& edge : n->edgeUpdates()) {
                    output << "        __edge_update__ " << edge._decl->getName()
                           << "." << edge._propTypeName << "\n";
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
                output << "        __func__ " << signature->getFullName() << "\n";

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

            case PlanGraphOpcode::EXPR_EVAL: {
                const auto* n = dynamic_cast<ExprEvalNode*>(node.get());
                output << "        __exprs__\n";
                for (const Expr* expr : n->getExprs()) {
                    const VarDecl* var = expr->getExprVarDecl();
                    std::string_view name = "unknown";
                    std::string_view kindDesc = "unknown";
                    if (var) {
                        name = var->getName();
                        const Expr::Kind kind = expr->getKind();
                        kindDesc = ExprKindDescription::value(kind);
                    }

                    const std::string out =  fmt::format("__expr__ ({}): {}", kindDesc, name);
                    output << out << '\n';
                }
            }
            break;

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

            case PlanGraphOpcode::JOIN: {
                const auto* n = dynamic_cast<JoinNode*>(node.get());
                bioassert(n, "Null join node.");

                const std::string_view key1 = n->getFirstJoinKey()->getName();
                const std::string_view key2 = n->getSecondJoinKey()->getName();

                output << "        __key1__: " << key1<< '\n';
                output << "        __key2__: " << key2 << '\n';
            }
            break;

            case PlanGraphOpcode::UNWIND: {
                const auto* n = dynamic_cast<UnwindNode*>(node.get());
                bioassert(n, "Null unwind node.");

                const Expr* arg = n->arg();
                const VarDecl* var = n->var();
                const std::string_view varName = var->getName();

                output << "        ";
                dumpExpr(output, arg);
                output << " AS " << varName << '\n';
            }
            break;

            case PlanGraphOpcode::GET_OUT_EDGES:
            case PlanGraphOpcode::GET_IN_EDGES:
            case PlanGraphOpcode::GET_EDGES:
            case PlanGraphOpcode::GET_EDGE_TARGET:
            case PlanGraphOpcode::PROJECT_RESULTS:
            case PlanGraphOpcode::CARTESIAN_PRODUCT:
            case PlanGraphOpcode::SKIP:
            case PlanGraphOpcode::LIMIT:
            case PlanGraphOpcode::PRODUCE_RESULTS:
            case PlanGraphOpcode::LIST_GRAPH:
            case PlanGraphOpcode::LIST_AVAILABLE_GRAPHS:
            case PlanGraphOpcode::COMMIT:
            case PlanGraphOpcode::S3_CONNECT:
            case PlanGraphOpcode::S3_TRANSFER:
            case PlanGraphOpcode::SHOW_PROCEDURES:
            case PlanGraphOpcode::SHORTEST_PATH:
            case PlanGraphOpcode::LOAD_CSV:
            case PlanGraphOpcode::CREATE_VECTOR_INDEX:
            case PlanGraphOpcode::LOAD_VECTOR:
            case PlanGraphOpcode::LOAD_EMBEDDING:
            case PlanGraphOpcode::VECTOR_SEARCH:
            case PlanGraphOpcode::DELETE_VECTOR_INDEX:
            case PlanGraphOpcode::SHOW_VECTOR_INDEXES:
            case PlanGraphOpcode::LOAD_COMMIT:
            case PlanGraphOpcode::INSTALL_EXTENSION:
            case PlanGraphOpcode::SHOW_EXTENSIONS:
            case PlanGraphOpcode::PATH_EXPLORER:
            case PlanGraphOpcode::CREATE_PROPERTY_INDEX:
            case PlanGraphOpcode::INDEX_LOOKUP:
            case PlanGraphOpcode::DROP_INDEX:
            case PlanGraphOpcode::MERGE_DATAPARTS:
            // No extra info required
            break;

            case PlanGraphOpcode::_SIZE:
            case PlanGraphOpcode::UNKNOWN:
            break;
        }

        output << "    `\"]\n";
    }

    for (const auto& node : planGraph.nodes()) {
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
