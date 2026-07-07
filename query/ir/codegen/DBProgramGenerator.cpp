#include "DBProgramGenerator.h"

#include <algorithm>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/TypeRange.h"
#include "mlir/IR/Value.h"
#include "mlir/IR/ValueRange.h"
#include "mlir/Interfaces/DataLayoutInterfaces.h"
#include "llvm/ADT/SmallVector.h"

#include "DBOps.h"
#include "DBTypes.h"
#include "StorageTypes.h"

#include "DependencyEdge.h"
#include "EdgeMetadata.h"
#include "VariableDependency.h"
#include "VariableDependencyGraph.h"

#include "CypherAST.h"
#include "Projection.h"
#include "QueryCommand.h"
#include "SinglePartQuery.h"
#include "decl/VarDecl.h"
#include "expr/Expr.h"
#include "stmt/ReturnStmt.h"

#include "StringHashMap.h"

#include "BioAssert.h"
#include "FatalException.h"
#include "TuringException.h"

using namespace db;

namespace {

bool producesEdgeVar(const DependencyEdge* e) {
    const EdgeMetadata::EdgeType producedType = e->data().type();
    const bool getOut = producedType == EdgeMetadata::EdgeType::GET_OUT_EDGES;
    const bool getIn = producedType == EdgeMetadata::EdgeType::GET_IN_EDGES;
    return getOut || getIn;
}

bool producesNodeVar(const DependencyEdge* e) {
    const EdgeMetadata::EdgeType producedType = e->data().type();
    const bool getTgt = producedType == EdgeMetadata::EdgeType::GET_EDGE_TGT;
    const bool getSrc = producedType == EdgeMetadata::EdgeType::GET_EDGE_SRC;
    return getTgt || getSrc;
}

}

mlir::db::ColumnType DBProgramGenerator::allocColumnType(mlir::Type type) {
    return mlir::db::ColumnType::get(_mlirCtxt, type);
}

void DBProgramGenerator::registerValue(const VariableDependency* var, mlir::TypedValue<mlir::Type> val) {
    _varMap[var].emplace_back(val);
}

void DBProgramGenerator::addScanNodes(const VariableDependency* var) {
    bioassert(!_varMap.contains(var), "ScanNodes for registered variable");

    const auto col = allocColumnType(mlir::storage::NodeIDType::get(_mlirCtxt));
    auto scan = _opBuilder->create<mlir::db::ScanNodes>(_opBuilder->getUnknownLoc(), col);

    registerValue(var, scan.getResult());
}

template<typename EdgeOp>
void DBProgramGenerator::addEdgeTraversal(const VariableDependency* src,
                                          const VariableDependency* edge,
                                          const VariableDependency* tgt,
                                          const std::vector<const VariableDependency*>& carrySet) {
    static_assert(std::is_same_v<EdgeOp, mlir::db::GetOutEdges>
                      or std::is_same_v<EdgeOp, mlir::db::GetInEdges>, "Invalid op");

    bioassert(src, "Null source");
    bioassert(tgt, "Null target");

    bioassert(_varMap.contains(src), "Edge traversal without source");

    const auto srcs = allocColumnType(mlir::storage::NodeIDType::get(_mlirCtxt));
    const auto eids = allocColumnType(mlir::storage::EdgeIDType::get(_mlirCtxt));
    const auto etypes = allocColumnType(mlir::storage::EdgeTypeIDType::get(_mlirCtxt));
    const auto tgts = allocColumnType(mlir::storage::NodeIDType::get(_mlirCtxt));

    const mlir::Value input = _varMap[src].back();

    llvm::SmallVector<const VariableDependency*> carried;
    llvm::SmallVector<mlir::Value> operands {input};
    llvm::SmallVector<mlir::Type> results {srcs, eids, etypes, tgts};
    for (const VariableDependency* var : carrySet) {
        // source variable is expliclty filtered by the edge op
        if (var == src) {
            continue;
        }

        const mlir::Value column = _varMap[var].back();
        carried.push_back(var);
        operands.push_back(column);
        results.push_back(column.getType());
    }

    const auto loc = _opBuilder->getUnknownLoc();
    auto op = _opBuilder->create<EdgeOp>(loc, results, operands);

    const mlir::Value newSrcs = op.getResult(0);
    const mlir::Value newEdges = op.getResult(1);
    [[maybe_unused]] const mlir::Value newEdgeTypes = op.getResult(2);
    const mlir::Value newTgts = op.getResult(3);

    registerValue(src, newSrcs);
    registerValue(edge, newEdges);
    // FIXME: How do we register the edge types?
    registerValue(tgt, newTgts);

    // Register the new values of the carry set, appearing starting from index 4 in the
    // result range
    constexpr size_t GET_X_EDGES_RES_SIZE = 4;
    for (size_t i = 0; i < carried.size(); i++) {
        const size_t resultIndex = GET_X_EDGES_RES_SIZE + i;
        registerValue(carried[i], op.getResult(resultIndex));
    }
}

void DBProgramGenerator::generate(const CypherAST* ast, mlir::ModuleOp* module) {
    _mlirCtxt = module->getContext();
    bioassert(_mlirCtxt, "Null context");

    mlir::OpBuilder builder(module->getBodyRegion());
    _opBuilder = &builder;

    _mlirCtxt->loadDialect<mlir::db::DB>();
    const mlir::Location uloc = _opBuilder->getUnknownLoc();

    { // Create main
        _opBuilder->setInsertionPointToEnd(module->getBody());
        const mlir::FunctionType funcType = mlir::FunctionType::get(_mlirCtxt, {}, {});
        auto func = _opBuilder->create<mlir::func::FuncOp>(uloc, "main", funcType);
        mlir::Block& block = *func.addEntryBlock();
        _opBuilder->setInsertionPointToStart(&block);
    }

    generateTraversal(ast);
    generateOutput(ast);

    _opBuilder->create<mlir::func::ReturnOp>(uloc);
}

void DBProgramGenerator::generateTraversal(const CypherAST* ast) {
    VariableDependencyGraph vdg;
    vdg.buildFromAST(ast);

    if (vdg.empty()) {
        return;
    }

    std::unordered_set<const VariableDependency*> defined;

    struct Frame {
        const VariableDependency* _var {nullptr};
        const DependencyEdge* _predEdge {nullptr};
        const DependencyEdge* _predPredEdge {nullptr};
    };

    std::vector<Frame> stack;

    // Forms the "carried set" for each connected component
    std::vector<const VariableDependency*> carriedSet;

    // TODO: Use nodes at ends of diameter
    for (const VariableDependency& root : vdg.vars()) {
        if (defined.contains(&root)) {
            continue;
        }

        const auto isEdgeTgtEdgeVar = [](const DependencyEdge* e) -> bool {
            return e->data().type() == EdgeMetadata::EdgeType::GET_OUT_EDGES
                || e->data().type() == EdgeMetadata::EdgeType::GET_IN_EDGES;
        };
        const auto isEdgeTgtMetaVar = [](const DependencyEdge* e) -> bool {
            return e->isMetaEdge();
        };

        // A valid root is a non-meta Cypher variable which is a node
        const bool validRoot =
            std::ranges::none_of(root.incoming(), [&](const DependencyEdge* e) {
                return isEdgeTgtEdgeVar(e) || isEdgeTgtMetaVar(e);
            });

        if (!validRoot) {
            continue;
        }

        addScanNodes(&root);
        defined.insert(&root);

        carriedSet.clear();

        // DFS from this root
        stack.emplace_back(&root, nullptr);
        while (!stack.empty()) {
            const auto [var, pred, predPred] = stack.back();
            stack.pop_back();

            const auto seenOrMeta = [&defined](const DependencyEdge* e) {
                return !e->isMetaEdge() || defined.contains(e->src());
            };
            const bool canTraverse = std::ranges::all_of(var->incoming(), seenOrMeta);

            // If we cannot traverse now, we will find another path to this node
            if (!canTraverse) {
                continue;
            }

            // Have we found a (source, edge, target) triple yet on this traversal?
            const bool haveTriple = pred && predPred;

            for (const DependencyEdge* e : var->edges()) {
                const VariableDependency* other = e->src() == var ? e->tgt() : e->src();
                if (defined.contains(other)) {
                    continue;
                }

                if (haveTriple) {
                    // We have discovered a full (src, edge, tgt) triple, set the next
                    // elements on the stack to only have (src, edge) and await tgt
                    stack.emplace_back(other, e, nullptr);
                } else {
                    // We have not yet discovered a full (src, edge, tgt) triple, but the
                    // next elements on stack will have such a triple (with @ref pred)
                    stack.emplace_back(other, e, pred);
                }
            }

            // Only translate when we have a full triple
            if (!haveTriple) {
                defined.insert(var); // Mark as defined to avoid retraversal
                continue;
            }

            // The order we encountered the nodes may not be source, edge, target, it may
            // be target, edge, source. Determine a definitive order, irrespective of
            // traversal
            const DependencyEdge* edgeVarProd = producesEdgeVar(pred) ? pred : predPred;
            const DependencyEdge* nodeVarProd = producesNodeVar(pred) ? pred : predPred;
            bioassert(producesEdgeVar(edgeVarProd), "No edge producer");
            bioassert(producesNodeVar(nodeVarProd), "No node producer");

            // Only one of either source or target should be defined. Determine which end
            // of the triple is defined
            const bool edgeSrcDefined = defined.contains(edgeVarProd->src());
            const bool nodeTgtDefined = defined.contains(nodeVarProd->tgt());
            bioassert(edgeSrcDefined ^ nodeTgtDefined, "Ambiguous definition");
            bioassert(edgeSrcDefined || nodeTgtDefined, "No defined start");

            VariableDependency* src = nullptr;
            VariableDependency* edge = nullptr;
            VariableDependency* tgt = nullptr;

            // Orientate the operation such that the source operand is defined
            if (edgeSrcDefined) {
                src = edgeVarProd->src();
                edge = edgeVarProd->tgt();
                tgt = nodeVarProd->tgt();
            } else /* (nodeTgtDefined) */ {
                src = nodeVarProd->tgt();
                edge = nodeVarProd->src();
                tgt = edgeVarProd->src();
            }

            const EdgeMetadata::EdgeType edgeType = edgeVarProd->data().type();
            switch (edgeType) {
                case EdgeMetadata::EdgeType::GET_OUT_EDGES:
                    addGetOutEdges(src, edge, tgt, carriedSet);
                break;

                case EdgeMetadata::EdgeType::GET_IN_EDGES:
                    addGetInEdges(src, edge, tgt, carriedSet);
                break;

                case EdgeMetadata::EdgeType::MERGE:
                    throw TuringException("MERGE edges not yet supported.");
                break;

                case EdgeMetadata::EdgeType::GET_EDGES:
                    throw TuringException("Undirected edges not yet supported.");
                break;

                case EdgeMetadata::EdgeType::GET_EDGE_TGT:
                case EdgeMetadata::EdgeType::GET_EDGE_SRC:
                    throw FatalException(fmt::format("Attempted to translate {}",
                                                     EdgeTypeName::value(edgeType)));
                break;

                case EdgeMetadata::EdgeType::_SIZE:
                    throw FatalException("Attempted to translate invalid edge.");
                break;
            }

            defined.insert(src);
            defined.insert(edge);
            defined.insert(tgt);

            carriedSet.push_back(src);
            carriedSet.push_back(edge);
            carriedSet.push_back(tgt);
        }
    }
}

void DBProgramGenerator::generateOutput(const CypherAST* ast) {
    const CypherAST::QueryCommands& queries = ast->queries();
    if (queries.size() != 1) {
        throw TuringException("Multiple queries not yet supported.");
    };

    const QueryCommand* query = queries.front();

    const SinglePartQuery* sglPart = dynamic_cast<const SinglePartQuery*>(query);
    if (!sglPart) {
        throw TuringException("Non-single part queries are not yet supported.");
    }

    const ReturnStmt* rtn = sglPart->getReturnStmt();
    const Projection* proj = rtn->getProjection();

    const bool all = proj->isReturningAll();
    // FIXME: Detect the unused MLIR vars and return those
    bioassert(!all, "Returning all is not yet supported.");

    const Projection::Items& returned = proj->items();

    std::unordered_map<std::string_view, mlir::Value> finalIdentities;
    for (auto& [cypherVar, mlirCol] : _varMap) {
        const std::string_view varName = cypherVar->getName();

        bioassert(not mlirCol.empty(), "No definitions for {}", varName);
        const mlir::Value finalValue = mlirCol.back();

        finalIdentities[varName] = finalValue;
    }

    const auto lookup = [&](auto&& item) -> mlir::Value {
        using Type = std::remove_cvref_t<decltype(item)>;

        const VarDecl* var = nullptr;
        if constexpr (std::is_same_v<Type, Expr* >) {
            var = item->getExprVarDecl();
        } else {
            var = item;
        }
        bioassert(var, "Could not determine return variable.");

        const std::string_view name = var->getName();
        const auto findIt = finalIdentities.find(name);
        bioassert(findIt != end(finalIdentities), "Return item {} not found.", name);

        const mlir::Value mlirCol = findIt->second;
        return mlirCol;
    };

    llvm::SmallVector<mlir::Value> outputted;
    for (const Projection::ReturnItem item : returned) {
        const mlir::Value itemCol = std::visit(lookup, item);
        outputted.push_back(itemCol);
    }

    const mlir::Location loc = _opBuilder->getUnknownLoc();
    _opBuilder->create<mlir::db::Output>(loc, mlir::ValueRange{outputted});
}
