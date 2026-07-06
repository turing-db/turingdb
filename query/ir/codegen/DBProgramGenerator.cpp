#include "DBProgramGenerator.h"

#include <algorithm>
#include <unordered_set>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/TypeRange.h"
#include "mlir/IR/ValueRange.h"
#include "mlir/Interfaces/DataLayoutInterfaces.h"

#include "DBOps.h"
#include "DBTypes.h"
#include "StorageTypes.h"

#include "DependencyEdge.h"
#include "EdgeMetadata.h"
#include "VariableDependency.h"
#include "VariableDependencyGraph.h"

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

void DBProgramGenerator::addGetOutEdges(const VariableDependency* src,
                                        const VariableDependency* edge,
                                        const VariableDependency* tgt) {
    bioassert(src, "Null source");
    bioassert(tgt, "Null target");

    bioassert(_varMap.contains(src), "GetOutEdges without source");

    const auto srcs = allocColumnType(mlir::storage::NodeIDType::get(_mlirCtxt));
    const auto etype = allocColumnType(mlir::storage::EdgeTypeIDType::get(_mlirCtxt));
    const auto tgts = allocColumnType(mlir::storage::NodeIDType::get(_mlirCtxt));
    const auto edges = allocColumnType(mlir::storage::EdgeIDType::get(_mlirCtxt));

    const auto input = _varMap[src].back();

    const auto loc = _opBuilder->getUnknownLoc();
    auto goe = _opBuilder->create<mlir::db::GetOutEdges>(
        loc, mlir::TypeRange {srcs, edges, etype, tgts}, mlir::ValueRange {input});

    const mlir::Value newSrcs = goe.getResult(0);
    registerValue(src, newSrcs);
    const mlir::Value newEIDs = goe.getResult(1);
    if (edge) {
        registerValue(edge, newEIDs);
    }
    [[maybe_unused]] const mlir::Value newETypes = goe.getResult(2);
    // FIXME: How do we register the edge types?
    const mlir::Value newTgts = goe.getResult(3);
    registerValue(tgt, newTgts);
}

void DBProgramGenerator::addGetInEdges(const VariableDependency* src,
                                       const VariableDependency* edge,
                                       const VariableDependency* tgt) {
    bioassert(src, "Null source");
    bioassert(tgt, "Null target");

    bioassert(_varMap.contains(src), "GetInEdges without source");

    const auto srcs = allocColumnType(mlir::storage::NodeIDType::get(_mlirCtxt));
    const auto etype = allocColumnType(mlir::storage::EdgeTypeIDType::get(_mlirCtxt));
    const auto tgts = allocColumnType(mlir::storage::NodeIDType::get(_mlirCtxt));
    const auto edges = allocColumnType(mlir::storage::EdgeIDType::get(_mlirCtxt));

    const auto input = _varMap[src].back();

    const auto loc = _opBuilder->getUnknownLoc();
    auto gie = _opBuilder->create<mlir::db::GetInEdges>(
        loc, mlir::TypeRange {srcs, edges, etype, tgts}, mlir::ValueRange {input});

    const mlir::Value newSrcs = gie.getResult(0);
    registerValue(src, newSrcs);
    const mlir::Value newEIDs = gie.getResult(1);
    if (edge) {
        registerValue(edge, newEIDs);
    }
    [[maybe_unused]] const mlir::Value newETypes = gie.getResult(2);
    // FIXME: How do we register the edge types?
    const mlir::Value newTgts = gie.getResult(3);
    registerValue(tgt, newTgts);
}

void DBProgramGenerator::generate(const CypherAST* ast, mlir::ModuleOp* module) {
    _mlirCtxt = module->getContext();
    bioassert(_mlirCtxt, "Null context");

    mlir::OpBuilder builder(module->getBodyRegion());
    _opBuilder = &builder;

    _mlirCtxt->loadDialect<mlir::db::DB>();
    const mlir::Location loc = _opBuilder->getUnknownLoc();

    VariableDependencyGraph vdg;
    vdg.buildFromAST(ast);

    { // Create main
        _opBuilder->setInsertionPointToEnd(module->getBody());
        const mlir::FunctionType funcType = mlir::FunctionType::get(_mlirCtxt, {}, {});
        auto func = _opBuilder->create<mlir::func::FuncOp>(loc, "main", funcType);
        mlir::Block& block = *func.addEntryBlock();
        _opBuilder->setInsertionPointToStart(&block);
    }

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

    // TODO: Use nodes at ends of diameter, find using dijkstra
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
                    addGetOutEdges(src, edge, tgt);
                break;

                case EdgeMetadata::EdgeType::GET_IN_EDGES:
                    addGetInEdges(src, edge, tgt);
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
        }
    }
}
