#include "DBProgramGenerator.h"

#include <algorithm>
#include <unordered_set>

#include "StorageTypes.h"
#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/TypeRange.h"
#include "mlir/IR/ValueRange.h"
#include "mlir/Interfaces/DataLayoutInterfaces.h"

#include "DBTypes.h"
#include "DBOps.h"

#include "VariableDependency.h"
#include "VariableDependencyGraph.h"
#include "EdgeMetadata.h"
#include "DependencyEdge.h"

#include "BioAssert.h"

using namespace db;

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

void DBProgramGenerator::addGetOutEdges(const VariableDependency* src, const VariableDependency* edge, const VariableDependency* tgt) {
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

void DBProgramGenerator::generate(const CypherAST* ast, mlir::ModuleOp* module) {
    _mlirCtxt = module->getContext();
    bioassert(_mlirCtxt, "Null context");

    mlir::OpBuilder builder(module->getBodyRegion());
    _opBuilder = &builder;

    _mlirCtxt->loadDialect<mlir::db::DB>();
    const mlir::Location loc = _opBuilder->getUnknownLoc();

    VariableDependencyGraph vdg;
    vdg.buildFromAST(ast);

    if (vdg.empty()) {
        return;
    }

    { // Create main
        _opBuilder->setInsertionPointToEnd(module->getBody());
        const mlir::FunctionType funcType = mlir::FunctionType::get(_mlirCtxt, {}, {});
        auto func = _opBuilder->create<mlir::func::FuncOp>(loc, "main", funcType);
        mlir::Block& block = *func.addEntryBlock();
        _opBuilder->setInsertionPointToStart(&block);
    }

    std::unordered_set<const VariableDependency*> defined;
    std::deque<const DependencyEdge*> dq;

    for (const VariableDependency& v : vdg.vars()) {
        { // FIXME: Temporary guard to prevent starting from meta nodes
            const bool isMeta = std::ranges::any_of(
                v.incoming(), [](const DependencyEdge* e) { return e->isMetaEdge(); });
            bioassert(!isMeta, "Cannot start with meta node");
        }

        { // FIXME: Temporary guard to prevent starting from edges
            [[maybe_unused]] const bool isEdge =
                std::ranges::any_of(v.incoming(), [](const DependencyEdge* e) {
                    EdgeMetadata::EdgeType type = e->data().type();
                    return type == EdgeMetadata::EdgeType::GET_OUT_EDGES
                        || type == EdgeMetadata::EdgeType::GET_IN_EDGES;
                });
            // bioassert(!isEdge, "Cannot start with edge");
        }

        const auto seenOrMeta = [&defined](const DependencyEdge* e) {
            return !e->isMetaEdge() || defined.contains(e->src());
        };

        const bool canTraverse = std::ranges::all_of(v.incoming(), seenOrMeta);

        if (!canTraverse) {
            continue;
        }

        // This variable is already defined, do not create a scan for it
        if (!defined.contains(&v)) {
            addScanNodes(&v);
            defined.insert(&v);
        }

        // dfs from this root
        for (const DependencyEdge* e : v.edges()) {
            dq.push_back(e);
        }

        while (!dq.empty()) {
            const DependencyEdge* e = dq.back();
            dq.pop_back();

            const VariableDependency* other = e->src() == &v ? e->tgt() : e->src();
            if (defined.contains(other)) {
                continue;
            }

            // get out edges -> look ahead 1 edge for subsequent targets of the out edge
            if (e->data().type() == EdgeMetadata::EdgeType::GET_OUT_EDGES) {
                const VariableDependency* edgeVar = e->src() == &v ? e->tgt() : e->src();

                // if we already defined this edge, nothing to do
                if (defined.contains(edgeVar)) {
                    continue;
                }

                bioassert(edgeVar->outgoing().size() == 1,
                          "Edge variable had multiple outgoing edges");

                const DependencyEdge* outgoing = edgeVar->outgoing().front();

                if (outgoing->data().type() == EdgeMetadata::EdgeType::GET_EDGE_TGT) {
                    const VariableDependency* tgt = outgoing->tgt();
                    addGetOutEdges(&v, edgeVar, tgt);
                    defined.insert(tgt);
                }

                defined.insert(edgeVar);
            }

            // get edge target -> edge var must be anonymous, just get src and tgts
            if (e->data().type() == EdgeMetadata::EdgeType::GET_EDGE_TGT) {
                // Neither variable is already defined: either src or tgt == v, which
                // was defined this cycle, other var is checked and skipped by
                // `defined.contains(other)` on queue loop entry.
                const VariableDependency* src = e->src();
                const VariableDependency* tgt = e->tgt();

                const VariableDependency* edge = nullptr; // Edge variable is unused

                addGetOutEdges(src, edge, tgt);
                defined.insert(src); // @ref v is either @ref src or @ref tgt, so
                defined.insert(tgt); // will be reinserted to defined, but idempotently
            }
        }

        // The idea is that we explore the edges of v. if we have a getXedges, we try and
        // create a triple. if we have a getedge{src/tgt} then just translate that with an
        // anonymous edge var. It amounts to a DFS with 1-lookahead
    }
}
