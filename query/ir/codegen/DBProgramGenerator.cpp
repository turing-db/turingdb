#include "DBProgramGenerator.h"

#include <algorithm>
#include <unordered_set>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/Interfaces/DataLayoutInterfaces.h"

#include "DBTypes.h"
#include "DBOps.h"

#include "VariableDependency.h"
#include "VariableDependencyGraph.h"
#include "EdgeMetadata.h"
#include "DependencyEdge.h"

#include "BioAssert.h"

using namespace db;

mlir::db::ColumnType DBProgramGenerator::allocColumnType(const VariableDependency*) {
    // TODO Remove in favour of typed columns
    return mlir::db::ColumnType::get(_mlirCtxt);
}

void DBProgramGenerator::registerValue(const VariableDependency* var, mlir::TypedValue<mlir::Type> val) {
    _varMap[var].emplace_back(val);
}

void DBProgramGenerator::addScanNodes(const VariableDependency* var) {
    bioassert(!_varMap.contains(var), "ScanNodes for registered variable");

    const mlir::db::ColumnType col = allocColumnType(var);
    auto scan = _opBuilder->create<mlir::db::ScanNodes>(_opBuilder->getUnknownLoc(), col);

    registerValue(var, scan.getResult());
}

void DBProgramGenerator::addGetOutEdges(const VariableDependency* src, const VariableDependency* edge, const VariableDependency* tgt) {
    bioassert(_varMap.contains(src), "GetOutEdges without source");
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

    std::unordered_set<const VariableDependency*> visited;
    std::deque<const DependencyEdge*> dq;

    for (const VariableDependency& v : vdg.vars()) {
        if (visited.contains(&v)) {
            continue;
        }

        { // FIXME: Temporary guard to prevent starting from meta nodes
            const bool isMeta = std::ranges::any_of(
                v.incoming(), [](const DependencyEdge* e) { return e->isMetaEdge(); });
            bioassert(!isMeta, "Cannot start with meta node");
        }

        { // FIXME: Temporary guard to prevent starting from edges
            const bool isEdge =
                std::ranges::any_of(v.incoming(), [](const DependencyEdge* e) {
                    EdgeMetadata::EdgeType type = e->data().type();
                    return type == EdgeMetadata::EdgeType::GET_OUT_EDGES
                        || type == EdgeMetadata::EdgeType::GET_IN_EDGES;
                });
            bioassert(!isEdge, "Cannot start with edge");
        }

        const auto seenOrMeta = [&visited](const DependencyEdge* e) {
            return !e->isMetaEdge() || visited.contains(e->src());
        };

        const bool canTraverse = std::ranges::all_of(v.incoming(), seenOrMeta);

        if (!canTraverse) {
            continue;
        }

        addScanNodes(&v);

        // dfs from this root
        for (const DependencyEdge* e : v.edges()) {
            dq.push_back(e);
        }

        while (!dq.empty()) {
            const DependencyEdge* e = dq.back();
            dq.pop_back();

            // get out edges -> look for subsequent targets of the out edge
            if (e->data().type() == EdgeMetadata::EdgeType::GET_OUT_EDGES) {
                // @ref other is the edge variable
                const VariableDependency* other = e->src() == &v ? e->tgt() : e->src();

                // TODO: remove this
                bioassert(!visited.contains(other), "Invalid traversal.");

                for (const DependencyEdge* f : other->outgoing()) {
                    if (f->data().type() == EdgeMetadata::EdgeType::GET_EDGE_TGT) {
                        addGetOutEdges(&v, other, f->tgt());
                    }
                }
            }
        }

        // The idea is that we explore the edges of v. if we have a getXedges, we try and
        // create a triple. if we have a getedge{src/tgt} then just translate that with an
        // anonymous edge var
    }
}
