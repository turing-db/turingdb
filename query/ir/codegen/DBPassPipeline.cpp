#include "DBPassPipeline.h"

#include <array>

#include "FatalException.h"

using namespace db;

namespace {

using DBPassFactory = std::unique_ptr<mlir::Pass> (*)(const mlir::db::DBPassContext*);

constexpr size_t DB_PASS_COUNT = 35;

// The optimisation pipeline every query runs through, in order. An EXPLAIN prefix
// reporting on a pass walks the same table one pass at a time, which is what keeps the
// pipeline it reports on and the pipeline that runs the same one. Every factory is handed
// the context; only the edge pair proofs, the join's cost model and the metadata count
// read it, the rewrites beside them answering off the IR alone.
const std::array<DBPassFactory, DB_PASS_COUNT> dbPassPipeline = {
    [](const mlir::db::DBPassContext* context) { return mlir::db::createProveDistinctEdges(context); },
    [](const mlir::db::DBPassContext*) { return mlir::db::createSinkMakePath(); },
    [](const mlir::db::DBPassContext*) { return mlir::db::createFuseScanByLabel(); },
    [](const mlir::db::DBPassContext*) { return mlir::db::createPushDownFilters(); },
    [](const mlir::db::DBPassContext*) { return mlir::db::createTrimUnreadColumns(); },
    [](const mlir::db::DBPassContext*) { return mlir::db::createFuseUnwindEquality(); },
    [](const mlir::db::DBPassContext*) { return mlir::db::createFuseScanByNodeIDs(); },
    [](const mlir::db::DBPassContext*) { return mlir::db::createFuseScanByPropertyValue(); },
    [](const mlir::db::DBPassContext*) { return mlir::db::createFuseScanEdges(); },
    [](const mlir::db::DBPassContext*) { return mlir::db::createFuseEdgeTypePredicates(); },
    [](const mlir::db::DBPassContext*) { return mlir::db::createFuseEdgesByType(); },
    [](const mlir::db::DBPassContext*) { return mlir::db::createFuseScanEdgesByType(); },
    [](const mlir::db::DBPassContext*) { return mlir::db::createNarrowEdgeTypeReads(); },
    [](const mlir::db::DBPassContext*) { return mlir::db::createFuseScanOutEdgesByLabel(); },
    [](const mlir::db::DBPassContext*) { return mlir::db::createFuseScanInEdgesByLabel(); },
    [](const mlir::db::DBPassContext*) { return mlir::db::createFuseScanEdgesByEndpointLabel(); },
    [](const mlir::db::DBPassContext*) { return mlir::db::createFuseEdgesByEndpointLabel(); },
    [](const mlir::db::DBPassContext*) { return mlir::db::createRemoveRedundantLabelChecks(); },
    [](const mlir::db::DBPassContext*) { return mlir::db::createFuseExploreEndConstraint(); },
    [](const mlir::db::DBPassContext*) { return mlir::db::createFuseDistinctEdges(); },
    [](const mlir::db::DBPassContext*) { return mlir::db::createFuseExploreHopLabels(); },
    [](const mlir::db::DBPassContext*) { return mlir::db::createFusePathElements(); },
    [](const mlir::db::DBPassContext*) { return mlir::db::createFuseExploreEndNodes(); },
    [](const mlir::db::DBPassContext*) { return mlir::db::createFuseExploreEndFactor(); },
    [](const mlir::db::DBPassContext*) { return mlir::db::createFuseExploreEndSet(); },
    [](const mlir::db::DBPassContext*) { return mlir::db::createPushDownFilters(); },
    [](const mlir::db::DBPassContext*) { return mlir::db::createFuseScanByNodeIDs(); },
    [](const mlir::db::DBPassContext*) { return mlir::db::createFuseScanByPropertyValue(); },
    [](const mlir::db::DBPassContext*) { return mlir::db::createReusePropertyReads(); },
    [](const mlir::db::DBPassContext*) { return mlir::db::createCountPathRows(); },
    [](const mlir::db::DBPassContext*) { return mlir::db::createFuseListFetchNode(); },
    [](const mlir::db::DBPassContext* context) { return mlir::db::createFuseHashJoin(context); },
    [](const mlir::db::DBPassContext*) { return mlir::db::createTrimUnreadColumns(); },
    [](const mlir::db::DBPassContext*) { return mlir::db::createFuseExploreDistinctEnds(); },
    [](const mlir::db::DBPassContext* context) { return mlir::db::createCountFromMetadata(context); },
};

}

DBPassPipeline::DBPassPipeline(mlir::MLIRContext* context)
    : _passManager(context)
{
    _passManager.enableVerifier(false);

    for (const DBPassFactory factory : dbPassPipeline) {
        _passManager.addPass(factory(&_passContext));
    }
}

DBPassPipeline::~DBPassPipeline() {
}

void DBPassPipeline::run(mlir::ModuleOp module, const mlir::db::DBPassContext& passContext) {
    _passContext = passContext;

    mlir::LogicalResult result = mlir::failure();
    try {
        result = _passManager.run(module);
    } catch (...) {
        _passContext = mlir::db::DBPassContext {};
        throw;
    }

    _passContext = mlir::db::DBPassContext {};

    if (mlir::failed(result)) {
        throw FatalException("DB pass pipeline failed");
    }
}

void DBPassPipeline::fillPassNames(std::vector<std::string_view>& passNames) {
    const mlir::db::DBPassContext passContext;

    for (const DBPassFactory factory : dbPassPipeline) {
        const std::unique_ptr<mlir::Pass> pass = factory(&passContext);
        const llvm::StringRef passName = pass->getArgument();

        passNames.push_back(std::string_view(passName.data(), passName.size()));
    }
}

std::unique_ptr<mlir::Pass> DBPassPipeline::createPass(size_t passIndex, const mlir::db::DBPassContext* passContext) {
    return dbPassPipeline[passIndex](passContext);
}
