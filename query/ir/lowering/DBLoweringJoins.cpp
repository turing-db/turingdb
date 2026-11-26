#include "DBLowering.h"

#include <mlir/IR/Location.h>

#include "mlir/IR/Block.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/Types.h"

#include "NLOps.h"

#include "IRException.h"
#include "llvm/Support/Casting.h"

using namespace db;

namespace nl = mlir::nl;

void DBLowering::lowerCrossProduct(mlir::db::CrossProduct product) {
    // The outer factor roots where this op would have - the entry block at top
    // level. The inner factor roots inside the outer factor's innermost loop
    // body, so its loops nest under the outer loop: a nested-loop join where the
    // inner factor re-runs once per outer chunk.
    mlir::Block* const rootBlock = _rootBlock;

    llvm::SmallVector<mlir::Value, 4> outerColumns;
    mlir::Block* const outerBody = lowerFactor(product.getLeftFactor(), rootBlock, outerColumns);

    llvm::SmallVector<mlir::Value, 4> innerColumns;
    mlir::Block* const innerBody = lowerFactor(product.getRightFactor(), outerBody, innerColumns);

    // The cross sits at the deepest point - the inner factor's innermost loop
    // body, where both factors have a chunk bound.
    setInsertionInto(innerBody);

    nl::CrossProduct cross = _builder.create<nl::CrossProduct>(_builder.getUnknownLoc(),
                                                               outerColumns,
                                                               innerColumns);

    // The pairs come out chunk by chunk, so the product drives a loop of its own
    // nested in the inner factor's - the third level of the nest, below the two the
    // factors opened. That binds the db results to the loop variables, carries the
    // limit handle when one governs the product, and leaves the loop body as the
    // innermost one, which is where the consumer (the lowered db.output) goes and
    // where an enclosing factor roots when this product is itself a factor - the
    // three-way MATCH (a), (b), (c).
    buildLoopForSource(cross.getResult(), product.getOperation());
}

void DBLowering::lowerHashJoin(mlir::db::HashJoin join) {
    // Both nests root where this op stands - the entry block at top level, the enclosing
    // factor's innermost loop body inside a product - so they are siblings rather than
    // nested: the built side is read once, not once per chunk of the other.
    mlir::Block* const rootBlock = _rootBlock;

    llvm::SmallVector<mlir::Value, 4> buildColumns;
    mlir::Block* const buildBody = lowerFactor(join.getRightFactor(), rootBlock, buildColumns);
    rowAlignFactorChunks(buildColumns);

    const mlir::Location loc = _builder.getUnknownLoc();

    _builder.setInsertionPointToStart(rootBlock);
    nl::HashJoinBuffer buffer = _builder.create<nl::HashJoinBuffer>(loc,
                                                                    join.getRightKey(),
                                                                    join.getLeftKey());
    const mlir::Value state = buffer.getState();

    setInsertionInto(buildBody);
    _builder.create<nl::HashJoinCollect>(loc, state, buildColumns);

    llvm::SmallVector<mlir::Value, 4> probeColumns;
    mlir::Block* const probeBody = lowerFactor(join.getLeftFactor(), rootBlock, probeColumns);
    rowAlignFactorChunks(probeColumns);

    setInsertionInto(probeBody);

    llvm::SmallVector<mlir::Type, 8> resultTypes;
    for (const mlir::Value column : probeColumns) {
        resultTypes.push_back(column.getType());
    }
    for (const mlir::Value column : buildColumns) {
        resultTypes.push_back(column.getType());
    }

    // The join's chunks are the left factor's yielded columns followed by the right
    // factor's, which is how the probe lays its own out: probed side then built side.
    const nl::IteratorType iteratorType = nl::IteratorType::get(_builder.getContext(), resultTypes);
    nl::HashJoinProbe probe = _builder.create<nl::HashJoinProbe>(loc,
                                                                 iteratorType,
                                                                 state,
                                                                 probeColumns);

    // A key many build rows carry makes more pairs than the probe chunk holds, so the
    // pairs come out chunk by chunk and the probe drives a loop of its own nested in the
    // probe factor's - as a cross product's pairs do. That binds the db results to the
    // loop variables, carries the limit handle when one governs the join, and leaves the
    // loop body as the innermost one, where the consumer goes and where an enclosing
    // factor roots when this join is itself a factor.
    buildLoopForSource(probe.getResult(), join.getOperation());
}

mlir::Block* DBLowering::lowerFactor(mlir::Region& factor,
                                     mlir::Block* rootBlock,
                                     llvm::SmallVectorImpl<mlir::Value>& yieldedChunks) {
    // Root this factor's scans at rootBlock and track its own innermost loop;
    // save and restore the caller's so nested or sibling products are unaffected.
    mlir::Block* const previousRoot = _rootBlock;
    mlir::Block* const previousInnermostLoopBody = _innermostLoopBody;
    const mlir::Value previousInnermostCardinality = _innermostCardinality;
    _rootBlock = rootBlock;
    _innermostLoopBody = nullptr;
    _innermostCardinality = mlir::Value();

    // A factor is one self-contained block ending in a db.yield. Lower each op
    // as at top level; the yield names the columns this factor contributes, so
    // map its operands to the nl chunks they lowered to rather than lowering it.
    for (mlir::Operation& operation : factor.front()) {
        if (mlir::db::Yield yield = mlir::dyn_cast<mlir::db::Yield>(operation)) {
            for (const mlir::Value column : yield.getColumns()) {
                yieldedChunks.push_back(mapValue(column));
            }
        } else {
            lowerOperation(operation);
        }
    }

    // A factor's row count is read from its first yielded column, so a factor
    // that yields none cannot size its side of the product. The db.cross_product
    // verifier rejects this, so reaching it here means unverified IR - a
    // defensive backstop.
    if (yieldedChunks.empty()) {
        throw IRException("cross_product factor yields no column");
    }

    // The block the factor's columns are bound in, which is where the other factor has to
    // root: the innermost loop body of a factor that walks a relation, and the root block
    // itself for one whose columns are a single row bound above every loop - a scalar
    // aggregate, or a constant laid out over the one row it is
    mlir::Block* const factorBody = deepestOwnerBlock(yieldedChunks, rootBlock);

    _rootBlock = previousRoot;
    _innermostLoopBody = previousInnermostLoopBody;
    _innermostCardinality = previousInnermostCardinality;

    return factorBody;
}
