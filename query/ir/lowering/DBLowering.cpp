#include "DBLowering.h"

#include <optional>

#include "mlir/IR/Block.h"
#include "mlir/IR/Verifier.h"

#include "NLOps.h"

#include "views/GraphView.h"
#include "metadata/PropertyType.h"

#include "IRException.h"

using namespace db;

namespace nl = mlir::nl;
namespace storage = mlir::storage;

namespace {

// Map a stored property value type to the MLIR element type baked into the
// nullable value chunk. The element only has to round-trip back to this value
// type during translation, so each kind takes a distinct builtin.
mlir::Type valueTypeToElementType(mlir::OpBuilder& builder, ValueType valueType) {
    switch (valueType) {
        case ValueType::Int64:
            return builder.getIntegerType(64);
        break;

        case ValueType::UInt64:
            return builder.getIntegerType(64, /*isSigned=*/false);
        break;

        case ValueType::Double:
            return builder.getF64Type();
        break;

        case ValueType::Bool:
            return builder.getI1Type();
        break;

        case ValueType::String:
            return storage::StringType::get(builder.getContext());
        break;

        case ValueType::Embedding:
            return storage::EmbeddingType::get(builder.getContext());
        break;

        case ValueType::Invalid:
        case ValueType::_SIZE:
            throw IRException("Invalid property value type");
        break;
    }

    throw IRException("Unhandled property value type");
}

}

DBLowering::DBLowering(mlir::MLIRContext* context, const GraphView* view)
    : _builder(context),
    _view(view)
{
}

DBLowering::~DBLowering() {
}

mlir::func::FuncOp DBLowering::lower(mlir::func::FuncOp dbFunction, mlir::ModuleOp module) {
    // Check that we didn't failed MLIR verifier
    if (mlir::failed(mlir::verify(dbFunction))) {
        throw IRException("db function failed MLIR verification");
    }

    mlir::Region& dbBody = dbFunction->getRegion(0);
    if (!dbBody.hasOneBlock()) {
        throw IRException("DBLowering expects a db function with a single block");
    }

    mlir::MLIRContext* context = _builder.getContext();
    const mlir::Location loc = _builder.getUnknownLoc();

    // Create nl target function
    _builder.setInsertionPointToEnd(module.getBody());
    const auto functionType = mlir::FunctionType::get(context, {}, {});
    auto nlFunction = _builder.create<mlir::func::FuncOp>(loc, dbFunction.getSymName(), functionType);
    _entryBlock = nlFunction.addEntryBlock();

    // Create the ReturnOp of the target function right away
    _builder.setInsertionPointToStart(_entryBlock);
    _builder.create<mlir::func::ReturnOp>(loc);

    // Lower each operation of the db function. Top-level scans root their loop
    // in the entry block; a cross product retargets the root per factor.
    _valueMap.clear();
    _propertyTypes.clear();
    _rootBlock = _entryBlock;
    _innermostLoopBody = nullptr;
    _limitHandles.clear();
    _loopLimitHandle.clear();

    // Pre-scan for db.limits before any loop is built: nl.for's limit operand is
    // fixed at build time, so each handle must exist first to be threaded in, and
    // which loops a handle attaches to must be known up front.
    llvm::SmallVector<mlir::db::Limit, 2> limits;
    dbFunction.walk([&](mlir::db::Limit limit) {
        limits.push_back(limit);
    });

    // Hoist one nl.limit handle per db.limit to the top of the entry block, where
    // each dominates the loops, the update and the truncate that read it. The
    // reset scope is function scope (uncorrelated); a correlated limit would hoist
    // into its enclosing loop body instead - future work.
    if (!limits.empty()) {
        _builder.setInsertionPointToStart(_entryBlock);
        for (mlir::db::Limit limit : limits) {
            nl::Limit limitOp = _builder.create<nl::Limit>(loc, limit.getCount());
            _limitHandles[limit.getOperation()] = limitOp.getState();
        }
    }

    // Assign each limit's handle to the loops that produce its columns and their
    // enclosing nest, so only those loops early-exit (consumer loops downstream of
    // the truncate fan out freely). The first limit, in program order, to claim a
    // shared producer wins, so a loop never needs to carry two handles.
    for (mlir::db::Limit limit : limits) {
        const mlir::Value handle = _limitHandles[limit.getOperation()];
        for (const mlir::Value column : limit.getColumns()) {
            assignProducerLoops(column, handle);
        }
    }

    for (mlir::Operation& operation : dbBody.front()) {
        lowerOperation(operation);
    }

    // Peephole: a terminal LIMIT lowers to an nl.limit_truncate whose only
    // consumer is the nl.output right after it. Fold that pair into a single
    // limit-bearing nl.output, which emits the budgeted prefix off the handle
    // instead of copying it - the copy-free path for a LIMIT that feeds the sink.
    foldTruncatesIntoOutputs(nlFunction);

    // Run MLIR verifier on the nlFunction
    if (mlir::failed(mlir::verify(nlFunction))) {
        throw IRException("DBLowering produced an invalid nl function");
    }

    return nlFunction;
}

void DBLowering::lowerOperation(mlir::Operation& operation) {
    if (mlir::db::ScanNodes scanNodes = mlir::dyn_cast<mlir::db::ScanNodes>(operation)) {
        lowerScanNodes(scanNodes);
    } else if (mlir::db::GetOutEdges getOutEdges = mlir::dyn_cast<mlir::db::GetOutEdges>(operation)) {
        lowerGetOutEdges(getOutEdges);
    } else if (mlir::db::GetInEdges getInEdges = mlir::dyn_cast<mlir::db::GetInEdges>(operation)) {
        lowerGetInEdges(getInEdges);
    } else if (mlir::db::GetNodeProperties getNodeProperties = mlir::dyn_cast<mlir::db::GetNodeProperties>(operation)) {
        lowerGetNodeProperties(getNodeProperties);
    } else if (mlir::db::GetEdgeProperties getEdgeProperties = mlir::dyn_cast<mlir::db::GetEdgeProperties>(operation)) {
        lowerGetEdgeProperties(getEdgeProperties);
    } else if (mlir::db::CrossProduct crossProduct = mlir::dyn_cast<mlir::db::CrossProduct>(operation)) {
        lowerCrossProduct(crossProduct);
    } else if (mlir::db::Limit limit = mlir::dyn_cast<mlir::db::Limit>(operation)) {
        lowerLimit(limit);
    } else if (mlir::db::Output output = mlir::dyn_cast<mlir::db::Output>(operation)) {
        lowerOutput(output);
    } else if (mlir::isa<mlir::func::ReturnOp>(operation)) {
        // We already added a ReturnOp to the nl function
    } else {
        throw IRException("DBLowering cannot lower operation '"
                          + operation.getName().getStringRef().str() + "'");
    }
}

void DBLowering::lowerScanNodes(mlir::db::ScanNodes scanNodes) {
    // A scan reads no column, so its loop sits at the top of the current root
    // block: the function entry at top level, or - inside a cross product - the
    // outer factor's innermost loop body, so the inner factor nests under it.
    setInsertionInto(_rootBlock);

    nl::ScanNodes nodes = _builder.create<nl::ScanNodes>(_builder.getUnknownLoc());
    buildLoopForSource(nodes.getResult(), scanNodes.getOperation());
}

void DBLowering::lowerGetOutEdges(mlir::db::GetOutEdges getOutEdges) {
    // Map the input node column and the carry set to the nl chunks they lowered
    // to. The fetch nests in the loop that binds its input chunk.
    const mlir::Value inputChunk = mapValue(getOutEdges.getInputNodes());

    llvm::SmallVector<mlir::Value, 4> carriedChunks;
    for (const mlir::Value carriedColumn : getOutEdges.getColumnsToFilter()) {
        carriedChunks.push_back(mapValue(carriedColumn));
    }

    setInsertionInto(ownerBlock(inputChunk));

    // The result iterator type - the four fixed edge chunks plus one per
    // carried chunk - is inferred from the operands.
    nl::GetOutEdges edges = _builder.create<nl::GetOutEdges>(_builder.getUnknownLoc(), inputChunk, carriedChunks);
    buildLoopForSource(edges.getResult(), getOutEdges.getOperation());
}

void DBLowering::lowerGetInEdges(mlir::db::GetInEdges getInEdges) {
    // The predecessor counterpart of lowerGetOutEdges: same shape, reverse
    // direction. Map the input node column and the carry set to the nl chunks
    // they lowered to. The fetch nests in the loop that binds its input chunk.
    const mlir::Value inputChunk = mapValue(getInEdges.getInputNodes());

    llvm::SmallVector<mlir::Value, 4> carriedChunks;
    for (const mlir::Value carriedColumn : getInEdges.getColumnsToFilter()) {
        carriedChunks.push_back(mapValue(carriedColumn));
    }

    setInsertionInto(ownerBlock(inputChunk));

    // The result iterator type - the four fixed edge chunks plus one per
    // carried chunk - is inferred from the operands.
    nl::GetInEdges edges = _builder.create<nl::GetInEdges>(_builder.getUnknownLoc(), inputChunk, carriedChunks);
    buildLoopForSource(edges.getResult(), getInEdges.getOperation());
}

void DBLowering::lowerGetNodeProperties(mlir::db::GetNodeProperties getNodeProperties) {
    const mlir::Value inputChunk = mapValue(getNodeProperties.getInputNodes());
    const llvm::StringRef property = getNodeProperties.getProperty();

    // Resolve the name once, hoisted above the loops, and bake the value type.
    const mlir::Value handle = getOrCreatePropertyTypeHandle(property);
    const mlir::Type valueChunkType = propertyValueChunkType(property);

    // A property read maps the input chunk in place, one value per node, so the
    // fetch nests in the loop that binds that chunk - it opens no loop of its own.
    setInsertionInto(ownerBlock(inputChunk));

    nl::GetNodeProperties fetch = _builder.create<nl::GetNodeProperties>(_builder.getUnknownLoc(),
                                                                         valueChunkType,
                                                                         inputChunk,
                                                                         handle);
    _valueMap[getNodeProperties.getResult()] = fetch.getValues();
}

void DBLowering::lowerGetEdgeProperties(mlir::db::GetEdgeProperties getEdgeProperties) {
    const mlir::Value inputChunk = mapValue(getEdgeProperties.getInputEdges());
    const llvm::StringRef property = getEdgeProperties.getProperty();

    const mlir::Value handle = getOrCreatePropertyTypeHandle(property);
    const mlir::Type valueChunkType = propertyValueChunkType(property);

    setInsertionInto(ownerBlock(inputChunk));

    nl::GetEdgeProperties fetch = _builder.create<nl::GetEdgeProperties>(_builder.getUnknownLoc(),
                                                                         valueChunkType,
                                                                         inputChunk,
                                                                         handle);
    _valueMap[getEdgeProperties.getResult()] = fetch.getValues();
}

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
    // body, where both factors have a chunk bound - just before whatever
    // consumes the product (the lowered db.output).
    setInsertionInto(innerBody);

    // Null when no limit governs this product (built in full); otherwise the
    // handle whose budget caps the build, so the cross lays out only the prefix
    // the limit can emit this step.
    const mlir::Value limitHandle = _loopLimitHandle.lookup(product.getOperation());
    nl::CrossProduct cross = _builder.create<nl::CrossProduct>(_builder.getUnknownLoc(),
                                                               outerColumns,
                                                               innerColumns,
                                                               limitHandle);

    // The product's results are the outer factor's yielded columns followed by
    // the inner's, the same order nl.cross_product lays out its results.
    const mlir::ResultRange dbResults = product.getResults();
    const mlir::ResultRange crossResults = cross.getResults();
    for (size_t resultIndex = 0; resultIndex < dbResults.size(); resultIndex++) {
        _valueMap[dbResults[resultIndex]] = crossResults[resultIndex];
    }
}

mlir::Block* DBLowering::lowerFactor(mlir::Region& factor,
                                     mlir::Block* rootBlock,
                                     llvm::SmallVectorImpl<mlir::Value>& yieldedChunks) {
    // Root this factor's scans at rootBlock and track its own innermost loop;
    // save and restore the caller's so nested or sibling products are unaffected.
    mlir::Block* const previousRoot = _rootBlock;
    mlir::Block* const previousInnermostLoopBody = _innermostLoopBody;
    _rootBlock = rootBlock;
    _innermostLoopBody = nullptr;

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

    if (!_innermostLoopBody) {
        throw IRException("cross_product factor opened no loop to iterate");
    }

    // A factor's row count is read from its first yielded column, so a factor
    // that yields none cannot size its side of the product. The db.cross_product
    // verifier rejects this, so reaching it here means unverified IR - a
    // defensive backstop.
    if (yieldedChunks.empty()) {
        throw IRException("cross_product factor yields no column");
    }

    mlir::Block* const innermostBody = _innermostLoopBody;
    _rootBlock = previousRoot;
    _innermostLoopBody = previousInnermostLoopBody;

    return innermostBody;
}

mlir::Value DBLowering::getOrCreatePropertyTypeHandle(llvm::StringRef propertyName) {
    const auto existing = _propertyTypes.find(propertyName);
    if (existing != _propertyTypes.end()) {
        return existing->second;
    }

    // The handle reads no chunk, so it sits at the very top of the entry block,
    // above every loop, where it dominates all the fetches that use it.
    _builder.setInsertionPointToStart(_entryBlock);

    nl::GetPropertyType handleOp = _builder.create<nl::GetPropertyType>(_builder.getUnknownLoc(),
                                                                        _builder.getStringAttr(propertyName));
    const mlir::Value handle = handleOp.getResult();
    _propertyTypes[propertyName] = handle;

    return handle;
}

mlir::Type DBLowering::propertyValueChunkType(llvm::StringRef propertyName) {
    if (!_view) {
        throw IRException("Lowering a property fetch needs a graph to resolve the type of '" + propertyName.str() + "'");
    }

    const std::optional<PropertyType> propertyType = _view->metadata().propTypes().get(propertyName);
    if (!propertyType) {
        throw IRException("Unknown property '" + propertyName.str() + "'");
    }

    const mlir::Type elementType = valueTypeToElementType(_builder, propertyType->_valueType);
    storage::NullableType nullableType = storage::NullableType::get(_builder.getContext(), elementType);

    return nl::ChunkType::get(_builder.getContext(), nullableType);
}

void DBLowering::lowerLimit(mlir::db::Limit limit) {
    // The nl chunks the limited columns lowered to; these are what the truncate
    // copies, and the consumer reads the cut copies.
    llvm::SmallVector<mlir::Value, 4> chunks;
    for (const mlir::Value column : limit.getColumns()) {
        chunks.push_back(mapValue(column));
    }

    // Limit::verify rejects an empty db.limit, so reaching it here means
    // unverified IR - a defensive backstop, as in lowerFactor for cross products.
    if (chunks.empty()) {
        throw IRException("db.limit requires at least one column");
    }

    const mlir::Location loc = _builder.getUnknownLoc();
    const mlir::Value handle = _limitHandles.lookup(limit.getOperation());

    // The representative is the first limited column, in the innermost producing
    // loop body (post-cross-product if there is one), so its row count is what
    // this step charges and the truncate copies.
    const mlir::Value representative = chunks.front();
    setInsertionInto(ownerBlock(representative));

    // Charge this step's rows, then copy the first emitThisStep rows of every
    // limited column into fresh chunks, just before the consumer (nl.output when
    // unchained, the inner sub-pipeline when chained).
    _builder.create<nl::LimitUpdate>(loc, handle, representative);
    nl::LimitTruncate truncate = _builder.create<nl::LimitTruncate>(loc, handle, chunks);

    // Map db.limit's results to the truncated chunks, so its consumer reads the
    // cut copies rather than the full producer chunks.
    const mlir::ResultRange dbResults = limit.getResults();
    const mlir::ResultRange truncatedChunks = truncate.getResults();
    for (size_t resultIndex = 0; resultIndex < dbResults.size(); resultIndex++) {
        _valueMap[dbResults[resultIndex]] = truncatedChunks[resultIndex];
    }
}

void DBLowering::assignProducerLoops(mlir::Value column, mlir::Value handle) {
    mlir::Operation* const definingOp = column.getDefiningOp();
    if (!definingOp) {
        // A cross-product factor's loop variable is a block argument with no
        // defining op; its producing loop is reached through the factor's yield
        // in the cross-product branch below, not from here.
        return;
    }

    const bool opensLoop = mlir::isa<mlir::db::ScanNodes, mlir::db::GetOutEdges, mlir::db::GetInEdges>(definingOp);
    const bool isCrossProduct = mlir::isa<mlir::db::CrossProduct>(definingOp);

    // The first limit, in program order, to claim a producer wins, so a loop
    // shared by two limits' nests carries the outer one and never two handles.
    if ((opensLoop || isCrossProduct) && !_loopLimitHandle.count(definingOp)) {
        _loopLimitHandle[definingOp] = handle;
    }

    if (isCrossProduct) {
        // A cross product takes no column operands - its factors are regions - so
        // recurse through each factor's db.yield operands to reach the factor
        // scans/edge loops that produce the crossed columns.
        mlir::db::CrossProduct cross = mlir::cast<mlir::db::CrossProduct>(definingOp);
        mlir::Region* const factors[] = {&cross.getLeftFactor(), &cross.getRightFactor()};
        for (mlir::Region* const factor : factors) {
            mlir::Operation* const yield = factor->front().getTerminator();
            for (const mlir::Value yielded : yield->getOperands()) {
                assignProducerLoops(yielded, handle);
            }
        }
    } else {
        // A non-loop producer (a property fetch) is traversed but not assigned -
        // it opens no loop - so its input chunk's loop is still reached.
        for (const mlir::Value operand : definingOp->getOperands()) {
            assignProducerLoops(operand, handle);
        }
    }
}

void DBLowering::foldTruncatesIntoOutputs(mlir::func::FuncOp nlFunction) {
    // Collect the foldable truncates first; erasing ops mid-walk is unsafe.
    llvm::SmallVector<nl::LimitTruncate, 4> foldable;

    nlFunction.walk([&](nl::LimitTruncate truncate) {
        const mlir::ResultRange results = truncate.getResults();

        // Foldable only if a single nl.output is the sole consumer of every
        // truncated column. Read the direction as "one shared output user => the
        // copy can be dropped", not the reverse: the loop tests that precondition,
        // gathering the shared user, and bails if any result has more than one use
        // or a different user - so erasing the truncate would leave nothing dangling.
        nl::Output output;
        for (const mlir::Value result : results) {
            if (!result.hasOneUse()) {
                return;
            }

            nl::Output user = mlir::dyn_cast<nl::Output>(*result.user_begin());
            if (!user || (output && output != user)) {
                return;
            }
            output = user;
        }

        if (!output || output.getLimit()) {
            return;
        }

        // The output must consume exactly the truncate's results - all of them, in
        // the same order, and nothing else. The fold rebuilds the output over the
        // truncate's *input* columns in the truncate's order, so that swap only
        // preserves what the output emits when the two lists line up one-for-one:
        //   truncate (%a,%b)->(%ta,%tb) ; output(%ta,%tb)  folds to  output(%a,%b) limit %h
        //   output(%ta, %unrelated)  does not fold: %unrelated is no truncate result, it would be dropped
        //   output(%tb, %ta)         does not fold: rebuilt as output(%a,%b), it would swap the projection
        const mlir::OperandRange outputColumns = output.getColumns();
        if (outputColumns.size() != results.size()) {
            return;
        }

        for (size_t columnIndex = 0; columnIndex < results.size(); columnIndex++) {
            if (outputColumns[columnIndex] != results[columnIndex]) {
                return;
            }
        }

        foldable.push_back(truncate);
    });

    for (nl::LimitTruncate truncate : foldable) {
        nl::Output output = mlir::cast<nl::Output>(*truncate.getResult(0).user_begin());

        // Re-emit the output over the untruncated inputs, carrying the handle, so
        // it streams the emitThisStep prefix off the counter instead of a copy.
        // The preceding nl.limit_update still sets that count. Drop the old output
        // and the now-unused truncate.
        _builder.setInsertionPoint(output);
        _builder.create<nl::Output>(output.getLoc(), truncate.getColumns(), truncate.getState());

        output.erase();
        truncate.erase();
    }
}

void DBLowering::lowerOutput(mlir::db::Output output) {
    llvm::SmallVector<mlir::Value, 4> columns;
    for (const mlir::Value column : output.getColumns()) {
        columns.push_back(mapValue(column));
    }

    if (columns.empty()) {
        throw IRException("db.output requires at least one column");
    }

    // nl.output is emitted limit-oblivious: when a db.limit governs these columns,
    // the columns mapped here are the truncated chunks (lowerLimit remapped
    // db.limit's results to the truncate's), so the chunk's own row count is the
    // budget-capped count. foldTruncatesIntoOutputs later rewrites the terminal
    // case - where the truncate feeds only this output - into nl.output ... limit,
    // dropping the copy.
    setInsertionInto(ownerBlock(columns.front()));
    _builder.create<nl::Output>(_builder.getUnknownLoc(), columns, mlir::Value());
}

void DBLowering::buildLoopForSource(mlir::Value iterator, mlir::Operation* dbOp) {
    // The handle this loop carries, or null when it produces no limited column -
    // giving the iterator-only builder's unbounded loop. A producing loop carries
    // its limit's handle so the break unwinds the producing nest; a consumer loop
    // (lowered after the limit) is never in the map and stays unbounded.
    const mlir::Value limitHandle = _loopLimitHandle.lookup(dbOp);
    nl::For forLoop = _builder.create<nl::For>(_builder.getUnknownLoc(), iterator, limitHandle);

    // The loop binds one variable per chunk the iterator produces, in the same
    // order as the db op's result columns: a scan binds its single node chunk;
    // an edge fetch binds sources, edge IDs, edge type IDs, targets, then one
    // filtered chunk per carried column. Recording db result -> loop variable
    // lets a later op find the chunk each column lowered to.
    mlir::Block* loopBody = forLoop.getBody();

    // This is the innermost loop opened so far in the current factor (loops
    // nest in dataflow order), so a cross product nests at this body.
    _innermostLoopBody = loopBody;

    const mlir::ResultRange dbResults = dbOp->getResults();
    for (size_t resultIndex = 0; resultIndex < dbResults.size(); resultIndex++) {
        _valueMap[dbResults[resultIndex]] = loopBody->getArgument(static_cast<unsigned>(resultIndex));
    }
}

void DBLowering::setInsertionInto(mlir::Block* block) {
    // Every home block already has a terminator - the entry block's func.return
    // or a loop body's implicit nl.yield - so the next op goes just before it,
    // after any siblings already lowered here.
    _builder.setInsertionPoint(block->getTerminator());
}

mlir::Value DBLowering::mapValue(mlir::Value dbValue) const {
    const auto slotIt = _valueMap.find(dbValue);
    if (slotIt == _valueMap.end()) {
        throw IRException("db column used before the operation that produces it");
    }

    return slotIt->second;
}

mlir::Block* DBLowering::ownerBlock(mlir::Value chunkValue) {
    // A lowered chunk is either an nl.for loop variable (a block argument) or a
    // chunk produced in place by a property fetch (an op result); either way the
    // block that holds it is the loop body a consumer must nest into.
    if (const mlir::BlockArgument blockArgument = mlir::dyn_cast<mlir::BlockArgument>(chunkValue)) {
        return blockArgument.getOwner();
    }

    return chunkValue.getDefiningOp()->getBlock();
}
