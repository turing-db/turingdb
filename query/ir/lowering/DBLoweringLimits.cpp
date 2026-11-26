#include "DBLowering.h"

#include <algorithm>
#include <mlir/IR/Location.h>
#include <optional>

#include "mlir/IR/Block.h"
#include "mlir/IR/Builders.h"

#include "NLOps.h"
#include "DBLoweringRowFlow.h"

#include "IRRaiseEffect.h"

#include "IRException.h"
#include "llvm/Support/Casting.h"

using namespace db;

namespace nl = mlir::nl;

namespace {

// The single nl.output that solely consumes every result in the range, or a null
// op if any result has more than one use, a non-nl.output user, or a different
// output than its siblings. Read the direction as "one shared output user => the
// truncate's copy can be dropped": a terminal truncate folds into its output
// exactly when this is non-null, and then erasing the truncate leaves nothing
// dangling.
nl::Output soleOutputConsumer(mlir::ResultRange results) {
    // A result's one-and-only nl.output user, or a null op for any other shape
    // (more than one use, or a lone use that is not an nl.output).
    const auto soleOutputUser = [](const mlir::Value result) -> nl::Output {
        if (!result.hasOneUse()) {
            return nl::Output();
        }
        return mlir::dyn_cast<nl::Output>(*result.user_begin());
    };

    if (results.empty()) {
        return nl::Output();
    }

    const nl::Output output = soleOutputUser(results.front());
    const bool sharedByAllResults = output && std::all_of(results.begin(), results.end(), [&](const mlir::Value result) {
        return soleOutputUser(result) == output;
    });

    return sharedByAllResults ? output : nl::Output();
}

bool opensSourceLoop(mlir::Operation* operation) {
    return mlir::isa<mlir::db::ScanNodes,
                     mlir::db::ConstScanNodes,
                     mlir::db::ConstScanEdges,
                     mlir::db::UnwindConst,
                     mlir::db::LoadCSV,
                     mlir::db::VectorSearch,
                     mlir::db::Unwind,
                     mlir::db::UnwindRange,
                     mlir::db::UnwindComputed,
                     mlir::db::ScanNodesByLabel,
                     mlir::db::ScanNodesByPropertyValue,
                     mlir::db::ScanEdges,
                     mlir::db::ScanEdgesByType,
                     mlir::db::ScanOutEdgesByLabelSrc,
                     mlir::db::ScanInEdgesByLabelTgt,
                     mlir::db::ScanOutEdgesByLabelTgt,
                     mlir::db::ScanInEdgesByLabelSrc,
                     mlir::db::ScanOutEdgesByLabelSrcTgt,
                     mlir::db::ScanInEdgesByLabelSrcTgt,
                     mlir::db::GetOutEdges,
                     mlir::db::GetInEdges,
                     mlir::db::GetEdges,
                     mlir::db::GetOutEdgesByType,
                     mlir::db::GetInEdgesByType,
                     mlir::db::GetOutEdgesByLabel,
                     mlir::db::GetInEdgesByLabel,
                     mlir::db::GetOutEdgesByTypeAndLabel,
                     mlir::db::GetInEdgesByTypeAndLabel,
                     mlir::db::ExplorePaths,
                     mlir::db::CallProcedure,
                     mlir::db::ScanMaterialized>(operation);
}

// The columns an op holding a body hands it through its block arguments, empty for an op
// holding none
mlir::OperandRange subqueryInputColumns(mlir::Operation* operation) {
    if (mlir::db::CallSubquery call = mlir::dyn_cast<mlir::db::CallSubquery>(operation)) {
        return call.getInputColumns();
    } else if (mlir::db::ExistsSubquery exists = mlir::dyn_cast<mlir::db::ExistsSubquery>(operation)) {
        return exists.getInputColumns();
    } else if (mlir::db::CountSubquery count = mlir::dyn_cast<mlir::db::CountSubquery>(operation)) {
        return count.getInputColumns();
    } else if (mlir::db::CollectSubquery collect = mlir::dyn_cast<mlir::db::CollectSubquery>(operation)) {
        return collect.getInputColumns();
    }

    return mlir::OperandRange(operation->operand_end(), operation->operand_end());
}

// The innermost body run one row at a time that holds the op, or null when none does
mlir::Operation* nearestPerRowSubquery(mlir::Operation* operation) {
    for (mlir::Operation* parent = operation->getParentOp(); parent; parent = parent->getParentOp()) {
        if (runsPerRow(parent)) {
            return parent;
        }
    }

    return nullptr;
}

// Reads every row it is handed before it emits any, then emits through a loop of its own
bool accumulatesThenEmits(mlir::Operation* operation) {
    return mlir::isa<mlir::db::Sort,
                     mlir::db::GroupAggregate,
                     mlir::db::Collect,
                     mlir::db::UnwindCollect,
                     mlir::db::ShortestPath>(operation);
}

// The db ops whose rows a projection is emitted over: a source, the nest a cross product
// or a hash join builds, and the emit loop a pipeline breaker opens over what it
// accumulated
bool opensRowLoop(mlir::Operation* operation) {
    const bool returningSubquery = mlir::isa<mlir::db::CallSubquery>(operation)
                                   && operation->getNumResults() > 0;

    return opensSourceLoop(operation)
        || returningSubquery
        || accumulatesThenEmits(operation)
        || mlir::isa<mlir::db::CrossProduct,
                     mlir::db::HashJoin,
                     mlir::db::OptionalMatch>(operation);
}

// A reduction emits its one row at function scope, so what follows it walks no rows of the
// relation it read - and that relation had to be read in full to reduce it
bool reducesToOneRow(mlir::Operation* operation) {
    return mlir::isa<mlir::db::Count,
                     mlir::db::Sum,
                     mlir::db::Min,
                     mlir::db::Max,
                     mlir::db::Avg,
                     mlir::db::StDev,
                     mlir::db::StDevP>(operation);
}

// Passes some of its rows on and keeps the rest back, so the rows reaching a cut below it
// are fewer than the rows a producer above it made. A unit body writes and yields nothing,
// so the rows come out of it as they went in.
bool dropsRows(mlir::Operation* operation) {
    mlir::db::CallSubquery call = mlir::dyn_cast<mlir::db::CallSubquery>(operation);
    if (call) {
        return !call.getUnit();
    } else if (mlir::isa<mlir::db::Conditional>(operation)) {
        return operation->getNumResults() > 0;
    }

    return mlir::isa<mlir::db::FilterOp,
                     mlir::db::FetchNodes,
                     mlir::db::FetchEdges,
                     mlir::db::HashJoin,
                     mlir::db::Skip,
                     mlir::db::Limit,
                     mlir::db::RemoveDuplicates>(operation);
}

// Hands every row it reads on as at least one row, and writes nothing. An optional match
// qualifies: a row its pattern misses comes out padded.
bool keepsEveryRow(mlir::Operation* operation) {
    if (mlir::isa<mlir::db::OptionalMatch>(operation)) {
        return true;
    }

    return hasNoEffectButRaising(operation)
        && !opensRowLoop(operation)
        && !dropsRows(operation)
        && !reducesToOneRow(operation);
}

}

void DBLowering::hoistLimitHandles(mlir::Region& region, mlir::Block* hoistBlock, mlir::Operation* holder) {
    const mlir::Location loc = _builder.getUnknownLoc();

    // Pre-scan for db.limits before any loop is built: nl.for's limit operand is
    // fixed at build time, so each handle must exist first to be threaded in, and
    // which loops a handle attaches to must be known up front. A limit fused into
    // a sort's top-K carries no streaming handle, so it is left out here - and so is
    // one inside a body run one row at a time, which that body hoists into its own
    // step, so the budget resets per input row.
    llvm::SmallVector<mlir::db::Limit, 2> limits;
    region.walk([&](mlir::db::Limit limit) {
        const bool fused = _fusedLimits.count(limit.getOperation());
        const bool heldHere = nearestPerRowSubquery(limit.getOperation()) == holder;

        if (!fused && heldHere) {
            limits.push_back(limit);
        }
    });

    // Hoist one nl.limit handle per db.limit to the top of the hoist block, where
    // each dominates the loops, the update and the truncate that read it.
    if (!limits.empty()) {
        llvm::SmallVector<mlir::Value, 2> counts;
        mlir::Operation* lastCountDefinition = nullptr;
        for (mlir::db::Limit limit : limits) {
            const mlir::Value boundCount = limit.getBoundCount();
            const mlir::Value count = boundCount ? mapValue(boundCount) : mlir::Value();
            counts.push_back(count);

            mlir::Operation* const definition = count ? count.getDefiningOp() : nullptr;
            const bool definedLaterInTheBlock = definition
                                             && definition->getBlock() == hoistBlock
                                             && (!lastCountDefinition || lastCountDefinition->isBeforeInBlock(definition));
            if (definedLaterInTheBlock) {
                lastCountDefinition = definition;
            }
        }

        const bool hoistsIntoTheEntryBlock = hoistBlock == _entryBlock;
        if (hoistsIntoTheEntryBlock) {
            setInsertionBelowHoisted(lastCountDefinition);
        } else if (lastCountDefinition) {
            _builder.setInsertionPointAfter(lastCountDefinition);
        } else {
            _builder.setInsertionPointToStart(hoistBlock);
        }

        for (const auto [limit, count] : llvm::zip_equal(limits, counts)) {
            nl::Limit limitOp = count ? _builder.create<nl::Limit>(loc, count)
                                      : _builder.create<nl::Limit>(loc, *limit.getCount());
            _limitHandles[limit.getOperation()] = limitOp.getState();

            if (hoistsIntoTheEntryBlock) {
                _lastHoisted = limitOp.getOperation();
            }
        }
    }

    // Assign each limit's handle to the loops that produce its columns and their
    // enclosing nest, so only those loops early-exit (consumer loops downstream of
    // the truncate fan out freely). The first limit, in program order, to claim a
    // shared producer wins, so a loop never needs to carry two handles.
    for (mlir::db::Limit limit : limits) {
        mlir::Operation* const limitOp = limit.getOperation();
        _producerWalkVisits.clear();

        bool producedByALoop = false;
        for (const mlir::Value column : limit.getColumns()) {
            producedByALoop |= assignProducerLoops(column, limitOp, /*rowsDroppedBeforeTheCut=*/false, holder);
        }

        // A cut over constants alone walks back to no loop at all - the constants are
        // bound above the nest - so the handle goes to the relation driving the
        // projection instead. Without it the nest runs to its end and the budget only
        // stops the output, producing every row to throw all but k away.
        if (!producedByALoop) {
            assignCardinalityDriverLoop(limit, holder);
        }

        assignOptionalMatchLimits(limit);
    }
}

void DBLowering::lowerLimit(mlir::db::Limit limit) {
    // A limit fused into a sort's top-K does no work of its own: the sort already
    // emits at most k sorted rows, so the limit forwards each input chunk straight
    // to its matching result. The db.output that follows then reads the sort's
    // emit-loop variables, exactly as if the limit were not there.
    if (_fusedLimits.count(limit.getOperation())) {
        const mlir::ResultRange results = limit.getResults();
        const mlir::OperandRange columns = limit.getColumns();
        for (size_t columnIndex = 0; columnIndex < results.size(); columnIndex++) {
            _valueMap[results[columnIndex]] = mapValue(columns[columnIndex]);
        }

        return;
    }

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

    rowAlignCutChunks(chunks);

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

    followCardinalityThrough(chunks, truncatedChunks);
}

void DBLowering::lowerSkip(mlir::db::Skip skip) {
    // The nl chunks the skipped columns lowered to; these are what the truncate
    // copies, and the consumer reads the cut copies.
    llvm::SmallVector<mlir::Value, 4> chunks;
    for (const mlir::Value column : skip.getColumns()) {
        chunks.push_back(mapValue(column));
    }

    // Skip::verify rejects an empty db.skip, so reaching it here means unverified
    // IR - a defensive backstop, as in lowerLimit.
    if (chunks.empty()) {
        throw IRException("db.skip requires at least one column");
    }

    rowAlignCutChunks(chunks);

    const mlir::Location loc = _builder.getUnknownLoc();

    // Hoist the skip handle to the top of the root block, above every loop of this
    // dataflow, so it dominates the update and the truncate placed in the producing loop
    // body.
    // Unlike a limit, a skip threads no operand onto the loops (it cannot
    // early-exit - every row past the dropped prefix must still be produced), so it
    // needs no up-front pre-scan: the handle is created here, in program order,
    // once the producing loops already exist.
    const mlir::Value boundCount = skip.getBoundCount();
    const mlir::Value count = boundCount ? mapValue(boundCount) : mlir::Value();

    setInsertionAtStart(_rootBlock, count);
    nl::Skip skipOp = count ? _builder.create<nl::Skip>(loc, count) : _builder.create<nl::Skip>(loc, *skip.getCount());
    const mlir::Value handle = skipOp.getState();

    // The representative is the first skipped column, in the innermost producing
    // loop body (post-cross-product if there is one), so its row count is what this
    // step charges and the truncate's suffix is cut from.
    const mlir::Value representative = chunks.front();
    setInsertionInto(ownerBlock(representative));

    // Charge this step's rows, then lift the surviving suffix of every skipped
    // column into fresh chunks, just before the consumer (nl.output when unchained,
    // the inner sub-pipeline when chained). The unchained case is folded away by
    // foldSkipTruncatesIntoOutputs - nl.output emits the suffix in place at an
    // offset - so this copy survives only when the suffix feeds an inner
    // sub-pipeline that reads from row zero.
    _builder.create<nl::SkipUpdate>(loc, handle, representative);
    nl::SkipTruncate truncate = _builder.create<nl::SkipTruncate>(loc, handle, chunks);

    // Map db.skip's results to the truncated chunks, so its consumer reads the cut
    // copies rather than the full producer chunks.
    const mlir::ResultRange dbResults = skip.getResults();
    const mlir::ResultRange truncatedChunks = truncate.getResults();
    for (size_t resultIndex = 0; resultIndex < dbResults.size(); resultIndex++) {
        _valueMap[dbResults[resultIndex]] = truncatedChunks[resultIndex];
    }

    followCardinalityThrough(chunks, truncatedChunks);
}

void DBLowering::detectTopKFusion(mlir::func::FuncOp dbFunction) {
    dbFunction.walk([&](mlir::db::Limit limit) {
        const mlir::OperandRange columns = limit.getColumns();
        if (columns.empty()) {
            return;
        }

        // Every column the limit caps must come from one db.sort - otherwise the
        // limit is not a terminal ORDER BY ... LIMIT and the streaming limit path
        // handles it.
        mlir::db::Sort sort;
        for (const mlir::Value column : columns) {
            mlir::db::Sort definingSort = column.getDefiningOp<mlir::db::Sort>();
            if (!definingSort || (sort && sort != definingSort)) {
                return;
            }

            sort = definingSort;
        }

        // Capping the sort to top-K must not starve another consumer, so the limit
        // must be the sole user of every result the sort produces.
        for (const mlir::Value result : sort.getResults()) {
            for (mlir::Operation* const user : result.getUsers()) {
                if (user != limit.getOperation()) {
                    return;
                }
            }
        }

        // The sole-user check above guarantees this limit is the only consumer of
        // the sort, so no other limit can claim it; record the fusion.
        _sortTopK[sort.getOperation()] = limit;
        _fusedLimits.insert(limit.getOperation());
    });
}

bool DBLowering::assignProducerLoops(mlir::Value column,
                                     mlir::Operation* limitOwner,
                                     bool rowsDroppedBeforeTheCut,
                                     mlir::Operation* holder) {
    bool reachedALoop = false;
    if (findCoveringProducerVisit(column, rowsDroppedBeforeTheCut, reachedALoop)) {
        return reachedALoop;
    }

    llvm::SmallVector<ProducerWalkFrame> frames;
    frames.emplace_back();
    visitProducer(column, limitOwner, rowsDroppedBeforeTheCut, holder, frames.back());

    for (;;) {
        ProducerWalkFrame& frame = frames.back();

        if (frame._nextOperand < frame._operands.size()) {
            const mlir::Value operand = frame._operands[frame._nextOperand];
            const bool operandRowsDropped = frame._operandsRowsDropped;
            frame._nextOperand++;

            bool operandReachedALoop = false;
            if (findCoveringProducerVisit(operand, operandRowsDropped, operandReachedALoop)) {
                frame._reachedALoop = frame._reachedALoop || operandReachedALoop;
                continue;
            }

            frames.emplace_back();
            visitProducer(operand, limitOwner, operandRowsDropped, holder, frames.back());
            continue;
        }

        _producerWalkVisits[frame._column] = ProducerWalkVisit {frame._reachedALoop, frame._rowsDroppedBeforeTheCut};
        const bool frameReachedALoop = frame._reachedALoop;
        frames.pop_back();

        if (frames.empty()) {
            return frameReachedALoop;
        }

        ProducerWalkFrame& parent = frames.back();
        parent._reachedALoop = parent._reachedALoop || frameReachedALoop;
    }
}

// A visit with rows dropped claims a subset of what one without claims, and reaches the
// same loops, so only a visit without them can add to one made with them
bool DBLowering::findCoveringProducerVisit(mlir::Value column, bool rowsDroppedBeforeTheCut, bool& reachedALoop) const {
    const auto visitIt = _producerWalkVisits.find(column);
    const bool wasVisited = visitIt != _producerWalkVisits.end();
    const bool isCovered = wasVisited && (rowsDroppedBeforeTheCut || !visitIt->second._rowsDroppedBeforeTheCut);
    if (isCovered) {
        reachedALoop = visitIt->second._reachedALoop;
    }

    return isCovered;
}

void DBLowering::visitProducer(mlir::Value column,
                               mlir::Operation* limitOwner,
                               bool rowsDroppedBeforeTheCut,
                               mlir::Operation* holder,
                               ProducerWalkFrame& frame) {
    frame._column = column;
    frame._rowsDroppedBeforeTheCut = rowsDroppedBeforeTheCut;
    frame._operandsRowsDropped = rowsDroppedBeforeTheCut;

    mlir::Operation* const definingOp = column.getDefiningOp();
    if (!definingOp) {
        // A subquery body reads the rows in flight through its block arguments, so the
        // walk carries on from the input column each one stands for. A cross-product
        // factor's loop variable is a block argument too, with no producer to reach from
        // here: its loop is reached through the factor's yield in the branch below.
        const mlir::BlockArgument argument = mlir::cast<mlir::BlockArgument>(column);
        mlir::Operation* const bodyHolder = argument.getOwner()->getParentOp();
        const mlir::OperandRange inputs = subqueryInputColumns(bodyHolder);

        if (argument.getArgNumber() >= inputs.size()) {
            return;
        }

        // The handle this body holds is created inside the loop over its input rows, so a
        // loop outside the body cannot carry it: the walk stops at the boundary.
        if (bodyHolder == holder) {
            return;
        }

        frame._operands.push_back(inputs[argument.getArgNumber()]);
        return;
    }

    const bool opensLoop = opensSourceLoop(definingOp);
    const bool isCrossProduct = mlir::isa<mlir::db::CrossProduct>(definingOp);
    const bool isHashJoin = mlir::isa<mlir::db::HashJoin>(definingOp);
    const bool isSubquery = mlir::isa<mlir::db::CallSubquery>(definingOp);

    const bool emitsThroughLoop = isSubquery
                                  || accumulatesThenEmits(definingOp)
                                  || mlir::isa<mlir::db::OptionalMatch>(definingOp);

    // An op reading the whole relation before emitting any of it, and a materialize, whose
    // scans are the emit loops, need every row fed to them: the walk stops, and the limit
    // budgets the emit loop alone. An optional match emits each step's rows in input order, so
    // once the budget is spent no later step adds a row: the walk goes on to bound its inputs.
    const bool accumulatesTheRelation = accumulatesThenEmits(definingOp)
                                     || mlir::isa<mlir::db::Materialize>(definingOp);
    const bool breaksPipeline = accumulatesTheRelation || reducesToOneRow(definingOp);

    frame._reachedALoop = opensLoop || isCrossProduct || isHashJoin || emitsThroughLoop;

    // A loop's budget only stops it from taking another step, so bounding one never trims
    // the step it is in. A cross product's or a hash join's budget cuts the rows it pairs,
    // which stands only while every row it makes reaches the output: an op below the cut
    // that drops rows - a filter, a skip, a dedup - would leave it discarding rows that
    // would have survived, and the cut short of its count. So either keeps the handle only
    // on a path that drops nothing; its factor loops take it either way.
    // Declining the handle is not the same as reaching no loop: a cross product is still a
    // producer the walk found, so reachedALoop stands and the cut keeps its nest.
    const bool boundsCrossProduct = isCrossProduct && !rowsDroppedBeforeTheCut;
    const bool boundsHashJoin = isHashJoin && !rowsDroppedBeforeTheCut;
    const bool takesTheHandle = opensLoop || boundsCrossProduct || boundsHashJoin || emitsThroughLoop;

    // The first limit, in program order, to claim a producer wins, so a loop
    // shared by two limits' nests carries the outer one and never two handles.
    if (takesTheHandle) {
        _loopLimits.try_emplace(definingOp, limitOwner);
    }

    if (breaksPipeline) {
        return;
    }

    const bool rowsDropped = rowsDroppedBeforeTheCut || dropsRows(definingOp);
    frame._operandsRowsDropped = rowsDropped;

    if (isHashJoin) {
        // A hash join's built side has to be read whole - a build loop stopped short of
        // its rows would leave the probe unable to match them - so the budget reaches only
        // the probed factor's loops. That is the left factor, the one the probe walks.
        mlir::db::HashJoin join = mlir::cast<mlir::db::HashJoin>(definingOp);
        mlir::Operation* const probeYield = join.getLeftFactor().front().getTerminator();
        for (const mlir::Value yielded : probeYield->getOperands()) {
            frame._operands.push_back(yielded);
        }
    } else if (isCrossProduct) {
        // A cross product takes no column operands - its factors are regions - so
        // walk through each factor's db.yield operands to reach the factor
        // scans/edge loops that produce the crossed columns.
        mlir::db::CrossProduct cross = mlir::cast<mlir::db::CrossProduct>(definingOp);
        mlir::Region* const factors[] = {&cross.getLeftFactor(), &cross.getRightFactor()};
        for (mlir::Region* const factor : factors) {
            mlir::Operation* const yield = factor->front().getTerminator();
            for (const mlir::Value yielded : yield->getOperands()) {
                frame._operands.push_back(yielded);
            }
        }
    } else if (isSubquery) {
        // A subquery's results come out of its body, so the walk goes through the body's
        // yield to the loops inside it, and from them through the block arguments back to
        // the inputs' own loops. A body that carries nothing hands its inputs back through
        // the op itself, so the walk reaches them from here.
        mlir::db::CallSubquery call = mlir::cast<mlir::db::CallSubquery>(definingOp);
        mlir::Operation* const yield = call.getBody().front().getTerminator();
        for (const mlir::Value yielded : yield->getOperands()) {
            frame._operands.push_back(yielded);
        }

        if (!call.getCarriesScope()) {
            for (const mlir::Value input : call.getInputColumns()) {
                frame._operands.push_back(input);
            }
        }
    } else {
        // A non-loop producer (a property fetch) is traversed but not assigned -
        // it opens no loop - so its input chunk's loop is still reached.
        for (const mlir::Value operand : definingOp->getOperands()) {
            frame._operands.push_back(operand);
        }
    }
}

void DBLowering::assignCardinalityDriverLoop(mlir::db::Limit limit, mlir::Operation* holder) {
    mlir::Operation* const limitOp = limit.getOperation();

    // The relation driving the projection is the loop opened last before the cut: its rows
    // are the ones nl.output emits the constants over, so they are the rows this budget
    // cuts. A reduction in between leaves no such loop - it emits its one row at function
    // scope, and the relation behind it had to be read in full to reduce it.
    // A row-dropping op only matters once the driver is behind it: what it drops are the
    // driver's rows, which is what bars a cross-product driver from the budget.
    mlir::Operation* driver = nullptr;
    bool rowsDropped = false;
    for (mlir::Operation& operation : *limitOp->getBlock()) {
        if (&operation == limitOp) {
            break;
        }

        if (opensRowLoop(&operation)) {
            driver = &operation;
            rowsDropped = false;
        } else if (reducesToOneRow(&operation)) {
            driver = nullptr;
        } else if (dropsRows(&operation)) {
            rowsDropped = true;
        }
    }

    if (!driver) {
        return;
    }

    assignProducerLoops(driver->getResult(0), limitOp, rowsDropped, holder);
}

void DBLowering::assignOptionalMatchLimits(mlir::db::Limit limit) {
    mlir::Operation* const limitOp = limit.getOperation();

    for (mlir::Operation* operation = limitOp->getPrevNode(); operation; operation = operation->getPrevNode()) {
        const bool isOptionalMatch = mlir::isa<mlir::db::OptionalMatch>(operation);
        const bool drainedUnderThisCut = isOptionalMatch && _loopLimits.lookup(operation) == limitOp;

        if (drainedUnderThisCut) {
            _optionalMatchLimits[operation] = limitOp;
        } else if (!keepsEveryRow(operation)) {
            return;
        }
    }
}

void DBLowering::foldTruncatesIntoOutputs(mlir::func::FuncOp nlFunction) {
    // Collect the foldable truncates first; erasing ops mid-walk is unsafe.
    llvm::SmallVector<nl::LimitTruncate, 4> foldable;

    nlFunction.walk([&](nl::LimitTruncate truncate) {
        const mlir::ResultRange results = truncate.getResults();

        // Foldable only if a single nl.output solely consumes every truncated
        // column; soleOutputConsumer returns that shared output (a null op if not).
        // Not const: the nl.output accessors below are non-const, as MLIR generates.
        nl::Output output = soleOutputConsumer(results);
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
        _builder.create<nl::Output>(output.getLoc(),
                                    truncate.getColumns(),
                                    truncate.getState(),
                                    mlir::Value(),
                                    output.getCardinality(),
                                    output.getColumnNamesAttr());

        eraseOp(output);
        eraseOp(truncate);
    }
}

void DBLowering::foldSkipTruncatesIntoOutputs(mlir::func::FuncOp nlFunction) {
    // The skip sibling of foldTruncatesIntoOutputs: a terminal SKIP whose
    // nl.skip_truncate feeds only an nl.output folds into a skip-bearing output
    // that emits the surviving suffix in place (offset getSkipThisStep()) instead
    // of copying it to the front. Collect first; erasing ops mid-walk is unsafe.
    llvm::SmallVector<nl::SkipTruncate, 4> foldable;

    nlFunction.walk([&](nl::SkipTruncate truncate) {
        const mlir::ResultRange results = truncate.getResults();

        // Foldable only if a single nl.output solely consumes every truncated
        // column; soleOutputConsumer returns that shared output (a null op if not).
        // The single shared user also self-excludes the SKIP+LIMIT case: there an
        // nl.limit sits between this skip and the output, so the truncate's result
        // feeds nl.limit_update (and the limit's own consumer), never one nl.output
        // - the shared-user test fails and the skip stays a copy, bounded by the
        // limit's loop early-exit.
        // Not const: the nl.output accessors below are non-const, as MLIR generates.
        nl::Output output = soleOutputConsumer(results);

        // Bail if the output already carries a handle: a folded output carries at
        // most one of limit/skip, and the rebuild below would drop a pre-existing
        // one.
        if (!output || output.getLimit() || output.getSkip()) {
            return;
        }

        // The output must consume exactly the truncate's results - all of them, in
        // the same order, and nothing else - so rebuilding it over the truncate's
        // input columns preserves the projection. Same precondition as the limit
        // fold.
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

    for (nl::SkipTruncate truncate : foldable) {
        nl::Output output = mlir::cast<nl::Output>(*truncate.getResult(0).user_begin());

        // Re-emit the output over the untruncated inputs, carrying the skip handle
        // in the third operand, so it streams the surviving suffix off the counter
        // (offset getSkipThisStep(), getEmitThisStep() rows) instead of a copy. The
        // preceding nl.skip_update still sets that offset and count. Drop the old
        // output and the now-unused truncate.
        _builder.setInsertionPoint(output);
        _builder.create<nl::Output>(output.getLoc(),
                                    truncate.getColumns(),
                                    mlir::Value(),
                                    truncate.getState(),
                                    output.getCardinality(),
                                    output.getColumnNamesAttr());

        eraseOp(output);
        eraseOp(truncate);
    }
}

mlir::Value DBLowering::loopLimitHandle(mlir::Operation* dbOp) const {
    mlir::Operation* const limitOwner = _loopLimits.lookup(dbOp);
    if (!limitOwner) {
        return mlir::Value();
    }

    return _limitHandles.lookup(limitOwner);
}
