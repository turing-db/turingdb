#include "DBLowering.h"

#include <mlir/IR/Location.h>

#include "mlir/IR/Block.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/Types.h"

#include "NLOps.h"

#include "DBColumnElement.h"

#include "IRValueTypes.h"

#include "IRException.h"
#include "llvm/Support/Casting.h"

using namespace db;

namespace nl = mlir::nl;

void DBLowering::lowerSort(mlir::db::Sort sort) {
    // The nl chunks the sorted columns lowered to; these are what sort_collect
    // appends to the buffers, and the emit loop yields back sorted.
    llvm::SmallVector<mlir::Value, 4> chunks;
    for (const mlir::Value column : sort.getColumns()) {
        chunks.push_back(mapValue(column));
    }

    // Sort::verify rejects an empty db.sort, so reaching it here means unverified
    // IR - a defensive backstop, as in lowerFactor and lowerLimit.
    if (chunks.empty()) {
        throw IRException("db.sort requires at least one column");
    }

    rowAlignBufferedChunks(chunks);

    const mlir::Location loc = _builder.getUnknownLoc();

    // The accumulator and its sort spec are hoisted to the top of the root block,
    // above every loop of this dataflow, so the buffers exist before the producing
    // loop fills them and the handle dominates the collect and the emit loop. A sort
    // fused with a terminal db.limit carries that count as its top-K bound, so the
    // accumulator keeps only the best k rows; an unfused sort keeps every row.
    const auto fusedIt = _sortTopK.find(sort.getOperation());
    mlir::IntegerAttr topKAttr;
    mlir::Value boundTopK;
    if (fusedIt != _sortTopK.end()) {
        mlir::db::Limit fusedLimit = fusedIt->second;
        const mlir::Value boundCount = fusedLimit.getBoundCount();

        topKAttr = fusedLimit.getCountAttr();
        boundTopK = boundCount ? mapValue(boundCount) : mlir::Value();
    }

    setInsertionAtStart(_rootBlock, boundTopK);

    nl::SortBuffer bufferOp = _builder.create<nl::SortBuffer>(loc,
                                                              sort.getKeyColumnsAttr(),
                                                              sort.getKeyAscendingAttr(),
                                                              topKAttr,
                                                              boundTopK);
    const mlir::Value state = bufferOp.getState();

    // The collect appends each step's chunk of every column to the buffers. It
    // sits in the innermost producing loop body, where all sorted columns are
    // bound together (the same block db.output would emit from), so the buffers
    // stay row-aligned.
    const mlir::Value representative = chunks.front();
    setInsertionInto(ownerBlock(representative));
    _builder.create<nl::SortCollect>(loc, state, chunks);

    // The emit phase is an nl.sort source iterator plus its nl.for, placed after
    // the producing loop (before the func.return) so the buffers are full when
    // the loop first steps. The iterator yields one chunk per collected column,
    // so its chunk types are exactly the collected chunk types.
    llvm::SmallVector<mlir::Type, 4> chunkTypes;
    for (const mlir::Value chunk : chunks) {
        chunkTypes.push_back(chunk.getType());
    }

    const nl::IteratorType iteratorType = nl::IteratorType::get(_builder.getContext(), chunkTypes);

    setInsertionInto(_rootBlock);
    nl::Sort sortOp = _builder.create<nl::Sort>(loc, iteratorType, state);

    // The emit loop binds one variable per sorted column. It is the only loop of this
    // sort a limit may bound - assignProducerLoops stops at the sort and hands the
    // handle to it, never to the producing loops, which had to see every row - so it
    // stops re-chunking once a downstream streaming limit is spent. A limit fused into
    // the top-K bounds nothing here: the accumulator already holds at most k rows.
    // buildLoopForSource maps db.sort's results to the loop variables, so the
    // db.output that follows lowers into the emit loop body reading the sorted chunks.
    buildLoopForSource(sortOp.getResult(), sort.getOperation());
}

// The shared seen-set is hoisted to the top of the root block, above every branch's
// loops, so it is emptied once per step of that block and dominates each branch's filter -
// the same placement lowerRemoveDuplicates gives a dedup's private set.
void DBLowering::lowerDistinctSet(mlir::db::DistinctSet distinctSet) {
    _builder.setInsertionPointToStart(_rootBlock);

    _valueMap[distinctSet.getSet()] = _builder.create<nl::Distinct>(_builder.getUnknownLoc()).getState();
}

void DBLowering::lowerMaterialize(mlir::db::Materialize materialize) {
    const mlir::Location loc = _builder.getUnknownLoc();

    llvm::SmallVector<mlir::Value, 4> chunks;
    for (const mlir::Value column : materialize.getColumns()) {
        chunks.push_back(mapValue(column));
    }

    rowAlignBufferedChunks(chunks);

    _builder.setInsertionPointToStart(_rootBlock);
    const mlir::Value state = _builder.create<nl::RowBuffer>(loc).getState();

    setInsertionInto(deepestOwnerBlock(chunks, _rootBlock));
    _builder.create<nl::RowCollect>(loc, state, chunks);

    _valueMap[materialize.getRows()] = state;
}

void DBLowering::lowerScanMaterialized(mlir::db::ScanMaterialized scan) {
    const mlir::Value state = mapValue(scan.getRows());

    nl::RowCollect collect;
    for (mlir::Operation* user : state.getUsers()) {
        if (nl::RowCollect stateCollect = mlir::dyn_cast<nl::RowCollect>(user)) {
            collect = stateCollect;
            break;
        }
    }

    if (!collect) {
        throw IRException("db.scan_materialized reads rows no db.materialize collected");
    }

    drainCollectedBranches({collect}, state, _rootBlock, scan.getOperation());
}

void DBLowering::lowerRemoveDuplicates(mlir::db::RemoveDuplicates distinct) {
    // The nl chunks the deduped columns lowered to; these are what the filter
    // reads to build each row's key, and gathers the survivors from.
    llvm::SmallVector<mlir::Value, 4> chunks;
    for (const mlir::Value column : distinct.getColumns()) {
        chunks.push_back(mapValue(column));
    }

    // RemoveDuplicates::verify rejects an empty db.remove_duplicates, so reaching
    // it here means unverified IR - a defensive backstop, as in lowerSort.
    if (chunks.empty()) {
        throw IRException("db.remove_duplicates requires at least one column");
    }

    rowAlignBufferedChunks(chunks);

    const mlir::Location loc = _builder.getUnknownLoc();

    // The seen-set handle is hoisted to the top of the root block, above every loop
    // of this dataflow, so it is reset once per step of that block and dominates the
    // filter placed in the producing loop body. A dedup naming a shared set reads that
    // one instead, so every branch of a union records its rows in the same set.
    mlir::Value state;
    if (const mlir::Value sharedSet = distinct.getSet()) {
        state = mapValue(sharedSet);
    } else {
        _builder.setInsertionPointToStart(_rootBlock);
        state = _builder.create<nl::Distinct>(loc).getState();
    }

    // The filter sits in the innermost producing loop body, where all deduped
    // columns are bound together (the same block db.output would emit from), and
    // emits each step's not-yet-seen rows as fresh survivor chunks. It opens no
    // loop of its own: DISTINCT streams, so - unlike db.sort - the rows are
    // filtered in place in the producing loop, not accumulated and re-emitted.
    setInsertionInto(deepestOwnerBlock(chunks, _rootBlock));
    nl::DistinctFilter filter = _builder.create<nl::DistinctFilter>(loc, state, chunks);

    // Map db.remove_duplicates' results to the survivor chunks, so its consumer
    // reads the deduped rows: nl.output when the query ends here, or a downstream
    // traversal when a WITH DISTINCT feeds a further MATCH (the chained case). The
    // survivor chunk is a genuine cut chunk (like nl.limit_truncate's), so that
    // consumer needs no DISTINCT awareness of its own.
    const mlir::ResultRange dbResults = distinct.getResults();
    const mlir::ResultRange filteredChunks = filter.getResults();
    for (size_t resultIndex = 0; resultIndex < dbResults.size(); resultIndex++) {
        _valueMap[dbResults[resultIndex]] = filteredChunks[resultIndex];
    }

    followCardinalityThrough(chunks, filteredChunks);
}

void DBLowering::foldBroadcastsIntoOutputs(mlir::func::FuncOp nlFunction) {
    llvm::SmallVector<nl::Output, 4> foldable;

    nlFunction.walk([&foldable](nl::Output output) {
        const mlir::OperandRange columns = output.getColumns();
        if (output.getCardinality() || columns.empty()) {
            return;
        }

        mlir::Value sharedCardinality;
        for (const mlir::Value column : columns) {
            nl::BroadcastConstant broadcast = column.getDefiningOp<nl::BroadcastConstant>();
            if (!broadcast || !column.hasOneUse()) {
                return;
            }

            const mlir::Value cardinality = broadcast.getCardinality();
            if (!cardinality) {
                return;
            } else if (!sharedCardinality) {
                sharedCardinality = cardinality;
            } else if (cardinality != sharedCardinality) {
                return;
            }
        }

        foldable.push_back(output);
    });

    for (nl::Output output : foldable) {
        llvm::SmallVector<mlir::Value, 4> constants;
        llvm::SmallVector<nl::BroadcastConstant, 4> broadcasts;
        for (const mlir::Value column : output.getColumns()) {
            nl::BroadcastConstant broadcast = column.getDefiningOp<nl::BroadcastConstant>();
            constants.push_back(broadcast.getValue());
            broadcasts.push_back(broadcast);
        }

        _builder.setInsertionPoint(output);
        _builder.create<nl::Output>(output.getLoc(),
                                    constants,
                                    output.getLimit(),
                                    output.getSkip(),
                                    broadcasts.front().getCardinality(),
                                    output.getColumnNamesAttr());

        eraseOp(output);
        for (nl::BroadcastConstant broadcast : broadcasts) {
            eraseOp(broadcast);
        }
    }
}

void DBLowering::lowerFilter(mlir::db::FilterOp filter) {
    llvm::SmallVector<mlir::Value, 4> columnChunks;
    llvm::SmallVector<mlir::Type, 4> resultTypes;
    for (const mlir::Value column : filter.getColumnsToFilter()) {
        const mlir::Value columnChunk = mapValue(column);
        const mlir::Type chunkType = columnChunk.getType();

        columnChunks.push_back(columnChunk);
        resultTypes.push_back(chunkType);
    }

    // A predicate over constants alone holds one value standing for every row, and the cut
    // reads a mask row by row: it is laid out over the rows of the columns it cuts, so the
    // mask has exactly as many rows as they do - the driving relation's when a column of
    // it is what the cut carries, and the single row a projection of constants is when
    // nothing drives it.
    const mlir::Value maskDriver = cardinalityDriver(columnChunks);
    const mlir::Value maskChunk = rowAlignedChunk(mapValue(filter.getMask()), maskDriver);

    // Inserted into the deepest block, where all operands are defined
    mlir::Value insertionReference = maskChunk;
    for (const mlir::Value columnChunk : columnChunks) {
        mlir::Block* const block = deeperBlock(insertionReference, columnChunk);
        if (ownerBlock(columnChunk) == block) {
            insertionReference = columnChunk;
        }
    }

    setInsertionInto(ownerBlock(insertionReference));

    const mlir::Location uloc = _builder.getUnknownLoc();
    nl::Filter nlFilter = _builder.create<nl::Filter>(uloc, resultTypes, maskChunk, columnChunks);

    const mlir::ResultRange filteredColumns = filter.getFilteredColumns();
    for (size_t columnIndex = 0; columnIndex < filteredColumns.size(); columnIndex++) {
        const mlir::Value filteredCol = filteredColumns[columnIndex];
        const mlir::Value outputChunk = nlFilter.getResult(columnIndex);
        _valueMap[filteredCol] = outputChunk;
    }

    followCardinalityThrough(columnChunks, nlFilter.getResults());
}

nl::Output DBLowering::lowerOutput(mlir::db::Output output) {
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

    // If there is a column which is produced by a block which is not the entry block
    // (e.g. a loop block), then set the anchor to be that block. Otherwise no relation
    // drives the projection, as in RETURN 1, and the output is the single row it holds.
    // Getting the owner block of the returned column is sufficient because in nested
    // loops, each Cypher variable is redefined each op due to the carry set implictly
    // filtering. Otherwise we would need to check for the *deepest* block of all
    // returned values.
    mlir::Block* anchorBlock = _entryBlock;
    for (const mlir::Value column : columns) {
        mlir::Block* const columnBlock = ownerBlock(column);
        if (columnBlock != _entryBlock) {
            anchorBlock = columnBlock;
            break;
        }
    }

    setInsertionInto(anchorBlock);

    return _builder.create<nl::Output>(_builder.getUnknownLoc(),
                                       columns,
                                       mlir::Value(),
                                       mlir::Value(),
                                       mlir::Value(),
                                       output.getColumnNamesAttr());
}
