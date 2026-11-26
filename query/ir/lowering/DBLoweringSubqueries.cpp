#include "DBLowering.h"

#include <mlir/IR/Location.h>

#include "mlir/IR/Block.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/Types.h"

#include "NLOps.h"
#include "DBLoweringRowFlow.h"

#include "IRException.h"
#include "llvm/Support/Casting.h"

using namespace db;

namespace nl = mlir::nl;
namespace storage = mlir::storage;

void DBLowering::lowerPatternComprehension(mlir::db::PatternComprehension comprehension) {
    const mlir::Location loc = _builder.getUnknownLoc();
    mlir::MLIRContext* const context = _builder.getContext();

    llvm::SmallVector<mlir::Value, 4> inputChunks;
    for (const mlir::Value column : comprehension.getInputColumns()) {
        inputChunks.push_back(mapValue(column));
    }

    // The step the accumulator covers is the one binding the columns the comprehension is
    // read beside, so it is emptied once per chunk of them - and the pattern's nest and
    // the build both go there, the build after the nest.
    mlir::Block* const stepBlock = deepestOwnerBlock(inputChunks, _rootBlock);

    // Any chunk of the step counts its rows; a comprehension read where nothing is in
    // flight has none, and covers the single empty row instead.
    const mlir::Value cardinality = inputChunks.empty() ? mlir::Value {} : cardinalityDriver(inputChunks);

    setInsertionInto(stepBlock);
    nl::RowListsBuffer buffer = _builder.create<nl::RowListsBuffer>(loc, cardinality);
    const mlir::Value state = buffer.getState();

    // The pattern reads this step's rows through its block arguments, and the row tag
    // through the trailing one; a pattern with nothing to join onto has neither.
    mlir::Block& patternBlock = comprehension.getPattern().front();
    for (size_t inputIndex = 0; inputIndex < inputChunks.size(); inputIndex++) {
        _valueMap[patternBlock.getArgument(static_cast<unsigned>(inputIndex))] = inputChunks[inputIndex];
    }

    if (!inputChunks.empty()) {
        _valueMap[patternBlock.getArgument(static_cast<unsigned>(inputChunks.size()))] = buffer.getTag();
    }

    // A dataflow of its own, rooted where the accumulator sits so its loops nest inside
    // this step; the caller's root and innermost loop are restored once it is lowered.
    mlir::Block* const previousRoot = _rootBlock;
    mlir::Block* const previousInnermostLoopBody = _innermostLoopBody;
    const mlir::Value previousInnermostCardinality = _innermostCardinality;
    _rootBlock = stepBlock;
    _innermostLoopBody = nullptr;
    _innermostCardinality = mlir::Value();

    mlir::Value rowTagChunk;
    mlir::Value valueChunk;

    for (mlir::Operation& operation : patternBlock) {
        mlir::db::ComprehensionYield yield = mlir::dyn_cast<mlir::db::ComprehensionYield>(operation);
        if (!yield) {
            lowerOperation(operation);
            continue;
        }

        if (yield.getRowTags()) {
            rowTagChunk = mapValue(yield.getRowTags());
        }

        valueChunk = mapValue(yield.getValue());
    }

    // PatternComprehension::verify guarantees the yield, so an empty one here means
    // unverified IR - the defensive backstop lowerOptionalMatch keeps too.
    if (!valueChunk) {
        throw IRException("db.pattern_comprehension yields no value");
    }

    // The matches the value is read against are the ones the tag counts, since a WHERE has
    // cut both together
    valueChunk = rowListsValueChunk(valueChunk, rowTagChunk ? rowTagChunk : rowsInFlight());

    llvm::SmallVector<mlir::Value, 2> stagedChunks {valueChunk};
    if (rowTagChunk) {
        stagedChunks.push_back(rowTagChunk);
    }

    setInsertionInto(deepestOwnerBlock(stagedChunks, stepBlock));
    _builder.create<nl::RowListsCollect>(loc, state, rowTagChunk, mlir::Value(), valueChunk);

    _rootBlock = previousRoot;
    _innermostLoopBody = previousInnermostLoopBody;
    _innermostCardinality = previousInnermostCardinality;

    // Every row gets a list, the empty one where the pattern matched nothing, so the
    // lists are a container chunk rather than the nullable one a list comprehension builds
    const mlir::Type listType = storage::ListType::get(context, listedElementType(context, valueChunk));
    const nl::ChunkType resultType = nl::ChunkType::get(context, listType);

    setInsertionInto(stepBlock);
    nl::RowListsResult lists = _builder.create<nl::RowListsResult>(loc, resultType, state, cardinality);

    _valueMap[comprehension.getResult()] = lists.getResult();
}

mlir::Value DBLowering::rowListsValueChunk(mlir::Value valueChunk, mlir::Value rows) {
    // A value computed from constants alone holds one cell standing for every row, so it is
    // laid out over the rows it is read against
    const mlir::Value aligned = rowAlignedChunk(valueChunk, rows);

    return listItemChunk(aligned);
}

void DBLowering::lowerOptionalMatch(mlir::db::OptionalMatch optionalMatch) {
    const mlir::Location loc = _builder.getUnknownLoc();

    llvm::SmallVector<mlir::Value, 4> inputChunks;
    for (const mlir::Value column : optionalMatch.getInputColumns()) {
        inputChunks.push_back(mapValue(column));
    }

    // The step the accumulator covers is the one binding the columns the pattern joins
    // onto, so it is emptied once per chunk of them - and the pattern's nest and the drain
    // loop both go there, the drain after the nest.
    mlir::Block* const stepBlock = deepestOwnerBlock(inputChunks, _rootBlock);

    setInsertionInto(stepBlock);
    nl::OptionalBuffer buffer = _builder.create<nl::OptionalBuffer>(loc, inputChunks);
    const mlir::Value state = buffer.getState();

    // Every row drained reaches the cut, so once the pattern has matched as many rows as
    // the cut still admits, the drain spends the budget before it reaches a missed row.
    mlir::Operation* const optionalMatchOp = optionalMatch.getOperation();
    mlir::Value patternLimit;
    if (mlir::Operation* const cut = _optionalMatchLimits.lookup(optionalMatchOp)) {
        patternLimit = _builder.create<nl::LimitRemaining>(loc, _limitHandles.lookup(cut)).getState();
        _limitHandles[optionalMatchOp] = patternLimit;
    }

    // The pattern reads this step's rows through its block arguments, and the row tag
    // through the trailing one; a pattern with nothing to join onto has neither.
    mlir::Block& patternBlock = optionalMatch.getPattern().front();
    for (size_t inputIndex = 0; inputIndex < inputChunks.size(); inputIndex++) {
        _valueMap[patternBlock.getArgument(static_cast<unsigned>(inputIndex))] = inputChunks[inputIndex];
    }

    if (!inputChunks.empty()) {
        _valueMap[patternBlock.getArgument(static_cast<unsigned>(inputChunks.size()))] = buffer.getTag();
    }

    // A dataflow of its own, rooted where the accumulator sits so its loops nest inside
    // this step; the caller's root and innermost loop are restored once it is lowered.
    mlir::Block* const previousRoot = _rootBlock;
    mlir::Block* const previousInnermostLoopBody = _innermostLoopBody;
    const mlir::Value previousInnermostCardinality = _innermostCardinality;
    _rootBlock = stepBlock;
    _innermostLoopBody = nullptr;
    _innermostCardinality = mlir::Value();

    if (patternLimit) {
        mlir::db::OptionalYield patternYield = mlir::cast<mlir::db::OptionalYield>(patternBlock.getTerminator());

        _producerWalkVisits.clear();
        for (const mlir::Value column : patternYield.getColumns()) {
            assignProducerLoops(column, optionalMatchOp, /*rowsDroppedBeforeTheCut=*/false, optionalMatchOp);
        }
    }

    llvm::SmallVector<mlir::Value, 4> matchedChunks;
    mlir::Value matchedTag;

    for (mlir::Operation& operation : patternBlock) {
        mlir::db::OptionalYield yield = mlir::dyn_cast<mlir::db::OptionalYield>(operation);
        if (!yield) {
            lowerOperation(operation);
            continue;
        }

        for (const mlir::Value column : yield.getColumns()) {
            matchedChunks.push_back(mapValue(column));
        }

        if (yield.getTag()) {
            matchedTag = mapValue(yield.getTag());
        }
    }

    // OptionalMatch::verify rejects a pattern yielding no column, so reaching this means
    // unverified IR - a defensive backstop, as in lowerFactor and lowerSort.
    if (matchedChunks.empty()) {
        throw IRException("db.optional_match pattern yields no column");
    }

    // The drain rebuilds a missed row's carried columns out of this step's input chunks, so
    // the two must be the same chunk type however the db types were spelled: a column a
    // CALL yielded enters type-erased and comes back refined, and the chunk behind it is
    // the same one either way.
    for (size_t inputIndex = 0; inputIndex < inputChunks.size(); inputIndex++) {
        if (matchedChunks[inputIndex].getType() != inputChunks[inputIndex].getType()) {
            throw IRException("db.optional_match carries an input column back as another chunk type");
        }
    }

    for (size_t chunkIndex = inputChunks.size(); chunkIndex < matchedChunks.size(); chunkIndex++) {
        matchedChunks[chunkIndex] = paddedColumnChunk(matchedChunks[chunkIndex]);
    }

    setInsertionInto(deepestOwnerBlock(matchedChunks, stepBlock));
    _builder.create<nl::OptionalCollect>(loc, state, matchedTag, matchedChunks);

    if (patternLimit) {
        _builder.create<nl::LimitUpdate>(loc, patternLimit, matchedChunks.front());
    }

    _rootBlock = previousRoot;
    _innermostLoopBody = previousInnermostLoopBody;
    _innermostCardinality = previousInnermostCardinality;

    llvm::SmallVector<mlir::Type, 4> chunkTypes;
    for (const mlir::Value chunk : matchedChunks) {
        chunkTypes.push_back(chunk.getType());
    }

    const nl::IteratorType iteratorType = nl::IteratorType::get(_builder.getContext(), chunkTypes);
    setInsertionInto(stepBlock);
    nl::OptionalDrain drain = _builder.create<nl::OptionalDrain>(loc, iteratorType, state);

    buildLoopForSource(drain.getResult(), optionalMatchOp);
}

void DBLowering::lowerCallSubquery(mlir::db::CallSubquery call) {
    llvm::SmallVector<mlir::Value, 4> inputChunks;
    for (const mlir::Value column : call.getInputColumns()) {
        inputChunks.push_back(mapValue(column));
    }

    // The step the body runs over is the one binding the columns it reads, so its loops
    // nest in that block and re-run once per chunk of them.
    mlir::Block* const stepBlock = deepestOwnerBlock(inputChunks, _rootBlock);

    const bool returning = !call.getUnit();
    if (returning && call.getOptional()) {
        lowerOptionalSubquery(call, stepBlock, inputChunks);
        return;
    } else if (runsPerRow(call)) {
        lowerSubqueryPerRow(call, stepBlock, inputChunks);
        return;
    }

    mlir::Block* const previousInnermostLoopBody = _innermostLoopBody;
    const mlir::Value previousInnermostCardinality = _innermostCardinality;

    llvm::SmallVector<mlir::Value, 4> yieldedChunks;
    mlir::Value yieldedTag;
    lowerSubqueryBody(call, stepBlock, inputChunks, yieldedChunks, yieldedTag);

    // A unit body leaves the rows in flight as they were, so what follows the op reads the
    // step's own chunks again. A body carrying its scope hands the rows on: its yielded
    // chunks are the results, bound in the body's innermost loop, which is where the rest
    // of the query goes on from - unless the body opened no loop and the step is still the
    // relation in flight.
    const bool bodyOpenedALoop = _innermostLoopBody != nullptr;
    if (!returning || !bodyOpenedALoop) {
        _innermostLoopBody = previousInnermostLoopBody;
        _innermostCardinality = previousInnermostCardinality;
    }

    if (!returning) {
        setInsertionInto(stepBlock);
        return;
    }

    const mlir::ResultRange results = call.getResults();
    for (size_t resultIndex = 0; resultIndex < results.size(); resultIndex++) {
        _valueMap[results[resultIndex]] = yieldedChunks[resultIndex];
    }
}

void DBLowering::lowerSubqueryBody(mlir::db::CallSubquery call,
                                   mlir::Block* stepBlock,
                                   llvm::ArrayRef<mlir::Value> inputChunks,
                                   llvm::SmallVectorImpl<mlir::Value>& yieldedChunks,
                                   mlir::Value& yieldedTag) {
    mlir::Block& bodyBlock = call.getBody().front();
    for (size_t inputIndex = 0; inputIndex < inputChunks.size(); inputIndex++) {
        _valueMap[bodyBlock.getArgument(static_cast<unsigned>(inputIndex))] = inputChunks[inputIndex];
    }

    // A dataflow of its own, rooted in the step so its loops nest inside it - and so its
    // accumulators, hoisted to the root, reset once per step. The caller's root comes back
    // once the body is lowered; the innermost loop is left as the body set it.
    mlir::Block* const previousRoot = _rootBlock;
    _rootBlock = stepBlock;
    _innermostLoopBody = nullptr;
    _innermostCardinality = mlir::Value();

    for (mlir::Operation& operation : bodyBlock) {
        mlir::db::SubqueryYield yield = mlir::dyn_cast<mlir::db::SubqueryYield>(operation);
        if (!yield) {
            lowerOperation(operation);
            continue;
        }

        for (const mlir::Value column : yield.getColumns()) {
            yieldedChunks.push_back(mapValue(column));
        }

        if (yield.getTag()) {
            yieldedTag = mapValue(yield.getTag());
        }
    }

    _rootBlock = previousRoot;
}

void DBLowering::lowerSubqueryPerRow(mlir::db::CallSubquery call,
                                     mlir::Block* stepBlock,
                                     llvm::ArrayRef<mlir::Value> inputChunks) {
    const mlir::Location loc = _builder.getUnknownLoc();
    const bool unit = call.getUnit();

    mlir::Block* const previousInnermostLoopBody = _innermostLoopBody;
    const mlir::Value previousInnermostCardinality = _innermostCardinality;

    llvm::SmallVector<mlir::Value, 4> yieldedChunks;

    // No input column means the step is the single empty row Cypher starts from: the body
    // runs once, in the step block, and what it yields is the result
    if (inputChunks.empty()) {
        hoistLimitHandles(call.getBody(), stepBlock, call);

        mlir::Value yieldedTag;
        lowerSubqueryBody(call, stepBlock, inputChunks, yieldedChunks, yieldedTag);

        if (unit) {
            _innermostLoopBody = nullptr;
            _innermostCardinality = mlir::Value();

            setInsertionInto(stepBlock);
        } else if (!_innermostLoopBody) {
            _innermostLoopBody = previousInnermostLoopBody;
            _innermostCardinality = previousInnermostCardinality;

            setInsertionInto(stepBlock);
        }

        const mlir::ResultRange results = call.getResults();
        for (size_t resultIndex = 0; resultIndex < results.size(); resultIndex++) {
            _valueMap[results[resultIndex]] = yieldedChunks[resultIndex];
        }

        return;
    }

    // The loop over the step's rows carries the handle of a limit downstream of the op,
    // as the loops feeding that limit do, so a spent budget stops the walk over the rows
    const mlir::Value limitHandle = loopLimitHandle(call.getOperation());

    setInsertionInto(stepBlock);
    nl::EachRow eachRow = _builder.create<nl::EachRow>(loc, inputChunks);
    nl::For rowLoop = _builder.create<nl::For>(loc, eachRow.getResult(), limitHandle);
    mlir::Block* const rowBody = rowLoop.getBody();

    llvm::SmallVector<mlir::Value, 4> rowChunks;
    for (const mlir::BlockArgument rowChunk : rowBody->getArguments()) {
        rowChunks.push_back(rowChunk);
    }

    // The body roots in the row loop, so its accumulators and the handles of its own
    // limits are hoisted into that body and cover one input row at a time
    hoistLimitHandles(call.getBody(), rowBody, call);

    mlir::Value yieldedTag;
    lowerSubqueryBody(call, rowBody, rowChunks, yieldedChunks, yieldedTag);

    // A unit body hands nothing back, so there is no chunk to pair the row with and no
    // relation for what follows to walk: the step's own rows are in flight still
    if (unit) {
        _innermostLoopBody = previousInnermostLoopBody;
        _innermostCardinality = previousInnermostCardinality;

        setInsertionInto(stepBlock);
        return;
    }

    // A constant the body returned stands for one value over the rows beside it rather
    // than for a row of its own, and the product reads its row count off the first chunk
    // it is handed, so the constants are laid out over those rows first.
    rowAlignFactorChunks(yieldedChunks);

    // One row against N pairs the input row with each of the N rows the body yielded for
    // it, which is the op's result: the inputs then the body's columns
    setInsertionInto(deepestOwnerBlock(yieldedChunks, rowBody));
    nl::CrossProduct cross = _builder.create<nl::CrossProduct>(loc, rowChunks, yieldedChunks);

    buildLoopForSource(cross.getResult(), call.getOperation());
}

void DBLowering::lowerOptionalSubquery(mlir::db::CallSubquery call,
                                       mlir::Block* stepBlock,
                                       llvm::ArrayRef<mlir::Value> inputChunks) {
    const mlir::Location loc = _builder.getUnknownLoc();

    // A body run one row at a time makes the accumulator's step one input row: the loop
    // over the rows is opened first and the body roots in it. A body carrying the scope,
    // and one over the single empty row, run in the step block as they otherwise would.
    const bool perRow = runsPerRow(call) && !inputChunks.empty();

    mlir::Block* bodyRoot = stepBlock;
    llvm::SmallVector<mlir::Value, 4> stepChunks(inputChunks.begin(), inputChunks.end());

    if (perRow) {
        const mlir::Value limitHandle = loopLimitHandle(call.getOperation());

        setInsertionInto(stepBlock);
        nl::EachRow eachRow = _builder.create<nl::EachRow>(loc, inputChunks);
        nl::For rowLoop = _builder.create<nl::For>(loc, eachRow.getResult(), limitHandle);
        bodyRoot = rowLoop.getBody();

        stepChunks.clear();
        for (const mlir::BlockArgument rowChunk : bodyRoot->getArguments()) {
            stepChunks.push_back(rowChunk);
        }
    }

    setInsertionInto(bodyRoot);
    nl::OptionalBuffer buffer = _builder.create<nl::OptionalBuffer>(loc, stepChunks);
    const mlir::Value state = buffer.getState();

    // A body carrying the scope reads the tag through its trailing argument and hands it
    // back; the others are tagged here
    mlir::Block& bodyBlock = call.getBody().front();
    if (bodyBlock.getNumArguments() > inputChunks.size()) {
        _valueMap[bodyBlock.getArgument(static_cast<unsigned>(inputChunks.size()))] = buffer.getTag();
    }

    if (runsPerRow(call)) {
        hoistLimitHandles(call.getBody(), bodyRoot, call);
    }

    llvm::SmallVector<mlir::Value, 4> yieldedChunks;
    mlir::Value yieldedTag;
    lowerSubqueryBody(call, bodyRoot, stepChunks, yieldedChunks, yieldedTag);

    // The accumulator appends one buffer per column and the drain reads them back by
    // position, so a constant the body returned is laid out over the rows beside it before
    // any of them is collected.
    rowAlignFactorChunks(yieldedChunks);

    // A body carrying the scope hands the input columns back ahead of its own. The drain
    // rebuilds those out of the step's chunks rather than padding them, so they stay the
    // chunk the accumulator recorded; only the body's own columns need a null to pad with.
    const size_t carriedInputs = call.getCarriesScope() ? stepChunks.size() : 0;

    for (size_t inputIndex = 0; inputIndex < carriedInputs; inputIndex++) {
        if (yieldedChunks[inputIndex].getType() != stepChunks[inputIndex].getType()) {
            throw IRException("db.call_subquery carries an input column back as another chunk type");
        }
    }

    for (size_t chunkIndex = carriedInputs; chunkIndex < yieldedChunks.size(); chunkIndex++) {
        yieldedChunks[chunkIndex] = paddedColumnChunk(yieldedChunks[chunkIndex]);
    }

    // What the collect appends: the inputs as the body left them then its own columns
    // when it carries the scope, and the row's chunks crossed with its columns when it
    // runs per row - one row against N, with the tag in the outer group so it lines up
    llvm::SmallVector<mlir::Value, 4> collected;
    mlir::Value collectedTag = yieldedTag;

    if (perRow) {
        llvm::SmallVector<mlir::Value, 4> outer(stepChunks.begin(), stepChunks.end());
        outer.push_back(buffer.getTag());

        setInsertionInto(deepestOwnerBlock(yieldedChunks, bodyRoot));
        nl::CrossProduct cross = _builder.create<nl::CrossProduct>(loc, outer, yieldedChunks);
        nl::For pairs = _builder.create<nl::For>(loc, cross.getResult(), mlir::Value {});

        const mlir::Block::BlockArgListType pairChunks = pairs.getBody()->getArguments();
        const size_t inputCount = stepChunks.size();

        collected.assign(pairChunks.begin(), pairChunks.begin() + inputCount);
        collectedTag = pairChunks[inputCount];
        collected.append(pairChunks.begin() + inputCount + 1, pairChunks.end());
    } else {
        collected.assign(yieldedChunks.begin(), yieldedChunks.end());
    }

    setInsertionInto(deepestOwnerBlock(collected, bodyRoot));
    _builder.create<nl::OptionalCollect>(loc, state, collectedTag, collected);

    llvm::SmallVector<mlir::Type, 4> chunkTypes;
    for (const mlir::Value chunk : collected) {
        chunkTypes.push_back(chunk.getType());
    }

    // The drain's loop binds the op's results: the inputs then the body's columns, padded
    // where it yielded nothing
    const nl::IteratorType iteratorType = nl::IteratorType::get(_builder.getContext(), chunkTypes);
    setInsertionInto(bodyRoot);
    nl::OptionalDrain drain = _builder.create<nl::OptionalDrain>(loc, iteratorType, state);

    buildLoopForSource(drain.getResult(), call.getOperation());
}
