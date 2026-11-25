#include "DBLowering.h"

#include <array>

#include "mlir/IR/Block.h"
#include "mlir/IR/Builders.h"

#include "NLOps.h"

#include "DBColumnElement.h"
#include "IRConstantColumn.h"

#include "IRValueTypes.h"

#include "IRException.h"
#include "llvm/ADT/SmallPtrSet.h"
#include "llvm/Support/Casting.h"

using namespace db;

namespace nl = mlir::nl;

void DBLowering::setInsertionAtStart(mlir::Block* block, mlir::Value count) {
    mlir::Operation* const definition = count ? count.getDefiningOp() : nullptr;
    if (definition && definition->getBlock() == block) {
        _builder.setInsertionPointAfter(definition);
    } else {
        _builder.setInsertionPointToStart(block);
    }
}

mlir::Block* DBLowering::deepestOwnerBlock(llvm::ArrayRef<mlir::Value> chunks, mlir::Block* fallback) {
    mlir::Block* deepest = fallback;

    for (const mlir::Value chunk : chunks) {
        deepest = deeperOfBlocks(deepest, ownerBlock(chunk));
    }

    return deepest;
}

void DBLowering::setWriteInsertion(mlir::Value inputChunk, mlir::Value valueChunk) {
    mlir::Value reference = inputChunk;
    mlir::Block* const block = deeperBlock(reference, valueChunk);
    if (ownerBlock(valueChunk) == block) {
        reference = valueChunk;
    }

    setInsertionInto(ownerBlock(reference));
}

void DBLowering::setInsertionAfterHoisted() {
    if (_lastHoisted) {
        _builder.setInsertionPointAfter(_lastHoisted);
    } else {
        _builder.setInsertionPointToStart(_entryBlock);
    }
}

void DBLowering::setInsertionBelowHoisted(mlir::Operation* lastOperandDef) {
    const bool operandFollowsTheRun = lastOperandDef
                                   && (!_lastHoisted || _lastHoisted->isBeforeInBlock(lastOperandDef));

    if (operandFollowsTheRun) {
        _builder.setInsertionPointAfter(lastOperandDef);
    } else {
        setInsertionAfterHoisted();
    }
}

void DBLowering::setInsertionForUnaryOp(mlir::Value operandChunk) {
    mlir::Block* const insertBlock = ownerBlock(operandChunk);
    mlir::Block* const constantBlock = _constantComputationBlock ? _constantComputationBlock : _entryBlock;

    const bool readsAConstant = yieldsConstantColumn(operandChunk, _constantColumns)
                             && enclosesBlock(insertBlock, constantBlock);

    mlir::Operation* const operandDef = operandChunk.getDefiningOp();

    if (!readsAConstant) {
        setInsertionInto(deepestOwnerBlock({operandChunk}, _rootBlock));
    } else if (insertBlock != constantBlock) {
        _builder.setInsertionPointToStart(constantBlock);
    } else if (constantBlock == _entryBlock) {
        setInsertionBelowHoisted(operandDef);
    } else if (operandDef) {
        _builder.setInsertionPointAfter(operandDef);
    } else {
        setInsertionAfterHoisted();
    }
}

mlir::Value DBLowering::deepestBoundChunk(llvm::ArrayRef<mlir::Value> chunks) {
    mlir::Value deepest;
    for (const mlir::Value chunk : chunks) {
        if (!deepest) {
            deepest = chunk;
            continue;
        }

        mlir::Block* const block = deeperBlock(deepest, chunk);
        if (ownerBlock(chunk) == block) {
            deepest = chunk;
        }
    }

    return deepest;
}

mlir::Block* DBLowering::deeperBlock(mlir::Value first, mlir::Value second) {
    return deeperOfBlocks(ownerBlock(first), ownerBlock(second));
}

mlir::Block* DBLowering::deeperOfBlocks(mlir::Block* first, mlir::Block* second) {
    if (first == second) {
        return first;
    }

    // A chunk bound in a block that encloses the other is read once per step of it - a
    // hoisted constant, a metadata tally, the one row a per-row body walks - so the op
    // reading both belongs in the deeper block.
    if (enclosesBlock(first, second)) {
        return second;
    } else if (enclosesBlock(second, first)) {
        return first;
    }

    throw IRException("db operands read together must be bound in the same loop");
}

mlir::Operation* DBLowering::topLevelNestOf(mlir::Block* block) const {
    if (block == _entryBlock) {
        return nullptr;
    }

    mlir::Operation* enclosing = block->getParentOp();
    while (enclosing->getBlock() != _entryBlock) {
        enclosing = enclosing->getBlock()->getParentOp();
    }

    return enclosing;
}

void DBLowering::buildLoopForSource(mlir::Value iterator, mlir::Operation* dbOp) {
    // The handle this loop carries, or null when it produces no limited column -
    // giving the iterator-only builder's unbounded loop. A producing loop carries
    // its limit's handle so the break unwinds the producing nest; a consumer loop
    // (lowered after the limit) is never in the map and stays unbounded.
    const mlir::Value limitHandle = loopLimitHandle(dbOp);
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

    // A loop over a call that binds no return value has no variable, so there is no chunk
    // here for a constant projection to be sized against.
    _innermostCardinality = loopBody->getNumArguments() > 0 ? loopBody->getArgument(0)
                                                           : mlir::Value();

    const mlir::ResultRange dbResults = dbOp->getResults();
    for (size_t resultIndex = 0; resultIndex < dbResults.size(); resultIndex++) {
        _valueMap[dbResults[resultIndex]] = loopBody->getArgument(static_cast<unsigned>(resultIndex));
    }
}

void DBLowering::setInsertionInto(mlir::Block* block) {
    // The next op goes before the block's terminator, after the siblings already lowered
    // here - or, in a block enclosing the region being lowered or the root, before the op
    // holding it.
    mlir::Operation* nest = nullptr;
    if (_innermostRegionBody) {
        nest = block->findAncestorOpInBlock(*_innermostRegionBody->getParentOp());
    }

    if (!nest) {
        nest = block->findAncestorOpInBlock(*_rootBlock->getParentOp());
    }

    _builder.setInsertionPoint(nest ? nest : block->getTerminator());
}

void DBLowering::setInsertionAfterProducingLoop(mlir::Block* updateBlock) {
    if (updateBlock == _rootBlock) {
        setInsertionInto(_rootBlock);
        return;
    }

    mlir::Operation* const enclosing = _rootBlock->findAncestorOpInBlock(*updateBlock->getParentOp());
    if (!enclosing) {
        setInsertionInto(_rootBlock);
        return;
    }

    _builder.setInsertionPointAfter(enclosing);
}

void DBLowering::setInsertionForBinaryOp(mlir::Value lhs, mlir::Value rhs) {
    const std::array<mlir::Value, 2> operands {lhs, rhs};
    setInsertionForNaryOp(operands);
}

void DBLowering::setInsertionForNaryOp(llvm::ArrayRef<mlir::Value> operands) {
    // If every operand is a top-level constant (in `_entryBlock`), then place the op
    // after the last defined constant, below the hoisted run. Otherwise place the op in
    // the more deeply nested block.
    mlir::Value deepest = operands.front();
    for (const mlir::Value operand : operands.drop_front()) {
        if (deeperBlock(deepest, operand) != ownerBlock(deepest)) {
            deepest = operand;
        }
    }

    mlir::Block* const insertBlock = ownerBlock(deepest);
    mlir::Block* const constantBlock = _constantComputationBlock ? _constantComputationBlock : _entryBlock;

    const bool allConstants = llvm::all_of(operands, [this](mlir::Value operand) { return yieldsConstantColumn(operand, _constantColumns); });
    const bool readsConstantsAlone = insertBlock == _entryBlock
                                  || (allConstants && enclosesBlock(insertBlock, constantBlock));

    if (!readsConstantsAlone) {
        setInsertionInto(insertBlock);
        return;
    }

    llvm::SmallPtrSet<const mlir::Operation*, 4> defs;
    for (const mlir::Value operand : operands) {
        if (const mlir::Operation* const definingOp = operand.getDefiningOp()) {
            defs.insert(definingOp);
        }
    }

    // Walk the block to find which of the defining ops appears last — the new op
    // must go after that one to stay before any nl.for that follows. Each op appears once
    // in the block, so stop as soon as every def is seen.
    mlir::Operation* lastDef = nullptr;
    size_t defsFound = 0;
    for (mlir::Operation& op : *constantBlock) {
        if (defs.contains(&op)) {
            lastDef = &op;
            defsFound++;

            if (defsFound == defs.size()) {
                break;
            }
        }
    }

    if (constantBlock == _entryBlock) {
        setInsertionBelowHoisted(lastDef);
    } else if (lastDef) {
        _builder.setInsertionPointAfter(lastDef);
    } else {
        _builder.setInsertionPointToStart(constantBlock);
    }
}

bool DBLowering::enclosesBlock(mlir::Block* outer, mlir::Block* inner) {
    if (outer == inner) {
        return true;
    }

    mlir::Operation* const parent = inner->getParentOp();

    return parent && outer->findAncestorOpInBlock(*parent);
}

mlir::Block* DBLowering::accumulatorUpdateBlock(mlir::Block* producingBlock) const {
    if (enclosesBlock(_rootBlock, producingBlock)) {
        return producingBlock;
    }

    return _rootBlock;
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
