#include "DBLowering.h"

#include "mlir/IR/Block.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/Types.h"
#include "mlir/Transforms/RegionUtils.h"

#include "NLOps.h"
#include "DBLoweringRowFlow.h"

#include "IRConstantColumn.h"
#include "IRRowAlignment.h"
#include "NullLiteralType.h"

#include "llvm/ADT/SmallPtrSet.h"
#include "llvm/Support/Casting.h"

using namespace db;

namespace nl = mlir::nl;
namespace storage = mlir::storage;

bool DBLowering::isConstantReadAsRows(mlir::Value chunk) {
    const auto chunkType = mlir::dyn_cast<nl::ChunkType>(chunk.getType());
    if (!chunkType) {
        return false;
    }

    return isReadAsRows(chunkType.getElementType()) && yieldsConstantColumn(chunk, _constantColumns);
}

bool DBLowering::readsConstantsAsRows(mlir::Operation& operation) {
    return llvm::any_of(operation.getOpOperands(), [this](mlir::OpOperand& operand) {
        const mlir::Value chunk = _valueMap.lookup(operand.get());
        return chunk && !readsAheadOfTheRows(operand) && isConstantReadAsRows(chunk);
    });
}

mlir::Value DBLowering::rowsReadBy(mlir::Operation& operation) {
    llvm::SmallVector<mlir::Value, 4> chunks;
    for (mlir::OpOperand& operand : operation.getOpOperands()) {
        const mlir::Value chunk = _valueMap.lookup(operand.get());
        if (chunk && !readsAheadOfTheRows(operand)) {
            chunks.push_back(chunk);
        }
    }

    return cardinalityDriver(chunks);
}

mlir::Value DBLowering::rowsInFlight() const {
    if (_innermostCardinality) {
        return _innermostCardinality;
    }

    // With no relation in flight, an op of a body run once per row, or of a WHEN branch,
    // reads the one row each step of the nl.each_row its dataflow is rooted in binds
    nl::For rootLoop = mlir::dyn_cast_or_null<nl::For>(_rootBlock->getParentOp());
    const bool bindsOneRow = rootLoop && rootLoop.getIterator().getDefiningOp<nl::EachRow>();

    return bindsOneRow ? _rootBlock->getArgument(0) : mlir::Value();
}

void DBLowering::lowerWithConstantsLaidOut(mlir::Operation& operation,
                                           mlir::Value cardinality,
                                           ChunkPredicate laysOut) {
    const size_t enclosingLaidOutCount = _laidOutOperands.size();

    for (mlir::OpOperand& use : operation.getOpOperands()) {
        const mlir::Value operand = use.get();
        const mlir::Value chunk = _valueMap.lookup(operand);
        if (!chunk || readsAheadOfTheRows(use) || !laysOut(chunk)) {
            continue;
        }

        const mlir::Value laidOutChunk = rowAlignedChunk(chunk, cardinality);
        _laidOutOperands.push_back(LaidOutOperand {operand, chunk, laidOutChunk});
        _valueMap[operand] = laidOutChunk;
    }

    lowerOperationByKind(operation);

    while (_laidOutOperands.size() > enclosingLaidOutCount) {
        const LaidOutOperand laidOut = _laidOutOperands.pop_back_val();
        _valueMap[laidOut._operand] = laidOut._constantChunk;
    }
}

bool DBLowering::computesOnceOverConstants(mlir::Operation& operation) {
    const bool constantThroughOperands = operation.hasTrait<mlir::OpTrait::ConstantThroughOperands>();

    return constantThroughOperands && yieldsConstantColumn(operation.getResult(0), _constantColumns);
}

bool DBLowering::raisesOverConstants(mlir::Operation& operation) {
    const bool constantThroughOperands = operation.hasTrait<mlir::OpTrait::ConstantThroughOperands>();
    const bool readsConstantsAlone = constantThroughOperands && llvm::all_of(operation.getOperands(), [this](mlir::Value operand) {
        return yieldsConstantColumn(mapValue(operand), _constantColumns);
    });

    return readsConstantsAlone && !runsWithoutRaising(&operation);
}

void DBLowering::lowerOverReachingRows(mlir::Operation& operation) {
    lowerWithConstantsLaidOut(operation, rowsInFlight(), [](mlir::Value) { return true; });
}

void DBLowering::lowerOnceOverConstants(mlir::Operation& operation) {
    const mlir::Value previousInnermostCardinality = _innermostCardinality;
    _innermostCardinality = {};

    lowerWithConstantsLaidOut(operation, mlir::Value(), [this](mlir::Value chunk) {
        return isConstantReadAsRows(chunk);
    });

    _innermostCardinality = previousInnermostCardinality;

    for (const mlir::Value result : operation.getResults()) {
        const mlir::Value row = mapValue(result);
        if (yieldsConstantColumn(row, _constantColumns)) {
            if (mlir::Operation* const definition = row.getDefiningOp()) {
                moveAheadOfLoops(definition);
            }

            continue;
        }

        _builder.setInsertionPointAfterValue(row);

        nl::RowConstant constant = _builder.create<nl::RowConstant>(_builder.getUnknownLoc(), row);
        _valueMap[result] = constant.getResult();

        moveAheadOfLoops(constant.getOperation());
    }
}

void DBLowering::moveAheadOfLoops(mlir::Operation* computation) {
    mlir::Block* const block = computation->getBlock();

    const auto loopIt = llvm::find_if(*block, [](mlir::Operation& op) { return mlir::isa<nl::For>(op); });
    const bool followsALoop = loopIt != block->end() && loopIt->isBeforeInBlock(computation);
    if (!followsALoop) {
        return;
    }

    mlir::Operation* const firstLoop = &*loopIt;

    llvm::SmallVector<mlir::Operation*> cone;
    llvm::DenseSet<mlir::Operation*> visited;
    llvm::SmallVector<mlir::Operation*> worklist {computation};

    while (!worklist.empty()) {
        mlir::Operation* const op = worklist.pop_back_val();
        if (!visited.insert(op).second) {
            continue;
        }

        cone.push_back(op);

        op->walk([&](mlir::Operation* nested) {
            for (const mlir::Value operand : nested->getOperands()) {
                mlir::Operation* const definingOp = operand.getDefiningOp();
                const bool definedPastTheLoop = definingOp
                                             && definingOp->getBlock() == block
                                             && firstLoop->isBeforeInBlock(definingOp);
                if (definedPastTheLoop) {
                    worklist.push_back(definingOp);
                }
            }
        });
    }

    llvm::sort(cone, [](mlir::Operation* lhs, mlir::Operation* rhs) { return lhs->isBeforeInBlock(rhs); });

    for (mlir::Operation* const op : cone) {
        op->moveBefore(firstLoop);
    }
}

void DBLowering::lowerBoundValuesAhead(mlir::func::FuncOp dbFunction) {
    dbFunction.walk([this](mlir::Operation* op) {
        llvm::SmallVector<mlir::Value, 3> boundValues;
        if (mlir::db::Limit limit = mlir::dyn_cast<mlir::db::Limit>(op)) {
            boundValues.push_back(limit.getBoundCount());
        } else if (mlir::db::Skip skip = mlir::dyn_cast<mlir::db::Skip>(op)) {
            boundValues.push_back(skip.getBoundCount());
        } else if (mlir::db::VectorSearch search = mlir::dyn_cast<mlir::db::VectorSearch>(op)) {
            boundValues.append({search.getBoundIndexName(), search.getBoundK(), search.getBoundQueryVector()});
        }

        for (const mlir::Value bound : boundValues) {
            if (bound && readsNoRow(bound)) {
                lowerValueAhead(bound);
            }
        }
    });
}

void DBLowering::lowerValueAhead(mlir::Value bound) {
    llvm::SmallVector<mlir::Operation*> ahead;
    llvm::SmallVector<std::pair<mlir::Operation*, bool>, 8> worklist;
    llvm::SmallPtrSet<mlir::Operation*, 8> visited;

    const auto pushDefinition = [this, &worklist, &visited](mlir::Value value) {
        mlir::Operation* const definingOp = value.getDefiningOp();
        if (definingOp && !_valueMap.contains(value) && visited.insert(definingOp).second) {
            worklist.emplace_back(definingOp, false);
        }
    };

    pushDefinition(bound);

    while (!worklist.empty()) {
        const auto [op, operandsPushed] = worklist.back();
        if (operandsPushed) {
            ahead.push_back(op);
            worklist.pop_back();
            continue;
        }

        worklist.back().second = true;

        for (const mlir::Value operand : op->getOperands()) {
            pushDefinition(operand);
        }

        mlir::visitUsedValuesDefinedAbove(op->getRegions(), [&pushDefinition](mlir::OpOperand* operand) {
            pushDefinition(operand->get());
        });
    }

    for (mlir::Operation* const op : ahead) {
        if (op->hasTrait<mlir::OpTrait::ConstantThroughOperands>()) {
            lowerOnceOverConstants(*op);
        } else {
            lowerOperation(*op);
        }

        _loweredAhead.insert(op);
    }
}

void DBLowering::followCardinalityThrough(mlir::ValueRange inputChunks, mlir::ValueRange resultChunks) {
    if (!_innermostCardinality) {
        return;
    }

    // Any chunk of the driver's relation stands for it: a cut trimmed down to a column other
    // than the driver itself still narrows the relation the driver counts.
    for (size_t chunkIndex = 0; chunkIndex < inputChunks.size(); chunkIndex++) {
        if (!rowAlignedWith(inputChunks[chunkIndex], _innermostCardinality)) {
            continue;
        }

        _innermostCardinality = resultChunks[chunkIndex];
        return;
    }
}

void DBLowering::rowAlignFactorChunks(llvm::SmallVectorImpl<mlir::Value>& chunks) {
    mlir::Value cardinality;
    for (const mlir::Value chunk : chunks) {
        if (!yieldsConstantColumn(chunk, _constantColumns)) {
            cardinality = chunk;
            break;
        }
    }

    for (mlir::Value& chunk : chunks) {
        chunk = rowAlignedChunk(chunk, cardinality);
    }
}

// A buffer holds rows, and a constant holds one value standing for every row of the step
// instead, so what an accumulator would append is a column with no rows in it. The
// constants are laid out first: over the relation driving the step, or - where none drives
// it - over the single row the projection is.
void DBLowering::rowAlignBufferedChunks(llvm::MutableArrayRef<mlir::Value> chunks) {
    const mlir::Value cardinality = cardinalityDriver(chunks);

    for (mlir::Value& chunk : chunks) {
        chunk = rowAlignedChunk(chunk, cardinality);
    }
}

void DBLowering::rowAlignCutChunks(llvm::SmallVectorImpl<mlir::Value>& chunks) {
    // With no relation driving the projection a constant is the single row that
    // projection is, and the cut charges it as it stands.
    const mlir::Value rows = rowsInFlight();
    if (!rows) {
        return;
    }

    for (mlir::Value& chunk : chunks) {
        chunk = rowAlignedChunk(chunk, rows);
    }
}

mlir::Value DBLowering::cardinalityDriver(llvm::ArrayRef<mlir::Value> chunks) {
    for (const mlir::Value chunk : chunks) {
        if (!yieldsConstantColumn(chunk, _constantColumns)) {
            return chunk;
        }
    }

    return rowsInFlight();
}

mlir::Value DBLowering::rowAlignedChunk(mlir::Value chunk, mlir::Value cardinality) {
    // A chunk that carries rows is its own alignment
    if (!yieldsConstantColumn(chunk, _constantColumns)) {
        return chunk;
    }

    const auto chunkType = mlir::cast<nl::ChunkType>(chunk.getType());
    mlir::Type valueElement = chunkType.getElementType();
    const auto nullable = mlir::dyn_cast<storage::NullableType>(valueElement);
    if (nullable) {
        valueElement = nullable.getValueType();
    }

    // The rows are laid out as a nullable value chunk - present in every row - which is
    // what every fold, key serialization and reduction reads a value column as. A list or
    // a map literal is laid out as the plain container chunk an nl.collect drain emits
    // instead, since no row of a literal is absent; one a function answered may be null.
    // A node, an edge, their label sets or types, a path, a walk or a mask rides the plain
    // chunk every op takes it in, and an edge type a CASE left null keeps its null.
    mlir::MLIRContext* const context = _builder.getContext();
    const bool isContainer = llvm::isa<storage::ListType, storage::MapType>(valueElement);
    const bool staysPlain = !nullable && (isContainer || isReadAsRows(valueElement));
    const mlir::Type resultElement = staysPlain ? valueElement : nullableElementType(valueElement);
    const nl::ChunkType resultType = nl::ChunkType::get(context, resultElement);

    // With no relation driving the projection the value is laid out over the single row
    // that projection is, behind the constants and handles hoisted so far: a layout read
    // from a loop nested under it must not sit below that nest.
    mlir::Operation* const definition = chunk.getDefiningOp();

    if (cardinality) {
        setInsertionInto(ownerBlock(cardinality));
    } else if (!definition || definition->getBlock() == _entryBlock) {
        setInsertionBelowHoisted(definition);
    } else {
        _builder.setInsertionPointAfter(definition);
    }

    nl::BroadcastConstant broadcast = _builder.create<nl::BroadcastConstant>(_builder.getUnknownLoc(), resultType, chunk, cardinality);

    return broadcast.getResult();
}
