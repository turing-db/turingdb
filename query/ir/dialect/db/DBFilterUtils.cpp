#include "DBFilterUtils.h"

#include "mlir/IR/ValueRange.h"

#include "llvm/ADT/DenseSet.h"
#include "llvm/ADT/SmallPtrSet.h"

#include "NullLiteralType.h"
#include "DBPassUtils.h"
#include "DBRowCone.h"

using namespace mlir;
using namespace mlir::db;

void mlir::db::bypassFilter(FilterOp filter) {
    const ResultRange filtered = filter.getFilteredColumns();
    const Operation::operand_range carried = filter.getColumnsToFilter();
    for (size_t index = 0; index < filtered.size(); index++) {
        filtered[index].replaceAllUsesWith(carried[index]);
    }
}

bool mlir::db::isUntypedNullMask(Value mask) {
    const auto columnType = dyn_cast<ColumnType>(mask.getType());

    return columnType && isNullLiteralType(columnType.getType());
}

Value mlir::db::conjoin(llvm::ArrayRef<Value> conjuncts, Location loc, mlir::RewriterBase& rewriter) {
    const Type boolType = boolColumnType(rewriter.getContext());

    Value mask = conjuncts.front();
    for (const Value conjunct : conjuncts.drop_front()) {
        const bool bothUnknown = isUntypedNullMask(mask) && isUntypedNullMask(conjunct);
        if (bothUnknown) {
            continue;
        }

        mask = rewriter.create<AndOp>(loc, boolType, mask, conjunct).getResult();
    }

    return mask;
}

void mlir::db::eraseUnusedConjunction(Value mask, mlir::RewriterBase& rewriter) {
    llvm::SmallPtrSet<Operation*, 8> erased;
    llvm::SmallVector<Operation*> pending {mask.getDefiningOp()};
    while (!pending.empty()) {
        Operation* const op = pending.pop_back_val();
        if (!op || erased.contains(op)) {
            continue;
        }

        AndOp conjunction = dyn_cast<AndOp>(op);
        const bool unusedConjunction = conjunction && conjunction.getResult().use_empty();
        if (!unusedConjunction) {
            continue;
        }

        pending.push_back(conjunction.getLhs().getDefiningOp());
        pending.push_back(conjunction.getRhs().getDefiningOp());
        erased.insert(op);
        rewriter.eraseOp(conjunction);
    }
}

void mlir::db::dropConjunct(FilterOp filter, Value conjunct, mlir::RewriterBase& rewriter) {
    const Value mask = filter.getMask();

    AndOp conjunction = mask != conjunct && conjunct.hasOneUse() ? dyn_cast<AndOp>(*conjunct.getUsers().begin()) : AndOp();
    const Value other = !conjunction ? Value() : (conjunction.getLhs() == conjunct ? conjunction.getRhs() : conjunction.getLhs());

    if (other && !isUntypedNullMask(other)) {
        rewriter.replaceAllUsesWith(conjunction.getResult(), other);
        rewriter.eraseOp(conjunction);
    } else {
        llvm::SmallVector<Value, 4> kept;
        collectConjuncts(mask, kept);
        llvm::erase(kept, conjunct);

        if (kept.empty()) {
            bypassFilter(filter);
            rewriter.eraseOp(filter);
        } else {
            rewriter.setInsertionPoint(filter);
            const Value keptMask = conjoin(kept, filter.getLoc(), rewriter);

            rewriter.modifyOpInPlace(filter, [&filter, keptMask]() {
                filter.getMaskMutable().assign(keptMask);
            });
        }

        eraseUnusedConjunction(mask, rewriter);
    }

    Operation* const conjunctDef = conjunct.getDefiningOp();
    if (!conjunctDef) {
        return;
    }

    eraseUnusedRowConeOf(conjunctDef, rewriter);
}

void mlir::db::dropConjuncts(FilterOp filter, llvm::ArrayRef<Value> conjuncts, mlir::RewriterBase& rewriter) {
    llvm::SmallDenseSet<Value, 4> dropped;
    for (const Value conjunct : conjuncts) {
        if (dropped.insert(conjunct).second) {
            dropConjunct(filter, conjunct, rewriter);
        }
    }
}

bool mlir::db::readsOnlyIntoTheFilter(Operation* op, FilterOp filter) {
    Operation* const filterOp = filter.getOperation();
    Block* const block = filterOp->getBlock();

    llvm::SmallVector<Operation*> pending(op->getUsers().begin(), op->getUsers().end());
    llvm::SmallPtrSet<Operation*, 16> visited;
    while (!pending.empty()) {
        Operation* const reader = pending.pop_back_val();
        if (reader == filterOp || !visited.insert(reader).second) {
            continue;
        }

        const bool computesTheMask = reader->getBlock() == block && computesFromTheRow(reader) && !reader->use_empty();
        if (!computesTheMask) {
            return false;
        }

        llvm::append_range(pending, reader->getUsers());
    }

    return true;
}

Operation* mlir::db::filteredSource(FilterOp filter) {
    const Operation::operand_range columns = filter.getColumnsToFilter();
    Operation* const source = columns.empty() ? nullptr : columns.front().getDefiningOp();

    const bool makesEveryColumn = llvm::all_of(columns, [source](Value column) {
        return column.getDefiningOp() == source;
    });

    return makesEveryColumn ? source : nullptr;
}

bool mlir::db::keepsTheSourceRows(Operation& op) {
    ExplorePaths exploration = dyn_cast<ExplorePaths>(op);
    const bool walksEachSeed = exploration && !exploration.getDistinct();

    return isa<FilterOp>(op) || walksEachSeed || isEdgeHop(&op) || computesPerRow(&op);
}

bool mlir::db::rowsReachTheFilter(Operation* source, FilterOp filter, bool (*keepsTheRows)(Operation&)) {
    llvm::SmallPtrSet<Operation*, 16> readers {source};

    for (Operation& op : llvm::make_range(std::next(Block::iterator(source)), Block::iterator(filter.getOperation()))) {
        const bool readsTheRows = llvm::any_of(op.getOperands(), [&readers](Value operand) {
            Operation* const def = operand.getDefiningOp();
            return def && readers.contains(def);
        });

        if (!readsTheRows) {
            continue;
        }

        if (!keepsTheRows(op)) {
            return false;
        }

        readers.insert(&op);
    }

    for (Operation* const reader : readers) {
        for (Operation* const user : reader->getUsers()) {
            if (user != filter.getOperation() && !readers.contains(user)) {
                return false;
            }
        }
    }

    return true;
}

void mlir::db::collectConjuncts(Value mask, llvm::SmallVectorImpl<Value>& conjuncts) {
    llvm::SmallVector<Value> pending {mask};
    while (!pending.empty()) {
        const Value conjunct = pending.pop_back_val();

        if (AndOp conjunction = conjunct.getDefiningOp<AndOp>()) {
            pending.push_back(conjunction.getRhs());
            pending.push_back(conjunction.getLhs());
        } else {
            conjuncts.push_back(conjunct);
        }
    }
}

bool mlir::db::isEqualityOfTheFilter(EqOp equality, FilterOp filter) {
    if (!equality) {
        return false;
    }

    const bool readOnce = equality.getResult().hasOneUse();
    const bool besideTheFilter = equality->getBlock() == filter->getBlock();

    return readOnce && besideTheFilter;
}
