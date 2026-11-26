#include "DBCrossProducts.h"

using namespace mlir;
using namespace mlir::db;

Operation::operand_range mlir::db::factorYieldColumns(mlir::Region& factor) {
    mlir::Block& factorBlock = factor.front();
    Yield yield = cast<Yield>(factorBlock.getTerminator());

    return yield.getColumns();
}

Value mlir::db::productFactorColumn(CrossProduct product, size_t resultIndex) {
    const Operation::operand_range leftColumns = factorYieldColumns(product.getLeftFactor());
    if (resultIndex < leftColumns.size()) {
        return leftColumns[resultIndex];
    }

    return factorYieldColumns(product.getRightFactor())[resultIndex - leftColumns.size()];
}

size_t mlir::db::leftFactorWidth(CrossProduct product) {
    return factorYieldColumns(product.getLeftFactor()).size();
}

void mlir::db::collectFactorTypes(Region& leftFactor, Region& rightFactor, llvm::SmallVectorImpl<Type>& types) {
    llvm::append_range(types, factorYieldColumns(leftFactor).getTypes());
    llvm::append_range(types, factorYieldColumns(rightFactor).getTypes());
}

void mlir::db::collectFactorReads(const MaskCone& cone, CrossProduct product, ConeCache& cache, FactorReads& reads) {
    const size_t leftWidth = leftFactorWidth(product);
    for (const Value input : cone._inputs) {
        if (cache.yieldsConstantColumn(input)) {
            continue;
        }

        const Value column = cache.climbFilters(input);
        if (column.getDefiningOp() != product.getOperation()) {
            reads._readsElsewhere = true;
            continue;
        }

        const size_t resultIndex = cast<OpResult>(column).getResultNumber();
        if (resultIndex < leftWidth) {
            reads._leftResults.push_back(resultIndex);
        } else {
            reads._rightResults.push_back(resultIndex);
        }
    }
}

bool ConeCache::yieldsConstantColumn(Value column) {
    return ::db::yieldsConstantColumn(column, _constantColumns);
}

const ConjunctCone& ConeCache::getConjunctCone(Value conjunct, CrossProduct product) {
    std::unique_ptr<ConjunctCone>& cached = _conjunctCones[{conjunct, product.getOperation()}];
    if (!cached) {
        cached = std::make_unique<ConjunctCone>();
        collectRowCone({conjunct}, product->getBlock(), cached->_cone);
        collectFactorReads(cached->_cone, product, *this, cached->_reads);
    }

    return *cached;
}

Value ConeCache::climbFilters(Value column) {
    llvm::SmallVector<Value> climbing;
    Value climbed = column;
    while (FilterOp filter = climbed.getDefiningOp<FilterOp>()) {
        const auto climbedIt = _climbedColumns.find(climbed);
        if (climbedIt != _climbedColumns.end()) {
            climbed = climbedIt->second;
            break;
        }

        climbing.push_back(climbed);
        climbed = filter.getColumnsToFilter()[cast<OpResult>(climbed).getResultNumber()];
    }

    for (const Value filtered : climbing) {
        _climbedColumns[filtered] = climbed;
    }

    return climbed;
}

void ConeCache::clear() {
    _constantColumns.clear();
    _conjunctCones.clear();
    _climbedColumns.clear();
}

FilterStack::FilterStack(Operation* rows)
    : _block(rows->getBlock()),
    _top(rows)
{
    followReaders(rows);
}

FilterOp FilterStack::getReader() const {
    if (_readElsewhere) {
        return nullptr;
    }

    FilterOp reader = nullptr;
    for (Operation* const user : _top->getUsers()) {
        FilterOp filter = dyn_cast<FilterOp>(user);
        if (!filter || filter == reader || !readsTheTop(filter)) {
            continue;
        } else if (reader) {
            return nullptr;
        }

        reader = filter;
    }

    return reader;
}

bool FilterStack::isExclusiveTo(FilterOp reader) const {
    return !_readElsewhere && llvm::all_of(_pendingFilters, [&reader](Operation* filter) {
        return filter == reader.getOperation();
    });
}

void FilterStack::climbTo(FilterOp reader) {
    _pendingFilters.erase(reader);
    _top = reader;
    followReaders(reader);
}

bool FilterStack::readsTheTop(FilterOp filter) const {
    const Operation::operand_range columns = filter.getColumnsToFilter();
    if (columns.empty() || filter->getBlock() != _block) {
        return false;
    }

    return llvm::all_of(columns, [this](Value column) {
        return column.getDefiningOp() == _top;
    });
}

void FilterStack::followReaders(Operation* level) {
    llvm::SmallVector<Operation*> pending(level->getUsers().begin(), level->getUsers().end());
    while (!pending.empty()) {
        Operation* const op = pending.pop_back_val();
        if (!_visited.insert(op).second) {
            continue;
        }

        const bool inTheBlock = op->getBlock() == _block;
        if (inTheBlock && computesFromTheRow(op)) {
            llvm::append_range(pending, op->getUsers());
        } else if (inTheBlock && isa<FilterOp>(op)) {
            _pendingFilters.insert(op);
        } else {
            _readElsewhere = true;
        }
    }
}

Yield mlir::db::factorYield(Operation* op, unsigned factorIndex) {
    return cast<Yield>(op->getRegion(factorIndex).front().getTerminator());
}

bool mlir::db::isHashableKey(const JoinKeySide& side) {
    const Type keyType = cast<ColumnType>(side._key.getType()).getType();

    return !isa<storage::PathType, storage::PathRefType>(keyType);
}
