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

Yield mlir::db::factorYield(Operation* op, unsigned factorIndex) {
    return cast<Yield>(op->getRegion(factorIndex).front().getTerminator());
}

bool mlir::db::isHashableKey(const JoinKeySide& side) {
    const Type keyType = cast<ColumnType>(side._key.getType()).getType();

    return !isa<storage::PathType, storage::PathRefType>(keyType);
}
