#include "DBProgramGenerator.h"

#include "expr/Operators.h"

#include "mlir/IR/Block.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/Region.h"
#include "mlir/IR/TypeRange.h"
#include "mlir/IR/Value.h"
#include "mlir/IR/ValueRange.h"
#include "llvm/ADT/SmallVector.h"
#include "llvm/ADT/STLExtras.h"

#include "DBColumnElement.h"
#include "DBOps.h"
#include "DBTypes.h"
#include "DBBoundColumns.h"
#include "StorageTypes.h"
#include "IRConstantColumn.h"

#include "decl/EvaluatedType.h"
#include "expr/CaseExpr.h"
#include "expr/Expr.h"
#include "expr/ExprChain.h"
#include "expr/MapProjectionExpr.h"
#include "expr/SymbolExpr.h"

#include "BioAssert.h"

using namespace db;

void DBProgramGenerator::translateMapProjectionExpr(const Expr* expr, const MapProjectionExpr* projection) {
    if (projection->getType() == EvaluatedType::Null) {
        _part._exprMap[expr] = nullConstantColumn();
        return;
    }

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));
    const mlir::db::ColumnType noneType = allocColumnType(mlir::NoneType::get(_mlirCtxt));

    CarrySet carrySet;
    collectElementCarrySet(carrySet);

    const llvm::SmallVector<mlir::Value> carried(carrySet._columns.begin(), carrySet._columns.end());

    auto caseOp = _opBuilder.create<mlir::db::LazyCase>(loc, noneType, carried, 2u);
    const mlir::MutableArrayRef<mlir::Region> regions = caseOp.getBranches();

    generateCaseRegion(regions[0], carried, carrySet, mlir::Value {}, [&](mlir::Value) {
        const mlir::Value base = getOrTranslateExprColumn(projection->getBase());
        return _opBuilder.create<mlir::db::IsNotNullOp>(loc, boolType, base).getResult();
    });

    generateCaseRegion(regions[1], carried, carrySet, mlir::Value {}, [&](mlir::Value) {
        return projectedMap(projection);
    });

    _part._exprMap[expr] = selectionResult(caseOp);
}

void DBProgramGenerator::translateCaseExpr(const Expr* expr, const CaseExpr* caseExpr) {
    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));
    const mlir::db::ColumnType noneType = allocColumnType(mlir::NoneType::get(_mlirCtxt));

    // The simple form compares its subject against every WHEN value, which is the generic
    // form with that equality as the condition
    const Expr* const subjectExpr = caseExpr->getSubject();
    mlir::Value subject;
    if (subjectExpr) {
        translateExpr(subjectExpr);
        bioassert(_part._exprMap.contains(subjectExpr), "CASE subject with no column.");
        subject = _part._exprMap.at(subjectExpr);
    }

    CarrySet carrySet;
    collectElementCarrySet(carrySet);

    llvm::SmallVector<mlir::Value> carried(carrySet._columns.begin(), carrySet._columns.end());
    if (subject && !yieldsConstantColumn(subject, _constantColumns)) {
        carried.push_back(subject);
    }

    const CaseExpr::Branches& branches = caseExpr->getBranches();
    const Expr* const elseExpr = caseExpr->getElseExpr();
    const size_t regionCount = 2 * branches.size() + (elseExpr ? 1 : 0);

    auto caseOp = _opBuilder.create<mlir::db::LazyCase>(loc,
                                                        noneType,
                                                        carried,
                                                        static_cast<unsigned>(regionCount));

    const mlir::MutableArrayRef<mlir::Region> regions = caseOp.getBranches();

    const auto translateValue = [this](const Expr* valueExpr) {
        translateExpr(valueExpr);
        bioassert(_part._exprMap.contains(valueExpr), "CASE branch with no value column.");

        return _part._exprMap.at(valueExpr);
    };

    size_t regionIndex = 0;
    for (const CaseExpr::Branch& branch : branches) {
        generateCaseRegion(regions[regionIndex], carried, carrySet, subject, [&](mlir::Value regionSubject) {
            mlir::Value condition;

            for (const CaseExpr::Test& test : branch._tests) {
                const mlir::Value tested = translateCaseTest(subjectExpr, regionSubject, test);

                if (condition) {
                    condition = _opBuilder.create<mlir::db::OrOp>(loc, boolType, condition, tested).getResult();
                } else {
                    condition = tested;
                }
            }

            return condition;
        });

        generateCaseRegion(regions[regionIndex + 1], carried, carrySet, subject, [&](mlir::Value) {
            return translateValue(branch._then);
        });

        regionIndex += 2;
    }

    if (elseExpr) {
        generateCaseRegion(regions[regionIndex], carried, carrySet, subject, [&](mlir::Value) {
            return translateValue(elseExpr);
        });
    }

    _part._exprMap[expr] = selectionResult(caseOp);
}

void DBProgramGenerator::generateCaseRegion(mlir::Region& region,
                                            llvm::ArrayRef<mlir::Value> carried,
                                            const CarrySet& carrySet,
                                            mlir::Value subject,
                                            CaseRegionGenerator generateValue) {
    llvm::SmallVector<mlir::Type> argumentTypes;
    llvm::SmallVector<mlir::Location> argumentLocations;

    for (const mlir::Value column : carried) {
        argumentTypes.push_back(column.getType());
        argumentLocations.push_back(_opBuilder.getUnknownLoc());
    }

    const mlir::OpBuilder::InsertionGuard guard(_opBuilder);
    mlir::Block* const block = _opBuilder.createBlock(&region, {}, argumentTypes, argumentLocations);
    const mlir::Block::BlockArgListType arguments = block->getArguments();

    const PartScope outerPart = _part;

    _part._exprMap.clear();
    rebindCarrySet(arguments, /*firstColumn=*/0, carrySet);
    rebindProjectedColumns(carrySet._columns, arguments.take_front(carrySet._columns.size()));

    const bool carriesSubject = carried.size() > carrySet._columns.size();
    const mlir::Value regionSubject = carriesSubject ? mlir::Value(arguments.back()) : subject;

    const mlir::Value value = generateValue(regionSubject);
    _opBuilder.create<mlir::db::CaseYield>(_opBuilder.getUnknownLoc(), value);

    _part = outerPart;
}

template <typename SelectionOp>
mlir::Value DBProgramGenerator::selectionResult(SelectionOp selection) {
    dropUnreadCarriedColumns(selection, selection.getColumnsToFilter());

    llvm::SmallVector<mlir::Value> selected;
    selection.collectSelectedValues(selected);

    mlir::Value result = selection.getResult();
    result.setType(allocColumnType(selectedColumnElement(_mlirCtxt, selected)));

    return result;
}

mlir::Value DBProgramGenerator::translateCaseTest(const Expr* subjectExpr,
                                                  mlir::Value subject,
                                                  const CaseExpr::Test& test) {
    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));

    const EvaluatedType subjectType = subjectExpr ? subjectExpr->getType() : EvaluatedType::Invalid;

    const bool subjectIsNull = subjectExpr
                               && (subjectType == EvaluatedType::Null || isUntypedNullColumn(subject));

    if (test._kind == CaseExpr::TestKind::IsNull) {
        if (subjectIsNull) {
            return constantBool(true);
        }

        return _opBuilder.create<mlir::db::IsNullOp>(loc, boolType, subject).getResult();
    } else if (test._kind == CaseExpr::TestKind::IsNotNull) {
        if (subjectIsNull) {
            return constantBool(false);
        }

        return _opBuilder.create<mlir::db::IsNotNullOp>(loc, boolType, subject).getResult();
    }

    translateExpr(test._value);
    bioassert(_part._exprMap.contains(test._value), "CASE branch with no condition column.");

    const mlir::Value value = _part._exprMap.at(test._value);
    const EvaluatedType valueType = test._value->getType();

    // A test the analyzer typed Null is null on every row - a comparison with a null, an
    // ordering across types with no order between them - so its branch is never taken.
    // IS NULL above is how a Cypher CASE tests for a null
    const bool neverTaken = test._resultType == EvaluatedType::Null;
    const bool comparesDisjointTypes = subjectExpr && comparesAsDisjointTypes(subjectType, valueType);

    const bool testsEquality = test._operator == BinaryOperator::Equal
                               || test._operator == BinaryOperator::NotEqual;

    if (!subject) {
        return value;
    } else if (neverTaken) {
        return constantBool(false);
    } else if (comparesDisjointTypes && testsEquality) {
        return disjointComparison(subject, value, test._operator == BinaryOperator::Equal);
    }

    switch (test._operator) {
        case BinaryOperator::Equal:
            return _opBuilder.create<mlir::db::EqOp>(loc, boolType, subject, value).getResult();
        break;
        case BinaryOperator::NotEqual:
            return _opBuilder.create<mlir::db::NeqOp>(loc, boolType, subject, value).getResult();
        break;
        case BinaryOperator::LessThan:
            return _opBuilder.create<mlir::db::LtOp>(loc, boolType, subject, value).getResult();
        break;
        case BinaryOperator::GreaterThan:
            return _opBuilder.create<mlir::db::GtOp>(loc, boolType, subject, value).getResult();
        break;
        case BinaryOperator::LessThanOrEqual:
            return _opBuilder.create<mlir::db::LteOp>(loc, boolType, subject, value).getResult();
        break;
        case BinaryOperator::GreaterThanOrEqual:
            return _opBuilder.create<mlir::db::GteOp>(loc, boolType, subject, value).getResult();
        break;
        default:
            throwError(fmt::format("Unsupported comparison in a CASE branch: {}",
                                   BinaryOperatorDescription::value(test._operator)),
                       test._value);
        break;
    }
}

mlir::Value DBProgramGenerator::createCase(mlir::ValueRange conditions, mlir::ValueRange values, mlir::Value defaultValue) {
    llvm::SmallVector<mlir::Value> selected(values.begin(), values.end());
    if (defaultValue) {
        selected.push_back(defaultValue);
    }

    const mlir::db::ColumnType resultType = allocColumnType(selectedColumnElement(_mlirCtxt, selected));

    return _opBuilder.create<mlir::db::Case>(_opBuilder.getUnknownLoc(),
                                             resultType,
                                             conditions,
                                             values,
                                             defaultValue).getResult();
}

void DBProgramGenerator::translateCoalesce(const Expr* expr, const ExprChain* args) {
    bioassert(args && !args->empty(), "coalesce() with no arguments.");

    const ExprChain::ExprVector& argExprs = args->getExprs();
    const mlir::db::ColumnType noneType = allocColumnType(mlir::NoneType::get(_mlirCtxt));

    CarrySet carrySet;
    collectElementCarrySet(carrySet);

    const llvm::SmallVector<mlir::Value> carried(carrySet._columns.begin(), carrySet._columns.end());

    auto coalesce = _opBuilder.create<mlir::db::Coalesce>(_opBuilder.getUnknownLoc(),
                                                          noneType,
                                                          carried,
                                                          static_cast<unsigned>(argExprs.size()));
    const mlir::MutableArrayRef<mlir::Region> regions = coalesce.getArguments();

    for (size_t argumentIndex = 0; argumentIndex < argExprs.size(); argumentIndex++) {
        const Expr* const argumentExpr = argExprs[argumentIndex];

        generateCaseRegion(regions[argumentIndex], carried, carrySet, mlir::Value {}, [&](mlir::Value) {
            return translateArg(argumentExpr);
        });
    }

    _part._exprMap[expr] = selectionResult(coalesce);
}
