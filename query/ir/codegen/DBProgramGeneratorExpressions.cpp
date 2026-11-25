#include "DBProgramGenerator.h"

#include <optional>
#include <span>
#include <string>
#include <string_view>

#include "expr/Operators.h"

#include "mlir/IR/Builders.h"
#include "mlir/IR/Value.h"
#include "llvm/ADT/SmallVector.h"
#include "llvm/ADT/StringRef.h"

#include "DBOps.h"
#include "DBTypes.h"
#include "DBBoundColumns.h"
#include "StorageTypes.h"
#include "IRConstantColumn.h"
#include "IRValueTypes.h"

#include "decl/EvaluatedType.h"
#include "decl/ParameterDecl.h"
#include "decl/VarDecl.h"
#include "Literal.h"
#include "expr/BinaryExpr.h"
#include "expr/CaseExpr.h"
#include "expr/EntityTypeExpr.h"
#include "expr/ExistsExpr.h"
#include "expr/CountSubqueryExpr.h"
#include "expr/CollectSubqueryExpr.h"
#include "expr/Expr.h"
#include "expr/FunctionInvocationExpr.h"
#include "expr/IndexExpr.h"
#include "expr/ListComprehensionExpr.h"
#include "expr/ListPredicateExpr.h"
#include "expr/ReduceExpr.h"
#include "expr/AllReduceExpr.h"
#include "expr/MapProjectionExpr.h"
#include "expr/ListSliceExpr.h"
#include "expr/LiteralExpr.h"
#include "expr/LogicalExpr.h"
#include "expr/ParameterExpr.h"
#include "expr/PatternComprehensionExpr.h"
#include "expr/PropertyExpr.h"
#include "expr/PropertyLookupExpr.h"
#include "expr/StringExpr.h"
#include "expr/SymbolExpr.h"
#include "expr/UnaryExpr.h"

#include "BioAssert.h"
#include "FatalException.h"

using namespace db;

namespace {

// The type a list of @param shape is read under, as a literal of that shape infers it: the
// leaf's own type, none for a leaf no element names, any where the elements share none,
// under one list per level
mlir::Type listShapeType(mlir::OpBuilder& builder, const ListShape& shape) {
    mlir::MLIRContext* const context = builder.getContext();
    const EvaluatedType leaf = shape.getLeafType();
    const bool sharesNoType = leaf == EvaluatedType::Invalid;
    const std::optional<ValueType> leafValueType = sharesNoType ? std::nullopt : toValueType(leaf);

    mlir::Type element;
    if (leaf == EvaluatedType::Null) {
        element = mlir::NoneType::get(context);
    } else if (leafValueType) {
        element = valueTypeToElementType(builder, *leafValueType);
    } else {
        element = mlir::storage::AnyType::get(context);
    }

    for (size_t level = 0; level < shape.getDepth(); level++) {
        element = mlir::storage::ListType::get(context, element);
    }

    return element;
}

}

mlir::Value DBProgramGenerator::getOrTranslateExprColumn(const VariableColumnMap& variableColumns,
                                                         const Expr* expr) {
    const mlir::Value column = getOrTranslateBoundColumn(variableColumns, expr);

    return expr->getKind() == Expr::Kind::SYMBOL ? readWalkEntities(expr, column) : column;
}

mlir::Value DBProgramGenerator::getOrTranslateBoundColumn(const VariableColumnMap& variableColumns,
                                                          const Expr* expr) {
    // One column is held per variable, so an expression only has a column to be found
    // there when it is a variable and nothing else. Anything more is a computation over
    // columns, and has to be translated - as is a symbol naming no variable but the alias
    // of a projected item, which the translation resolves through the item that published
    // that column
    if (expr->getKind() == Expr::Kind::SYMBOL) {
        const VarDecl* var = expr->getExprVarDecl();
        bioassert(var, "Symbol expression without a declaration.");

        const auto findIt = variableColumns.find(var);
        if (findIt != end(variableColumns)) {
            return findIt->second;
        }
    }

    translateExprOverRows(expr);
    return _part._exprMap.at(expr);
}

mlir::Value DBProgramGenerator::getOrTranslateExprColumn(const Expr* expr) {
    const mlir::Value column = getOrTranslateBoundColumn(expr);

    return expr->getKind() == Expr::Kind::SYMBOL ? readWalkEntities(expr, column) : column;
}

// The column as bound, which for a group variable is its walk's handle rather than its list.
// Without a map, the one name the expression could name is looked up over the bindings
// rather than every one of them being gathered to answer for it
mlir::Value DBProgramGenerator::getOrTranslateBoundColumn(const Expr* expr) {
    if (expr->getKind() == Expr::Kind::SYMBOL) {
        const VarDecl* var = expr->getExprVarDecl();
        bioassert(var, "Symbol expression without a declaration.");

        if (const mlir::Value column = findVariableColumn(var)) {
            return column;
        }
    }

    translateExpr(expr);
    return _part._exprMap.at(expr);
}

void DBProgramGenerator::translateExpr(const Expr* expr) {
    if (_part._exprMap.contains(expr)) {
        return;
    }

    const Expr::Kind kind = expr->getKind();
    switch (kind) {
        case Expr::Kind::PROPERTY: {
            const PropertyExpr* propExpr = static_cast<const PropertyExpr*>(expr);
            _part._exprMap[expr] = translatePropertyExpr(propExpr);
        }
        break;

        case Expr::Kind::PROPERTY_LOOKUP: {
            const PropertyLookupExpr* lookupExpr = static_cast<const PropertyLookupExpr*>(expr);
            _part._exprMap[expr] = translatePropertyLookupExpr(lookupExpr);
        }
        break;

        case Expr::Kind::LITERAL: {
            const LiteralExpr* litExpr = static_cast<const LiteralExpr*>(expr);
            _part._exprMap[expr] = translateLiteralExpr(litExpr->getLiteral());
        }
        break;

        case Expr::Kind::BINARY: {
            const BinaryExpr* binExpr = static_cast<const BinaryExpr*>(expr);
            translateBinaryExpr(expr, binExpr);
        }
        break;

        case Expr::Kind::LOGICAL: {
            const LogicalExpr* logicalExpr = static_cast<const LogicalExpr*>(expr);
            translateLogicalExpr(expr, logicalExpr);
        }
        break;

        case Expr::Kind::SYMBOL: {
            const SymbolExpr* symbolExpr = static_cast<const SymbolExpr*>(expr);
            const VarDecl* decl = symbolExpr->getDecl();
            const std::string_view varName = decl->getName();

            // The symbol names either a traversal variable or the alias of a projected
            // item, and no traversal ever publishes a column under an alias: the item is
            // where that column comes from
            const auto projectedIt = _part._projectedColumns.find(decl);

            if (projectedIt != _part._projectedColumns.end()) {
                _part._exprMap[expr] = projectedIt->second;
            } else {
                const mlir::Value entityColumn = resolveEntityColumn(decl);
                if (entityColumn) {
                    _part._exprMap[expr] = entityColumn;
                }
            }

            // A variable a CALL yielded appears in no pattern, so it is no VDG variable;
            // its column is the one the call produced for that return value.
            if (!_part._exprMap.contains(expr)) {
                if (const mlir::Value yielded = findYieldedColumn(decl)) {
                    _part._exprMap[expr] = yielded;
                }
            }

            const bool bound = _part._exprMap.contains(expr);

            // The row a LOAD CSV bound is not a column of its own: the load publishes one
            // per field the query reads, and nothing stands for the whole record
            const bool namesACSVRow = decl->getType() == EvaluatedType::StringTable;
            if (namesACSVRow) {
                throwCSVRowRead(expr, varName);
            }

            bioassert(bound, "Symbol refers to unknown variable: {}", varName);

            _part._exprMap[expr] = readWalkEntities(expr, _part._exprMap.at(expr));
        }
        break;

        case Expr::Kind::UNARY: {
            const UnaryExpr* unaryExpr = static_cast<const UnaryExpr*>(expr);
            translateUnaryExpr(expr, unaryExpr);
        }
        break;

        case Expr::Kind::STRING: {
            translateStringExpr(expr);
        }
        break;

        case Expr::Kind::FUNCTION_INVOCATION: {
            const FunctionInvocationExpr* funcExpr = static_cast<const FunctionInvocationExpr*>(expr);
            translateFunctionInvocationExpr(expr, funcExpr);
        }
        break;

        case Expr::Kind::INDEX: {
            const IndexExpr* indexExpr = static_cast<const IndexExpr*>(expr);
            translateIndexExpr(expr, indexExpr);
        }
        break;

        case Expr::Kind::LIST_SLICE: {
            const ListSliceExpr* slice = static_cast<const ListSliceExpr*>(expr);
            translateListSliceExpr(expr, slice);
        }
        break;

        case Expr::Kind::ENTITY_TYPES: {
            const EntityTypeExpr* typeExpr = static_cast<const EntityTypeExpr*>(expr);
            _part._exprMap[expr] = translateEntityTypeExpr(typeExpr);
        }
        break;

        case Expr::Kind::CASE: {
            const CaseExpr* caseExpr = static_cast<const CaseExpr*>(expr);
            translateCaseExpr(expr, caseExpr);
        }
        break;

        case Expr::Kind::EXISTS: {
            const ExistsExpr* existsExpr = static_cast<const ExistsExpr*>(expr);
            translateExistsExpr(expr, existsExpr);
        }
        break;

        case Expr::Kind::COUNT_SUBQUERY: {
            const CountSubqueryExpr* countExpr = static_cast<const CountSubqueryExpr*>(expr);
            translateCountSubqueryExpr(expr, countExpr);
        }
        break;
        case Expr::Kind::COLLECT_SUBQUERY: {
            const CollectSubqueryExpr* collectExpr = static_cast<const CollectSubqueryExpr*>(expr);
            translateCollectSubqueryExpr(expr, collectExpr);
        }
        break;

        case Expr::Kind::LIST_COMPREHENSION: {
            const ListComprehensionExpr* comprehension = static_cast<const ListComprehensionExpr*>(expr);
            translateListComprehensionExpr(expr, comprehension);
        }
        break;

        case Expr::Kind::PATTERN_COMPREHENSION: {
            const PatternComprehensionExpr* comprehension =
                static_cast<const PatternComprehensionExpr*>(expr);
            translatePatternComprehensionExpr(expr, comprehension);
        }
        break;

        case Expr::Kind::LIST_PREDICATE: {
            const ListPredicateExpr* predicate = static_cast<const ListPredicateExpr*>(expr);
            translateListPredicateExpr(expr, predicate);
        }
        break;

        case Expr::Kind::REDUCE: {
            const ReduceExpr* reduce = static_cast<const ReduceExpr*>(expr);
            translateReduceExpr(expr, reduce);
        }
        break;

        case Expr::Kind::ALL_REDUCE: {
            const AllReduceExpr* allReduce = static_cast<const AllReduceExpr*>(expr);
            translateAllReduceExpr(expr, allReduce);
        }
        break;

        case Expr::Kind::MAP_PROJECTION: {
            const MapProjectionExpr* projection = static_cast<const MapProjectionExpr*>(expr);
            translateMapProjectionExpr(expr, projection);
        }
        break;

        case Expr::Kind::LIST:
            throwError(fmt::format("Unsupported expression: {}",
                                   ExprKindDescription::value(kind)),
                       expr);
        break;

        case Expr::Kind::PARAMETER:
            _part._exprMap[expr] = translateParameterExpr(static_cast<const ParameterExpr*>(expr));
        break;

        case Expr::Kind::_SIZE:
            throw FatalException("Invalid expression kind.");
        break;
    }
}

void DBProgramGenerator::translateUnaryExpr(const Expr* expr, const UnaryExpr* unaryExpr) {
    const UnaryOperator op = unaryExpr->getOperator();
    const Expr* subExpr = unaryExpr->getSubExpr();
    translateExpr(subExpr);
    bioassert(_part._exprMap.contains(subExpr), "Unary operation with unknown operand.");
    const mlir::Value operandVal = _part._exprMap.at(subExpr);
    const mlir::Location loc = _opBuilder.getUnknownLoc();

    switch (op) {
        case UnaryOperator::Not: {
            if (isUntypedNullColumn(operandVal)) {
                _part._exprMap[expr] = nullConstantColumn();
                break;
            }

            const mlir::db::ColumnType boolType =
                allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));
            auto notOp = _opBuilder.create<mlir::db::NotOp>(loc, boolType, operandVal);
            const mlir::Value result = notOp.getResult();
            _part._exprMap[expr] = result;
        }
        break;

        case UnaryOperator::Minus: {
            // Negating an unknown value is unknown: the null the operand is stands as the
            // negation
            if (expr->getType() == EvaluatedType::Null) {
                _part._exprMap[expr] = nullConstantColumn();
                break;
            }

            _part._exprMap[expr] = _opBuilder.create<mlir::db::Negate>(loc, operandVal).getResult();
        }
        break;

        case UnaryOperator::Plus: {
            const EvaluatedType operandType = subExpr->getType();
            if (operandType == EvaluatedType::Null) {
                _part._exprMap[expr] = nullConstantColumn();
                break;
            } else if (operandType != EvaluatedType::Any) {
                _part._exprMap[expr] = operandVal;
                break;
            }

            _part._exprMap[expr] = _opBuilder.create<mlir::db::UnaryPlus>(loc, operandVal).getResult();
        }
        break;

        case UnaryOperator::_SIZE:
            throwError("Unknown unary operator.", expr);
        break;
    }
}

mlir::Value DBProgramGenerator::disjointComparison(mlir::Value lhs, mlir::Value rhs, bool equality) {
    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));

    // A literal carries a value on every row it stands for, so only a column operand can be
    // absent, and a null test on the literal would meet its one row against the relation's
    llvm::SmallVector<mlir::Value, 2> columns;
    for (const mlir::Value operand : {lhs, rhs}) {
        if (!yieldsConstantColumn(operand, _constantColumns)) {
            columns.push_back(operand);
        }
    }

    if (columns.empty()) {
        return constantBool(!equality);
    }

    mlir::Value condition;
    for (const mlir::Value column : columns) {
        mlir::Value test;
        if (equality) {
            test = _opBuilder.create<mlir::db::IsNullOp>(loc, boolType, column).getResult();
        } else {
            test = _opBuilder.create<mlir::db::IsNotNullOp>(loc, boolType, column).getResult();
        }

        if (!condition) {
            condition = test;
        } else if (equality) {
            condition = _opBuilder.create<mlir::db::OrOp>(loc, boolType, condition, test).getResult();
        } else {
            condition = _opBuilder.create<mlir::db::AndOp>(loc, boolType, condition, test).getResult();
        }
    }

    if (equality) {
        return _opBuilder.create<mlir::db::AndOp>(loc, boolType, condition, nullConstantColumn()).getResult();
    }

    return _opBuilder.create<mlir::db::OrOp>(loc, boolType, condition, nullConstantColumn()).getResult();
}

void DBProgramGenerator::translateLogicalExpr(const Expr* expr, const LogicalExpr* logicalExpr) {
    const LogicalOperator op = logicalExpr->getOperator();

    mlir::Value result;
    for (const Expr* operandExpr : logicalExpr->getOperands()) {
        translateExpr(operandExpr);
        bioassert(_part._exprMap.contains(operandExpr), "Logical operation with unknown operand.");

        const mlir::Value operand = _part._exprMap.at(operandExpr);
        result = result ? translateLogicalOperator(op, result, operand) : operand;
    }

    _part._exprMap[expr] = result;
}

mlir::Value DBProgramGenerator::translateLogicalOperator(LogicalOperator op, mlir::Value lhs, mlir::Value rhs) {
    // AND, OR and XOR read a null side as an unknown boolean, which the runtime truth
    // tables answer against the other side's value per row. With both sides null there is
    // no value to answer against and the result is null, whichever of the three it is
    if (isUntypedNullColumn(lhs) && isUntypedNullColumn(rhs)) {
        return nullConstantColumn();
    }

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));

    switch (op) {
        case LogicalOperator::And:
            return _opBuilder.create<mlir::db::AndOp>(loc, boolType, lhs, rhs).getResult();
        break;
        case LogicalOperator::Or:
            return _opBuilder.create<mlir::db::OrOp>(loc, boolType, lhs, rhs).getResult();
        break;
        case LogicalOperator::Xor:
            return _opBuilder.create<mlir::db::XorOp>(loc, boolType, lhs, rhs).getResult();
        break;
        default:
            throwError("Unknown logical operator.");
        break;
    }
}

void DBProgramGenerator::translateBinaryExpr(const Expr* expr, const BinaryExpr* binExpr) {
    const Expr* lhsExpr = binExpr->getLHS();
    const Expr* rhsExpr = binExpr->getRHS();

    translateExpr(lhsExpr);
    translateExpr(rhsExpr);

    bioassert(_part._exprMap.contains(lhsExpr), "Binary operation with unknown LHS operand.");
    bioassert(_part._exprMap.contains(rhsExpr), "Binary operation with unknown RHS operand.");

    const mlir::Value lhs = _part._exprMap.at(lhsExpr);
    const mlir::Value rhs = _part._exprMap.at(rhsExpr);
    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));

    const BinaryOperator op = binExpr->getOperator();

    // Two types that can never hold equal values answer the equality without reading a row
    const bool comparesDisjointTypes = comparesAsDisjointTypes(lhsExpr->getType(), rhsExpr->getType());

    const bool testsKnownNull = lhsExpr->getType() == EvaluatedType::Null || isUntypedNullColumn(lhs);

    // The analyzer types Null an expression that is null on every row - a comparison with a
    // null, an ordering across types, arithmetic over a null - and the null names no column
    // the result could be carried in
    if (binExpr->getType() == EvaluatedType::Null) {
        _part._exprMap[expr] = nullConstantColumn();
        return;
    }

    switch (op) {
        case BinaryOperator::Equal:
            if (comparesDisjointTypes) {
                _part._exprMap[expr] = disjointComparison(lhs, rhs, /*equality=*/true);
            } else {
                _part._exprMap[expr] = _opBuilder.create<mlir::db::EqOp>(loc, boolType, lhs, rhs).getResult();
            }
        break;
        case BinaryOperator::IsNull:
            if (testsKnownNull) {
                _part._exprMap[expr] = constantBool(true);
            } else {
                _part._exprMap[expr] = _opBuilder.create<mlir::db::IsNullOp>(loc, boolType, lhs).getResult();
            }
        break;
        case BinaryOperator::IsNotNull:
            if (testsKnownNull) {
                _part._exprMap[expr] = constantBool(false);
            } else {
                _part._exprMap[expr] = _opBuilder.create<mlir::db::IsNotNullOp>(loc, boolType, lhs).getResult();
            }
        break;
        case BinaryOperator::Add: {
            const EvaluatedType resultType = binExpr->getType();

            // A type-erased cell joins a list as the list it holds or as one element of it,
            // which only its row says, so an add over one is decided row by row
            const bool addsACell = lhsExpr->getType() == EvaluatedType::Any
                                || rhsExpr->getType() == EvaluatedType::Any;
            const bool joinsAsText = resultType == EvaluatedType::String;
            const bool joinsAsList = resultType == EvaluatedType::List;
            const bool isConcatenation = !addsACell && (joinsAsText || joinsAsList);

            if (isConcatenation) {
                // A scalar concatenated with a list joins it as the list it stands for, so
                // both sides of the concatenation are lists
                const bool concatenatesLists = resultType == EvaluatedType::List;
                const bool lhsJoinsAList = concatenatesLists && lhsExpr->getType() != EvaluatedType::List;
                const bool rhsJoinsAList = concatenatesLists && rhsExpr->getType() != EvaluatedType::List;

                const mlir::Value lhsColumn = lhsJoinsAList ? joinedListOperand(lhs) : lhs;
                const mlir::Value rhsColumn = rhsJoinsAList ? joinedListOperand(rhs) : rhs;

                _part._exprMap[expr] = _opBuilder.create<mlir::db::ConcatOp>(loc, lhsColumn, rhsColumn).getResult();
            } else {
                _part._exprMap[expr] = _opBuilder.create<mlir::db::AddOp>(loc, lhs, rhs).getResult();
            }
        }
        break;
        case BinaryOperator::Concat:
            _part._exprMap[expr] = _opBuilder.create<mlir::db::ConcatOp>(loc, lhs, rhs).getResult();
        break;
        case BinaryOperator::Sub:
            _part._exprMap[expr] = _opBuilder.create<mlir::db::SubOp>(loc, lhs, rhs).getResult();
        break;
        case BinaryOperator::Mult:
            _part._exprMap[expr] = _opBuilder.create<mlir::db::MulOp>(loc, lhs, rhs).getResult();
        break;
        case BinaryOperator::Div:
            _part._exprMap[expr] = _opBuilder.create<mlir::db::DivOp>(loc, lhs, rhs).getResult();
        break;
        case BinaryOperator::GreaterThan:
            _part._exprMap[expr] = _opBuilder.create<mlir::db::GtOp>(loc, boolType, lhs, rhs).getResult();
        break;
        case BinaryOperator::LessThan:
            _part._exprMap[expr] = _opBuilder.create<mlir::db::LtOp>(loc, boolType, lhs, rhs).getResult();
        break;
        case BinaryOperator::GreaterThanOrEqual:
            _part._exprMap[expr] = _opBuilder.create<mlir::db::GteOp>(loc, boolType, lhs, rhs).getResult();
        break;
        case BinaryOperator::LessThanOrEqual:
            _part._exprMap[expr] = _opBuilder.create<mlir::db::LteOp>(loc, boolType, lhs, rhs).getResult();
        break;
        case BinaryOperator::NotEqual:
            if (comparesDisjointTypes) {
                _part._exprMap[expr] = disjointComparison(lhs, rhs, /*equality=*/false);
            } else {
                _part._exprMap[expr] = _opBuilder.create<mlir::db::NeqOp>(loc, boolType, lhs, rhs).getResult();
            }
        break;
        case BinaryOperator::Mod:
            _part._exprMap[expr] = _opBuilder.create<mlir::db::ModOp>(loc, lhs, rhs).getResult();
        break;
        case BinaryOperator::Pow:
            _part._exprMap[expr] = _opBuilder.create<mlir::db::PowOp>(loc, lhs, rhs).getResult();
        break;
        case BinaryOperator::In:
            _part._exprMap[expr] = _opBuilder.create<mlir::db::InOp>(loc, boolType, lhs, rhs).getResult();
        break;

        case BinaryOperator::_SIZE:
        break;
    }
}

void DBProgramGenerator::translateStringExpr(const Expr* expr) {
    const StringExpr* strExpr = static_cast<const StringExpr*>(expr);

    const Expr* lhsExpr = strExpr->getLHS();
    const Expr* rhsExpr = strExpr->getRHS();

    translateExpr(lhsExpr);
    translateExpr(rhsExpr);

    bioassert(_part._exprMap.contains(lhsExpr), "String operation with unknown LHS operand.");
    bioassert(_part._exprMap.contains(rhsExpr), "String operation with unknown RHS operand.");

    const mlir::Value lhs = _part._exprMap.at(lhsExpr);
    const mlir::Value rhs = _part._exprMap.at(rhsExpr);
    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));

    const StringOperator op = strExpr->getStringOperator();

    switch (op) {
        case StringOperator::StartsWith:
            _part._exprMap[expr] = _opBuilder.create<mlir::db::StartsWithOp>(loc, boolType, lhs, rhs).getResult();
        break;
        case StringOperator::EndsWith:
            _part._exprMap[expr] = _opBuilder.create<mlir::db::EndsWithOp>(loc, boolType, lhs, rhs).getResult();
        break;
        case StringOperator::Contains:
            _part._exprMap[expr] = _opBuilder.create<mlir::db::ContainsOp>(loc, boolType, lhs, rhs).getResult();
        break;
        case StringOperator::_SIZE:
            throwError("Unknown string operator.", expr);
        break;
    }
}

mlir::TypedAttr DBProgramGenerator::scalarLiteralAttr(const Literal* literal) {
    switch (literal->getKind()) {
        case Literal::Kind::BOOL: {
            const BoolLiteral* boolLiteral = static_cast<const BoolLiteral*>(literal);
            return _opBuilder.getBoolAttr(boolLiteral->getValue());
        }
        break;

        case Literal::Kind::INTEGER: {
            const IntegerLiteral* intLiteral = static_cast<const IntegerLiteral*>(literal);
            return _opBuilder.getI64IntegerAttr(intLiteral->getValue());
        }
        break;

        case Literal::Kind::DOUBLE: {
            const DoubleLiteral* doubleLiteral = static_cast<const DoubleLiteral*>(literal);
            return _opBuilder.getF64FloatAttr(doubleLiteral->getValue());
        }
        break;

        case Literal::Kind::STRING: {
            const StringLiteral* stringLiteral = static_cast<const StringLiteral*>(literal);
            const mlir::Type stringType = mlir::storage::StringType::get(_mlirCtxt);
            return mlir::StringAttr::get(stringLiteral->getValue(), stringType);
        }
        break;

        default:
            return {};
        break;
    }
}

mlir::DenseF32ArrayAttr DBProgramGenerator::embeddingLiteralAttr(const EmbeddingLiteral* literal) {
    const std::span<const float> floats = literal->getValue();

    return mlir::DenseF32ArrayAttr::get(_mlirCtxt, llvm::ArrayRef<float> {floats.data(), floats.size()});
}

mlir::Value DBProgramGenerator::translateLiteralExpr(const Literal* literal) {
    const mlir::Location uloc = _opBuilder.getUnknownLoc();

    mlir::TypedAttr valueAttr;

    switch (literal->getKind()) {
        case Literal::Kind::BOOL:
        case Literal::Kind::INTEGER:
        case Literal::Kind::DOUBLE:
        case Literal::Kind::STRING:
            valueAttr = scalarLiteralAttr(literal);
        break;

        case Literal::Kind::EMBEDDING: {
            const mlir::Attribute embeddingAttr = embeddingLiteralAttr(static_cast<const EmbeddingLiteral*>(literal));
            const mlir::db::ColumnType embResultType = allocColumnType(mlir::storage::EmbeddingType::get(_mlirCtxt));

            return _opBuilder.create<mlir::db::ConstantOp>(uloc, embResultType, embeddingAttr).getResult();
        }
        break;

        case Literal::Kind::NULL_LITERAL: {
            const mlir::Type nullableType = mlir::storage::NullableType::get(
                _mlirCtxt, mlir::NoneType::get(_mlirCtxt));
            valueAttr = mlir::StringAttr::get("", nullableType);
        }
        break;
        case Literal::Kind::LIST:
            return translateListLiteral(static_cast<const ListLiteral*>(literal));
        break;
        case Literal::Kind::MAP:
            return translateMapLiteral(static_cast<const MapLiteral*>(literal));
        break;
        default:
            throw FatalException("Unsupported literal kind in WHERE clause expression.");
        break;
    }

    const mlir::db::ColumnType resultType = allocColumnType(valueAttr.getType());
    return _opBuilder.create<mlir::db::ConstantOp>(uloc, resultType, valueAttr).getResult();
}

mlir::Value DBProgramGenerator::translateParameterExpr(const ParameterExpr* parameterExpr) {
    const auto argumentIt = _parameterArguments.find(parameterExpr->getDecl());
    bioassert(argumentIt != _parameterArguments.end(), "A parameter reached codegen with no argument of main.");

    return argumentIt->second;
}

// A parameter with a value has the type its literal would have; a null has the null literal's
mlir::Type DBProgramGenerator::parameterType(const ParameterDecl* decl) {
    const std::optional<ValueType> valueType = decl->getValueType();
    if (!valueType) {
        return mlir::storage::NullableType::get(_mlirCtxt, mlir::NoneType::get(_mlirCtxt));
    } else if (*valueType == ValueType::List) {
        return listShapeType(_opBuilder, decl->getListShape());
    }

    return valueTypeToElementType(_opBuilder, *valueType);
}

// The column of a null in every row, the shape translateLiteralExpr gives the null literal
mlir::Value DBProgramGenerator::nullConstantColumn() {
    const mlir::Type nullableType = mlir::storage::NullableType::get(_mlirCtxt,
                                                                    mlir::NoneType::get(_mlirCtxt));
    const mlir::TypedAttr valueAttr = mlir::StringAttr::get("", nullableType);
    const mlir::db::ColumnType resultType = allocColumnType(valueAttr.getType());

    return _opBuilder.create<mlir::db::ConstantOp>(_opBuilder.getUnknownLoc(), resultType, valueAttr).getResult();
}

mlir::Value DBProgramGenerator::constantBool(bool value) {
    const mlir::TypedAttr valueAttr = _opBuilder.getBoolAttr(value);
    const mlir::db::ColumnType resultType = allocColumnType(valueAttr.getType());

    return _opBuilder.create<mlir::db::ConstantOp>(_opBuilder.getUnknownLoc(), resultType, valueAttr).getResult();
}

mlir::Value DBProgramGenerator::constantDouble(double value) {
    const mlir::TypedAttr valueAttr = _opBuilder.getF64FloatAttr(value);
    const mlir::db::ColumnType resultType = allocColumnType(valueAttr.getType());

    return _opBuilder.create<mlir::db::ConstantOp>(_opBuilder.getUnknownLoc(), resultType, valueAttr).getResult();
}

mlir::Value DBProgramGenerator::constantString(llvm::StringRef value) {
    const mlir::Type stringType = mlir::storage::StringType::get(_mlirCtxt);
    const mlir::TypedAttr valueAttr = mlir::StringAttr::get(value, stringType);
    const mlir::db::ColumnType resultType = allocColumnType(stringType);

    return _opBuilder.create<mlir::db::ConstantOp>(_opBuilder.getUnknownLoc(), resultType, valueAttr).getResult();
}

mlir::Value DBProgramGenerator::constantLabelList(llvm::ArrayRef<std::string> labels) {
    const mlir::Type stringType = mlir::storage::StringType::get(_mlirCtxt);

    llvm::SmallVector<mlir::Attribute> elements;
    elements.reserve(labels.size());

    for (const std::string& label : labels) {
        elements.push_back(mlir::StringAttr::get(label, stringType));
    }

    mlir::db::ConstantOp constant = _opBuilder.create<mlir::db::ConstantOp>(_opBuilder.getUnknownLoc(),
                                                                           _opBuilder.getArrayAttr(elements));

    return constant.getResult();
}
