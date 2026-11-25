#include "DBProgramGenerator.h"

#include <numbers>
#include <string_view>

#include "mlir/IR/Builders.h"
#include "mlir/IR/Value.h"
#include "llvm/ADT/StringRef.h"

#include "DBOps.h"
#include "DBTypes.h"
#include "StorageEnums.h"
#include "StorageTypes.h"

#include "FunctionInvocation.h"
#include "FunctionSignature.h"
#include "decl/EvaluatedType.h"
#include "Literal.h"
#include "expr/Expr.h"
#include "expr/ExprChain.h"
#include "expr/FunctionInvocationExpr.h"
#include "expr/LiteralExpr.h"
#include "expr/SymbolExpr.h"

#include "BioAssert.h"

using namespace db;

namespace {

mlir::storage::UnicodeNormalForm toStorageNormalForm(UnicodeNormalForm form) {
    switch (form) {
        case UnicodeNormalForm::NFC:
            return mlir::storage::UnicodeNormalForm::NFC;
        break;
        case UnicodeNormalForm::NFD:
            return mlir::storage::UnicodeNormalForm::NFD;
        break;
        case UnicodeNormalForm::NFKC:
            return mlir::storage::UnicodeNormalForm::NFKC;
        break;
        case UnicodeNormalForm::NFKD:
            return mlir::storage::UnicodeNormalForm::NFKD;
        break;
        default:
            bioassert(false, "Unhandled Unicode normal form");
            return mlir::storage::UnicodeNormalForm::NFC;
        break;
    }
}

using UnaryFunctionEmitter = mlir::Value (*)(mlir::OpBuilder& builder, mlir::Location loc, mlir::Value input);

template <typename Op>
mlir::Value emitUnaryFunction(mlir::OpBuilder& builder, mlir::Location loc, mlir::Value input) {
    return builder.create<Op>(loc, input).getResult();
}

const std::unordered_map<std::string_view, UnaryFunctionEmitter> unaryFunctionEmitters = {
    {"labels", &emitUnaryFunction<mlir::db::Labels>},
    {"type", &emitUnaryFunction<mlir::db::EdgeType>},
    {"keys", &emitUnaryFunction<mlir::db::Keys>},
    {"properties", &emitUnaryFunction<mlir::db::PropertyMap>},
    {"startNode", &emitUnaryFunction<mlir::db::StartNode>},
    {"endNode", &emitUnaryFunction<mlir::db::EndNode>},
    {"toInteger", &emitUnaryFunction<mlir::db::ToInteger>},
    {"toFloat", &emitUnaryFunction<mlir::db::ToFloat>},
    {"toString", &emitUnaryFunction<mlir::db::ToString>},
    {"toBoolean", &emitUnaryFunction<mlir::db::ToBoolean>},
    {"toIntegerOrNull", &emitUnaryFunction<mlir::db::ToIntegerOrNull>},
    {"toFloatOrNull", &emitUnaryFunction<mlir::db::ToFloatOrNull>},
    {"toStringOrNull", &emitUnaryFunction<mlir::db::ToStringOrNull>},
    {"toBooleanOrNull", &emitUnaryFunction<mlir::db::ToBooleanOrNull>},
    {"toIntegerList", &emitUnaryFunction<mlir::db::ToIntegerList>},
    {"toFloatList", &emitUnaryFunction<mlir::db::ToFloatList>},
    {"toStringList", &emitUnaryFunction<mlir::db::ToStringList>},
    {"toBooleanList", &emitUnaryFunction<mlir::db::ToBooleanList>},
    {"datetime", &emitUnaryFunction<mlir::db::ToDateTime>},
    {"duration", &emitUnaryFunction<mlir::db::ToDuration>},
    {"point", &emitUnaryFunction<mlir::db::ToPoint>},
    {"size", &emitUnaryFunction<mlir::db::Size>},
    {"length", &emitUnaryFunction<mlir::db::Size>},
    {"isEmpty", &emitUnaryFunction<mlir::db::IsEmpty>},
    {"char_length", &emitUnaryFunction<mlir::db::CharLength>},
    {"character_length", &emitUnaryFunction<mlir::db::CharLength>},
    {"head", &emitUnaryFunction<mlir::db::Head>},
    {"last", &emitUnaryFunction<mlir::db::Last>},
    {"tail", &emitUnaryFunction<mlir::db::Tail>},
    {"abs", &emitUnaryFunction<mlir::db::Abs>},
    {"sign", &emitUnaryFunction<mlir::db::Sign>},
    {"ceil", &emitUnaryFunction<mlir::db::Ceil>},
    {"floor", &emitUnaryFunction<mlir::db::Floor>},
    {"round", &emitUnaryFunction<mlir::db::Round>},
    {"sqrt", &emitUnaryFunction<mlir::db::Sqrt>},
    {"exp", &emitUnaryFunction<mlir::db::Exp>},
    {"log", &emitUnaryFunction<mlir::db::Log>},
    {"log10", &emitUnaryFunction<mlir::db::Log10>},
    {"sin", &emitUnaryFunction<mlir::db::Sin>},
    {"cos", &emitUnaryFunction<mlir::db::Cos>},
    {"tan", &emitUnaryFunction<mlir::db::Tan>},
    {"cot", &emitUnaryFunction<mlir::db::Cot>},
    {"asin", &emitUnaryFunction<mlir::db::Asin>},
    {"acos", &emitUnaryFunction<mlir::db::Acos>},
    {"atan", &emitUnaryFunction<mlir::db::Atan>},
    {"degrees", &emitUnaryFunction<mlir::db::Degrees>},
    {"radians", &emitUnaryFunction<mlir::db::Radians>},
    {"haversin", &emitUnaryFunction<mlir::db::Haversin>},
    {"toUpper", &emitUnaryFunction<mlir::db::ToUpper>},
    {"toLower", &emitUnaryFunction<mlir::db::ToLower>},
    {"trim", &emitUnaryFunction<mlir::db::Trim>},
    {"ltrim", &emitUnaryFunction<mlir::db::LTrim>},
    {"rtrim", &emitUnaryFunction<mlir::db::RTrim>},
    {"reverse", &emitUnaryFunction<mlir::db::Reverse>},
};

using BinaryFunctionEmitter = mlir::Value (*)(mlir::OpBuilder& builder,
                                              mlir::Location loc,
                                              mlir::db::ColumnType resultType,
                                              mlir::Value lhs,
                                              mlir::Value rhs);

template <typename Op>
mlir::Value emitBinaryFunction(mlir::OpBuilder& builder,
                               mlir::Location loc,
                               mlir::db::ColumnType resultType,
                               mlir::Value lhs,
                               mlir::Value rhs) {
    return builder.create<Op>(loc, resultType, lhs, rhs).getResult();
}

const std::unordered_map<std::string_view, BinaryFunctionEmitter> binaryFunctionEmitters = {
    {"cosine_similarity", &emitBinaryFunction<mlir::db::CosineSimilarity>},
    {"euclidean_distance", &emitBinaryFunction<mlir::db::EuclideanDistance>},
    {"coll.indexOf", &emitBinaryFunction<mlir::db::IndexOf>},
};

}

void DBProgramGenerator::translateFunctionInvocationExpr(const Expr* expr,
                                                         const FunctionInvocationExpr* funcExpr) {
    const FunctionInvocation* invocation = funcExpr->getFunctionInvocation();
    const FunctionSignature* signature = invocation->getSignature();

    // The expression's own aggregate flag is contaminated by its arguments, so a scalar
    // function over a reduction - coalesce(max(n.age), 0) - carries it too. What says a
    // call reduces its input is the signature it resolved to.
    if (!signature->isAggregate()) {
        translateFunctionExpr(expr, invocation);
        return;
    }

    AggregateArguments arguments;
    translateAggregateArguments(invocation, nullptr, arguments);

    emitAggregate(expr, invocation, arguments);
}

mlir::Value DBProgramGenerator::translateArg(const Expr* argExpr) {
    translateExpr(argExpr);
    bioassert(_part._exprMap.contains(argExpr), "Function invocation with unknown argument.");
    return _part._exprMap.at(argExpr);
}

// A MERGE's rows mix the entities it wrote with the ones it bound, so no one label set or
// type stands for its column and it records neither: the caller falls back on the fetch,
// which reads each row where its own entity lives
mlir::Value DBProgramGenerator::translateCreatedMetadata(std::string_view funcName, const Expr* argExpr) {
    if (argExpr->getKind() != Expr::Kind::SYMBOL) {
        return {};
    }

    const SymbolExpr* symbolExpr = static_cast<const SymbolExpr*>(argExpr);
    const PartScope::WrittenEntity* written = findWrittenEntity(symbolExpr->getDecl());
    if (!written) {
        return {};
    }

    const bool canReadLabels = funcName == "labels" && !written->_labels.empty() && written->_labelsKnown;
    const bool canReadType = funcName == "type" && !written->_edgeType.empty() && written->_edgeTypeKnown;

    if (canReadLabels) {
        return constantLabelList(written->_labels);
    } else if (canReadType) {
        return constantString(written->_edgeType);
    } else {
        return {};
    }
}

void DBProgramGenerator::translateFunctionExpr(const Expr* expr,
                                               const FunctionInvocation* invocation) {
    const std::string_view funcName = invocation->getSignature()->getFullName();
    const ExprChain* args = invocation->getArguments();

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType noneType = allocColumnType(mlir::NoneType::get(_mlirCtxt));

    if (expr->getType() == EvaluatedType::Null) {
        _part._exprMap[expr] = nullConstantColumn();
        return;
    }

    const bool isEntityMetadata = funcName == "labels" || funcName == "type";
    if (isEntityMetadata && args && args->size() == 1) {
        const mlir::Value createdMetadata = translateCreatedMetadata(funcName, args->front());
        if (createdMetadata) {
            _part._exprMap[expr] = createdMetadata;
            return;
        }
    }

    if (funcName == "coalesce") {
        translateCoalesce(expr, args);
        return;
    } else if (funcName == "range") {
        translateRange(expr, args);
        return;
    } else if (funcName == "substring") {
        translateSubstring(expr, args);
        return;
    } else if (funcName == "split") {
        translateSplit(expr, args);
        return;
    } else if (funcName == "left") {
        translateStringEnd<mlir::db::Left>(expr, args);
        return;
    } else if (funcName == "right") {
        translateStringEnd<mlir::db::Right>(expr, args);
        return;
    } else if (funcName == "replace") {
        translateReplace(expr, args);
        return;
    } else if (funcName == "coll.sortMulti") {
        translateSortMulti(expr, args);
        return;
    } else if (funcName == "point.distance") {
        translatePointDistance(expr, args);
        return;
    } else if (funcName == "point.withinBBox") {
        translatePointWithinBBox(expr, args);
        return;
    } else if (funcName == "normalize") {
        translateNormalFormFunction<mlir::db::Normalize>(expr, args);
        return;
    } else if (funcName == "is_normalized") {
        translateNormalFormFunction<mlir::db::IsNormalized>(expr, args);
        return;
    }

    const bool readsTheMapItself = funcName == "properties"
                                && args && args->size() == 1
                                && args->front()->getType() == EvaluatedType::Map;
    if (readsTheMapItself) {
        _part._exprMap[expr] = translateArg(args->front());
        return;
    }

    // length() of a tagged cell reads a string or a list as well, so it is a size; nodes() and
    // relationships() of one read a path or nothing
    const Expr* const pathExpr = args && args->size() == 1 ? args->front() : nullptr;
    const EvaluatedType pathType = pathExpr ? pathExpr->getType() : EvaluatedType::Invalid;
    const bool readsATaggedPath = pathType == EvaluatedType::Any && (funcName == "nodes" || funcName == "relationships");
    const bool readsANamedPath = pathType == EvaluatedType::GraphPath || readsATaggedPath;
    if (readsANamedPath) {
        const mlir::Value path = translateArg(pathExpr);

        if (funcName == "length") {
            _part._exprMap[expr] = pathElementsColumn(path, mlir::storage::PathElementsKind::Length);
        } else if (funcName == "nodes") {
            _part._exprMap[expr] = pathElementsColumn(path, mlir::storage::PathElementsKind::Nodes);
        } else if (funcName == "relationships") {
            _part._exprMap[expr] = pathElementsColumn(path, mlir::storage::PathElementsKind::Relationships);
        } else {
            throwError(fmt::format("{}() does not read a path", funcName), expr);
        }

        return;
    }

    if (funcName == "id") {
        if (!args || args->size() != 1) {
            throwError("id() expects 1 argument.", expr);
        }

        const mlir::Value input = translateArg(args->front());
        _part._exprMap[expr] = _opBuilder.create<mlir::db::ElementID>(loc, input).getResult();
        return;
    }

    // datetime() reads the clock rather than converting an argument, so it is the one
    // name whose call with no argument is a different op from its call with one.
    const bool namesTheClock = funcName == "datetime" && (!args || args->empty());
    if (namesTheClock) {
        const mlir::db::ColumnType dateTimeType = allocColumnType(mlir::storage::DateTimeType::get(_mlirCtxt));

        _part._exprMap[expr] = _opBuilder.create<mlir::db::CurrentDateTime>(loc, dateTimeType).getResult();
        return;
    }

    if (funcName == "pi") {
        _part._exprMap[expr] = constantDouble(std::numbers::pi);
        return;
    } else if (funcName == "e") {
        _part._exprMap[expr] = constantDouble(std::numbers::e);
        return;
    }

    const auto unaryIt = unaryFunctionEmitters.find(funcName);
    if (unaryIt != end(unaryFunctionEmitters)) {
        if (!args || args->size() != 1) {
            throwError(fmt::format("{}() expects 1 argument.", funcName), expr);
        }

        const mlir::Value input = translateArg(args->front());
        _part._exprMap[expr] = unaryIt->second(_opBuilder, loc, input);
        return;
    }

    const auto binaryIt = binaryFunctionEmitters.find(funcName);
    if (binaryIt != end(binaryFunctionEmitters)) {
        if (!args || args->size() != 2) {
            throwError(fmt::format("{}() expects 2 arguments.", funcName), expr);
        }

        const mlir::Value lhs = translateArg(args->getExprs()[0]);
        const mlir::Value rhs = translateArg(args->getExprs()[1]);
        _part._exprMap[expr] = binaryIt->second(_opBuilder, loc, noneType, lhs, rhs);
        return;
    }

    throwError(fmt::format("Unsupported function: {}", funcName), expr);
}

void DBProgramGenerator::translatePointDistance(const Expr* expr, const ExprChain* args) {
    bioassert(args && args->size() == 2, "point.distance() takes 2 arguments.");

    const ExprChain::ExprVector& argExprs = args->getExprs();

    const mlir::Value lhs = translateArg(argExprs[0]);
    const mlir::Value rhs = translateArg(argExprs[1]);

    mlir::db::PointDistance distance = _opBuilder.create<mlir::db::PointDistance>(_opBuilder.getUnknownLoc(),
                                                                                 allocColumnType(_opBuilder.getF64Type()),
                                                                                 lhs,
                                                                                 rhs);

    _part._exprMap[expr] = distance.getResult();
}

void DBProgramGenerator::translatePointWithinBBox(const Expr* expr, const ExprChain* args) {
    bioassert(args && args->size() == 3, "point.withinBBox() takes 3 arguments.");

    const ExprChain::ExprVector& argExprs = args->getExprs();

    const mlir::Value point = translateArg(argExprs[0]);
    const mlir::Value lowerLeft = translateArg(argExprs[1]);
    const mlir::Value upperRight = translateArg(argExprs[2]);

    const mlir::Type boolType = mlir::storage::BoolType::get(_mlirCtxt);

    mlir::db::PointWithinBBox within = _opBuilder.create<mlir::db::PointWithinBBox>(_opBuilder.getUnknownLoc(),
                                                                                   allocColumnType(boolType),
                                                                                   point,
                                                                                   lowerLeft,
                                                                                   upperRight);

    _part._exprMap[expr] = within.getResult();
}

void DBProgramGenerator::translateRange(const Expr* expr, const ExprChain* args) {
    const size_t argCount = args ? args->size() : 0;
    const bool boundsGiven = argCount == 2 || argCount == 3;
    if (!boundsGiven) {
        throwError("range() expects 2 or 3 arguments.", expr);
    }

    const ExprChain::ExprVector& argExprs = args->getExprs();

    const mlir::Value start = translateArg(argExprs[0]);
    const mlir::Value end = translateArg(argExprs[1]);
    const mlir::Value step = argExprs.size() == 3 ? translateArg(argExprs[2]) : mlir::Value {};

    const mlir::Type listType = mlir::storage::ListType::get(_mlirCtxt, _opBuilder.getI64Type());

    mlir::db::Range range = _opBuilder.create<mlir::db::Range>(_opBuilder.getUnknownLoc(),
                                                               allocColumnType(listType),
                                                               start,
                                                               end,
                                                               step);

    _part._exprMap[expr] = range.getResult();
}

void DBProgramGenerator::translateSubstring(const Expr* expr, const ExprChain* args) {
    bioassert(args && (args->size() == 2 || args->size() == 3), "substring() takes 2 or 3 arguments.");

    const ExprChain::ExprVector& argExprs = args->getExprs();

    const mlir::Value string = translateArg(argExprs[0]);
    const mlir::Value start = translateArg(argExprs[1]);
    const mlir::Value length = argExprs.size() == 3 ? translateArg(argExprs[2]) : mlir::Value {};

    const mlir::Type stringType = mlir::storage::StringType::get(_mlirCtxt);

    mlir::db::Substring substring = _opBuilder.create<mlir::db::Substring>(_opBuilder.getUnknownLoc(),
                                                                           allocColumnType(stringType),
                                                                           string,
                                                                           start,
                                                                           length);

    _part._exprMap[expr] = substring.getResult();
}

void DBProgramGenerator::translateSortMulti(const Expr* expr, const ExprChain* args) {
    bioassert(args && !args->empty() && args->size() <= 4, "coll.sortMulti() takes 1 to 4 arguments.");

    const ExprChain::ExprVector& argExprs = args->getExprs();
    const size_t argCount = argExprs.size();

    const mlir::Value list = translateArg(argExprs[0]);
    const mlir::Value fields = argCount > 1 ? translateArg(argExprs[1]) : mlir::Value {};
    const mlir::Value limit = argCount > 2 ? translateArg(argExprs[2]) : mlir::Value {};
    const mlir::Value skip = argCount > 3 ? translateArg(argExprs[3]) : mlir::Value {};

    const mlir::db::ColumnType noneType = allocColumnType(mlir::NoneType::get(_mlirCtxt));

    mlir::db::SortMulti sortMulti = _opBuilder.create<mlir::db::SortMulti>(_opBuilder.getUnknownLoc(),
                                                                           noneType,
                                                                           list,
                                                                           fields,
                                                                           limit,
                                                                           skip);

    _part._exprMap[expr] = sortMulti.getResult();
}

void DBProgramGenerator::translateSplit(const Expr* expr, const ExprChain* args) {
    bioassert(args && args->size() == 2, "split() takes 2 arguments.");

    const ExprChain::ExprVector& argExprs = args->getExprs();

    const mlir::Value string = translateArg(argExprs[0]);
    const mlir::Value delimiter = translateArg(argExprs[1]);

    const mlir::Type stringType = mlir::storage::StringType::get(_mlirCtxt);
    const mlir::Type listType = mlir::storage::ListType::get(_mlirCtxt, stringType);

    mlir::db::Split split = _opBuilder.create<mlir::db::Split>(_opBuilder.getUnknownLoc(),
                                                               allocColumnType(listType),
                                                               string,
                                                               delimiter);

    _part._exprMap[expr] = split.getResult();
}

template <typename StringEndOp>
void DBProgramGenerator::translateStringEnd(const Expr* expr, const ExprChain* args) {
    bioassert(args && args->size() == 2, "left() and right() take 2 arguments.");

    const ExprChain::ExprVector& argExprs = args->getExprs();

    const mlir::Value string = translateArg(argExprs[0]);
    const mlir::Value length = translateArg(argExprs[1]);

    const mlir::Type stringType = mlir::storage::StringType::get(_mlirCtxt);

    StringEndOp stringEnd = _opBuilder.create<StringEndOp>(_opBuilder.getUnknownLoc(),
                                                           allocColumnType(stringType),
                                                           string,
                                                           length);

    _part._exprMap[expr] = stringEnd.getResult();
}

void DBProgramGenerator::translateReplace(const Expr* expr, const ExprChain* args) {
    bioassert(args && args->size() == 3, "replace() takes 3 arguments.");

    const ExprChain::ExprVector& argExprs = args->getExprs();

    const mlir::Value string = translateArg(argExprs[0]);
    const mlir::Value search = translateArg(argExprs[1]);
    const mlir::Value replacement = translateArg(argExprs[2]);

    const mlir::Type stringType = mlir::storage::StringType::get(_mlirCtxt);

    mlir::db::Replace replace = _opBuilder.create<mlir::db::Replace>(_opBuilder.getUnknownLoc(),
                                                                     allocColumnType(stringType),
                                                                     string,
                                                                     search,
                                                                     replacement);

    _part._exprMap[expr] = replace.getResult();
}

template <typename NormalFormOp>
void DBProgramGenerator::translateNormalFormFunction(const Expr* expr, const ExprChain* args) {
    bioassert(args && args->size() == 2, "A normal form function takes a string and a normal form.");

    const ExprChain::ExprVector& argExprs = args->getExprs();

    const mlir::Value string = translateArg(argExprs[0]);

    bioassert(argExprs[1]->getType() == EvaluatedType::NormalForm, "A normal form function reads its normal form from a literal.");

    const LiteralExpr* formExpr = static_cast<const LiteralExpr*>(argExprs[1]);
    const NormalFormLiteral* formLiteral = static_cast<const NormalFormLiteral*>(formExpr->getLiteral());
    const mlir::storage::UnicodeNormalForm form = toStorageNormalForm(formLiteral->getForm());

    NormalFormOp normalFormOp = _opBuilder.create<NormalFormOp>(_opBuilder.getUnknownLoc(), string, form);

    _part._exprMap[expr] = normalFormOp.getResult();
}
