#include "ExprAnalyzer.h"

#include <algorithm>
#include <vector>

#include "BinaryOperatorTyping.h"

#include "FunctionResolver.h"
#include "FunctionInvocation.h"
#include "QualifiedName.h"
#include "Symbol.h"
#include "decl/DeclContext.h"
#include "decl/EvaluatedType.h"
#include "decl/VarDecl.h"

#include "expr/All.h"
#include "expr/Expr.h"

using namespace db;

namespace {

ListShape selectedListShape(const Expr* value) {
    return ListShape::selected(value->getType(), value->getListShape());
}

// A scalar function answers null over a null argument, so a null stands in for whichever
// type the signature declares. An aggregate declares its own null overload
bool answersNullOverNull(const FunctionSignature* signature) {
    return !signature->unifiesItsArguments() && !signature->isAggregate();
}

bool takesArgumentCount(const FunctionSignature* signature, size_t count) {
    const bool tooFewArguments = count < signature->getMinArgCount();
    const bool tooManyArguments = !signature->unifiesItsArguments() && count > signature->argumentTypes().size();

    return !tooFewArguments && !tooManyArguments;
}

bool takesArgumentType(const FunctionSignature* signature, size_t argumentIndex, EvaluatedType argumentType) {
    const EvaluatedType expected = signature->argumentTypes()[argumentIndex].getType();

    return argumentType == expected || (answersNullOverNull(signature) && argumentType == EvaluatedType::Null);
}

bool takesArgumentTypes(const FunctionSignature* signature, const ExprChain::ExprVector& arguments) {
    for (size_t argumentIndex = 0; argumentIndex < arguments.size(); argumentIndex++) {
        if (!takesArgumentType(signature, argumentIndex, arguments[argumentIndex]->getType())) {
            return false;
        }
    }

    return true;
}

bool findRejectedArgument(FunctionResolver::FunctionSignatureRange signatures,
                          const ExprChain::ExprVector& arguments,
                          size_t& rejectedIndex) {
    std::vector<const FunctionSignature*> candidates;
    for (const FunctionSignature* signature : signatures) {
        if (!signature->unifiesItsArguments() && takesArgumentCount(signature, arguments.size())) {
            candidates.push_back(signature);
        }
    }

    if (candidates.empty()) {
        return false;
    }

    for (size_t argumentIndex = 0; argumentIndex < arguments.size(); argumentIndex++) {
        const EvaluatedType argumentType = arguments[argumentIndex]->getType();

        const bool taken = std::any_of(candidates.begin(), candidates.end(), [argumentIndex, argumentType](const FunctionSignature* candidate) {
            return takesArgumentType(candidate, argumentIndex, argumentType);
        });

        if (!taken) {
            rejectedIndex = argumentIndex;
            return true;
        }
    }

    return false;
}

}

void ExprAnalyzer::analyzeFuncInvocExpr(FunctionInvocationExpr* expr, const FunctionResolver* resolver) {
    const FunctionInvocation* invoc = expr->getFunctionInvocation();
    const QualifiedName* qualifiedName = invoc->getName();
    const std::vector<Symbol*>& names = qualifiedName->names();

    std::string name;

    for (size_t i = 0; i < names.size(); i++) {
        const Symbol* symbol = names[i];
        name += symbol->getName();

        if (i < names.size() - 1) {
            name += ".";
        }
    }

    const auto signatures = resolver->lookup(qualifiedName);

    // Check if there is at least one overload matching the function name
    if (signatures.empty()) {
        throwError(fmt::format("Function '{}' does not exist", name), expr);
    }

    const ExprChain* argsChain = invoc->getArguments();
    const ExprChain::ExprVector& providedArgs = argsChain->getExprs();

    bool isDynamic = false;
    bool isAggregate = false;

    for (Expr* arg : providedArgs) {
        analyzeExpr(arg);

        isDynamic |= arg->isDynamic();
        isAggregate |= arg->isAggregate();
    }

    // If at least one argument is dynamic, the function invocation is dynamic
    if (isDynamic) {
        expr->setDynamic();
    }

    // If at least one argument is aggregate, the function invocation is aggregate
    if (isAggregate) {
        expr->setAggregate();
    }

    // Nested aggregates not allowed by OpenCypher: reject here
    bool functionIsAggregate = false;
    for (const FunctionSignature* candidate : signatures) {
        if (candidate->isAggregate()) {
            functionIsAggregate = true;
            break;
        }
    }

    if (functionIsAggregate && isAggregate) {
        throwError("Aggregate functions cannot be nested inside other aggregate functions", expr);
    }

    const FunctionArgumentType* constantReadingARow = nullptr;
    const Expr* rowReadingArg = nullptr;

    // For each overload, check if the argument types match
    for (const FunctionSignature* signature : signatures) {
        const auto& expectedArgs = signature->argumentTypes();

        // A signature unifying its arguments declares none of them, so its arity is a
        // minimum alone and there is no expected type to match each against: they are
        // checked against each other once the overload is settled on.
        const bool unifiesArguments = signature->unifiesItsArguments();

        if (!takesArgumentCount(signature, providedArgs.size())) {
            continue;
        }

        if (!unifiesArguments && !takesArgumentTypes(signature, providedArgs)) {
            continue;
        }

        // A constant argument is read once per call, so an expression varying with the row
        // would have to be read again for every one. Turned away here rather than by the
        // procedure at runtime, once a row has already reached it - but only once every
        // overload has been tried, since another may take that argument per row.

        // A signature can declare fewer arguments than were provided - one unifying them
        // declares none - and more, when the ones behind the required count are optional
        const size_t declaredArgs = std::min(providedArgs.size(), expectedArgs.size());

        bool readsARowIntoAConstant = false;
        for (size_t argIndex = 0; argIndex < declaredArgs; argIndex++) {
            const FunctionArgumentType& expected = expectedArgs[argIndex];
            const Expr* arg = providedArgs[argIndex];

            if (expected.isConstant() && arg->isDynamic()) {
                if (!constantReadingARow) {
                    constantReadingARow = &expected;
                    rowReadingArg = arg;
                }

                readsARowIntoAConstant = true;
                break;
            }
        }

        if (readsARowIntoAConstant) {
            continue;
        }

        // Register variables for each argument
        for (Expr* arg : providedArgs) {
            const VarDecl* var = arg->getExprVarDecl();
            // Already registered: skip
            if (var) {
                continue;
            }

            // Not yet registered: create variable

            // Type is validated above
            const EvaluatedType type = arg->getType();
            const VarDecl* decl = _ctxt->createUnnamedVariable(_ast, type);
            arg->setExprVarDecl(decl);
        }

        if (signature->searchesItsList()) {
            BinaryTyping search;
            BinaryOperatorTyping::type(BinaryOperator::In, providedArgs[1]->getExprType(), providedArgs[0]->getExprType(), search);

            if (!search.isValid()) {
                throwError(fmt::format("'{}' cannot search this list: {}", name, search._error), expr);
            }
        }

        bool readsANull = false;
        for (size_t argIndex = 0; argIndex < providedArgs.size(); argIndex++) {
            const bool isNull = providedArgs[argIndex]->getType() == EvaluatedType::Null;
            const bool rejectsNull = argIndex < expectedArgs.size() && expectedArgs[argIndex].rejectsNull();

            if (isNull && !rejectsNull) {
                readsANull = true;
                break;
            }
        }

        // Found a valid signature
        if (answersNullOverNull(signature) && readsANull) {
            expr->setType(EvaluatedType::Null);
        } else if (unifiesArguments) {
            expr->setType(unifiedArgumentType(name, providedArgs));
        } else if (signature->returnTypes().size() == 1) {
            expr->setType(signature->returnTypes().front().getType());
        } else {
            expr->setType(EvaluatedType::Tuple);
        }

        // A collect's list holds the values of its argument, so it is one level deeper
        // than what it gathers - what an UNWIND of the list binds its variable to.
        if (signature->collectsItsArgument() && !providedArgs.empty()) {
            const Expr* collected = providedArgs.front();
            expr->setListShape(ListShape::collecting(collected->getType(), collected->getListShape()));
        }

        // A tail nests as deeply over the same elements as the list it drops one from, so
        // it hands on that list's shape - what an UNWIND of it reads to know its elements.
        if (signature->returnsItsArgumentShape() && !providedArgs.empty()) {
            expr->setListShape(providedArgs.front()->getListShape());
        }

        if (signature->returnsAnElementOfItsArgument() && !providedArgs.empty()) {
            const ListShape& listShape = providedArgs.front()->getListShape();

            if (listShape.isList()) {
                expr->setType(listShape.unwoundType());
                expr->setListShape(listShape.unwound());
            }
        }

        const ListShape& returnedShape = signature->returnedListShape();
        if (returnedShape.isList() && expr->getType() == EvaluatedType::List) {
            expr->setListShape(returnedShape);
        }

        if (unifiesArguments && expr->getType() == EvaluatedType::List) {
            ListShape unifiedShape;
            for (const Expr* arg : providedArgs) {
                unifiedShape = ListShape::unified(unifiedShape, selectedListShape(arg));
            }

            expr->setListShape(unifiedShape);
        }

        if (unifiesArguments) {
            EvaluatedType argumentsCommonType = EvaluatedType::Null;
            for (const Expr* arg : providedArgs) {
                argumentsCommonType = commonType(argumentsCommonType, arg->getCommonType());
            }

            expr->setCommonType(argumentsCommonType);
        }

        if (signature->isAggregate()) {
            if (isAggregate) {
                throwError(fmt::format("Aggregate functions may not be nested: the argument of "
                                       "'{}' is itself an aggregate", name), expr);
            }

            expr->setAggregate();
        } else if (invoc->isDistinct()) {
            throwError(fmt::format("DISTINCT may only be used in an aggregate function: "
                                   "'{}' is not an aggregate", name), expr);
        }

        expr->setSignature(signature);
        // Create a variable declaration for the function call so that it can be retrieved
        // later (for projection or in an expression / filter), e.g. RETURN sqrt(5)
        const VarDecl* decl = _ctxt->createUnnamedVariable(_ast, expr->getType());
        expr->setExprVarDecl(decl);

        return;
    }

    if (constantReadingARow) {
        throwError(fmt::format("Argument '{}' of '{}' must be constant, so it cannot read a row",
                               constantReadingARow->getName(),
                               name),
                   rowReadingArg);
    }

    size_t rejectedIndex = 0;
    if (findRejectedArgument(signatures, providedArgs, rejectedIndex)) {
        const Expr* rejected = providedArgs[rejectedIndex];

        throwError(fmt::format("Invalid arguments for function '{}': argument {} cannot be '{}'",
                               name,
                               rejectedIndex + 1,
                               EvaluatedTypeName::value(rejected->getType())),
                   rejected);
    }

    // Checked all overloaded signatures, none match: error
    throwError(fmt::format("Invalid arguments for function '{}'", name), expr);
}

EvaluatedType ExprAnalyzer::unifiedArgumentType(std::string_view name, std::span<Expr* const> args) const {
    EvaluatedType unified = EvaluatedType::Null;

    for (const Expr* arg : args) {
        const EvaluatedType folded = selectedType(unified, arg->getType());

        if (folded == EvaluatedType::Invalid) {
            throwError(fmt::format("'{}' cannot mix '{}' and '{}' arguments",
                                   name,
                                   EvaluatedTypeName::value(unified),
                                   EvaluatedTypeName::value(arg->getType())),
                       arg);
        }

        unified = folded;
    }

    return unified;
}

void ExprAnalyzer::analyzeCaseExpr(CaseExpr* expr) {
    const auto contaminate = [expr](const Expr* part) {
        if (part->isDynamic()) {
            expr->setDynamic();
        }

        if (part->isAggregate()) {
            expr->setAggregate();
        }
    };

    // The subject of the simple form, compared for equality against every WHEN value.
    // Null in the generic form, whose WHEN expressions are predicates of their own.
    Expr* const subject = expr->getSubject();
    if (subject) {
        analyzeExpr(subject);
        requireCaseSubject(subject);
        contaminate(subject);
    }

    EvaluatedType result = EvaluatedType::Null;
    EvaluatedType resultCommonType = EvaluatedType::Null;
    ListShape resultShape;

    for (CaseExpr::Branch& branch : expr->getBranches()) {
        for (CaseExpr::Test& test : branch._tests) {
            analyzeCaseTest(branch, subject, test);

            if (test._value) {
                contaminate(test._value);
            }
        }

        analyzeExpr(branch._then);

        result = unifyCaseBranch(result, branch._then);
        resultCommonType = commonType(resultCommonType, branch._then->getCommonType());
        resultShape = ListShape::unified(resultShape, selectedListShape(branch._then));

        contaminate(branch._then);
    }

    Expr* const elseExpr = expr->getElseExpr();
    if (elseExpr) {
        analyzeExpr(elseExpr);
        result = unifyCaseBranch(result, elseExpr);
        resultCommonType = commonType(resultCommonType, elseExpr->getCommonType());
        resultShape = ListShape::unified(resultShape, selectedListShape(elseExpr));
        contaminate(elseExpr);
    }

    expr->setType(result);
    expr->setCommonType(resultCommonType);

    if (result == EvaluatedType::List) {
        expr->setListShape(resultShape);
    }
}

void ExprAnalyzer::analyzeCaseTest(const CaseExpr::Branch& branch,
                                   const Expr* subject,
                                   CaseExpr::Test& test) {
    const bool testsNull = test._kind == CaseExpr::TestKind::IsNull
                           || test._kind == CaseExpr::TestKind::IsNotNull;

    if (!subject) {
        if (test._kind != CaseExpr::TestKind::Value) {
            throwError("A WHEN that compares needs a CASE subject to compare against",
                       testsNull ? branch._then : test._value);
        }

        if (branch._tests.size() > 1) {
            throwError("A CASE with no subject takes one predicate per WHEN, not a list of values",
                       test._value);
        }

        analyzeExpr(test._value);

        const EvaluatedType whenType = test._value->getType();
        if (!namesATruthValue(whenType)) {
            throwError(fmt::format("The WHEN condition of a CASE must be a boolean, not '{}'",
                                   EvaluatedTypeName::value(whenType)),
                       test._value);
        }

        test._resultType = whenType;
        return;
    }

    if (testsNull) {
        test._resultType = EvaluatedType::Bool;
        return;
    }

    analyzeExpr(test._value);

    const EvaluatedType subjectType = subject->getType();
    const EvaluatedType valueType = test._value->getType();
    const BinaryOperator op = test._operator;

    const bool testsOrdering = test._kind == CaseExpr::TestKind::Comparison
                               && (op == BinaryOperator::LessThan
                                   || op == BinaryOperator::GreaterThan
                                   || op == BinaryOperator::LessThanOrEqual
                                   || op == BinaryOperator::GreaterThanOrEqual);

    const BinaryOperator typedOperator = testsOrdering ? op : BinaryOperator::Equal;

    BinaryTyping typing;
    BinaryOperatorTyping::type(typedOperator, subject->getExprType(), test._value->getExprType(), typing);

    test._resultType = typing._result._type;

    if (typing.isValid()) {
        return;
    } else if (testsOrdering) {
        throwError(fmt::format("A CASE cannot order '{}' against '{}'",
                               EvaluatedTypeName::value(subjectType),
                               EvaluatedTypeName::value(valueType)),
                   test._value);
    }

    throwError(typing._error, test._value);
}

EvaluatedType ExprAnalyzer::unifyCaseBranch(EvaluatedType carried, const Expr* branch) const {
    const EvaluatedType unified = selectedType(carried, branch->getType());

    if (unified == EvaluatedType::Invalid) {
        throwError(fmt::format("A CASE cannot mix '{}' and '{}' branches",
                               EvaluatedTypeName::value(carried),
                               EvaluatedTypeName::value(branch->getType())),
                   branch);
    }

    return unified;
}

void ExprAnalyzer::requireCaseSubject(const Expr* subject) const {
    const EvaluatedType type = subject->getType();

    if (type == EvaluatedType::Null || namesAnEqualityOperand(type)) {
        return;
    }

    throwError(fmt::format("The subject of a CASE must be a value, not '{}'",
                           EvaluatedTypeName::value(type)),
               subject);
}
