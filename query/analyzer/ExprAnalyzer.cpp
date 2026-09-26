#include "ExprAnalyzer.h"

#include <algorithm>
#include <unordered_map>

#include "CypherAnalyzer.h"
#include "DiagnosticsManager.h"
#include "AnalyzeException.h"
#include "ReadStmtAnalyzer.h"
#include "CypherAST.h"
#include "FunctionDecls.h"
#include "FunctionResolver.h"
#include "FunctionInvocation.h"
#include "EdgePattern.h"
#include "NodePattern.h"
#include "Pattern.h"
#include "PatternElement.h"
#include "QualifiedName.h"
#include "Symbol.h"
#include "Literal.h"
#include "stmt/LoadCSVStmt.h"
#include "decl/DeclContext.h"
#include "decl/EvaluatedType.h"
#include "decl/VarDecl.h"

#include "embedding/EmbeddingBucket.h"
#include "StringBucket.h"

#include "expr/All.h"
#include "expr/Expr.h"

#include "BioAssert.h"

using namespace db;

namespace {

bool dateTimePartNamed(std::string_view name, DateTimePart& part) {
    static const std::unordered_map<std::string_view, DateTimePart> parts = {
        {"year",        DateTimePart::Year       },
        {"month",       DateTimePart::Month      },
        {"day",         DateTimePart::Day        },
        {"hour",        DateTimePart::Hour       },
        {"minute",      DateTimePart::Minute     },
        {"second",      DateTimePart::Second     },
        {"millisecond", DateTimePart::Millisecond},
        {"microsecond", DateTimePart::Microsecond},
    };

    const auto it = parts.find(name);
    if (it == parts.end()) {
        return false;
    }

    part = it->second;

    return true;
}

// The type a CASE takes when one branch gives @param carried and another gives
// @param branch: a null branch constrains nothing, an integer beside a double widens,
// and a one-character string literal reads as the string it is. Invalid when the two
// share no column type, which is what the caller reports.
EvaluatedType unifiedBranchType(EvaluatedType carried, EvaluatedType branch) {
    if (carried == branch) {
        return carried;
    }

    if (carried == EvaluatedType::Null) {
        return branch;
    } else if (branch == EvaluatedType::Null) {
        return carried;
    }

    const TypePairBitset pair(carried, branch);

    if (pair == TypePairBitset(EvaluatedType::Integer, EvaluatedType::Double)) {
        return EvaluatedType::Double;
    } else if (pair == TypePairBitset(EvaluatedType::Char, EvaluatedType::String)) {
        return EvaluatedType::String;
    }

    return EvaluatedType::Invalid;
}

// A type-erased cell concatenates as the text it holds, which is how the element of a list
// joins a string.
bool concatenatesListItem(TypePairBitset pair) {
    return pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::String);
}

// Two cells join as the texts they hold under '||', which concatenates whatever it is
// given. '+' computes over them instead, as the other arithmetic operators do.
bool concatenatesTwoListItems(TypePairBitset pair) {
    return pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::ListItem);
}

// A number joins a string as the text Cypher writes it with: 1 + ' apples' is '1 apples'.
// A boolean does not: true + 'x' has no meaning in Cypher.
bool concatenatesAsText(TypePairBitset pair) {
    return pair == TypePairBitset(EvaluatedType::Integer, EvaluatedType::String)
        || pair == TypePairBitset(EvaluatedType::Double, EvaluatedType::String);
}

// Whether an arithmetic operator's operands are a type-erased cell and a number, or two
// cells. A cell is numeric only once read, and its tag names a type per row rather than
// one for the column, so the answer is the double a reduction over cells lands on too.
bool computesOverListItem(TypePairBitset pair) {
    return pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::ListItem)
        || pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::Integer)
        || pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::Double);
}

// The types a list is homogeneous in: the scalars a value column holds, the entities a
// pattern binds - [n, m] is a list of nodes, as collect(n) gathers one - and the lists a
// nesting is made of.
bool namesAListElementType(EvaluatedType type) {
    return convertibleToValueType(type)
        || type == EvaluatedType::NodePattern
        || type == EvaluatedType::EdgePattern
        || type == EvaluatedType::List;
}

// The shape of the list these elements make: depth 1 over the one type they share, or
// one level deeper than the lists they are. Elements that share no type - an empty list,
// a mix of types, lists of differing shape - leave the leaf Invalid, which is what makes
// the list hand out tagged scalars. A null names no type of its own, so it agrees with
// every element: [1, null, 3] is a list of integers with a null in it.
ListShape sharedListShape(std::span<Expr* const> elements) {
    const Expr* first = nullptr;
    for (Expr* const element : elements) {
        if (element->getType() != EvaluatedType::Null) {
            first = element;
            break;
        }
    }

    if (!first) {
        return ListShape(EvaluatedType::Invalid, 1);
    }

    const EvaluatedType shared = first->getType();

    const auto agreesWithShared = [shared](const Expr* element) {
        const EvaluatedType type = element->getType();
        return type == EvaluatedType::Null || type == shared;
    };

    if (!std::ranges::all_of(elements, agreesWithShared)) {
        return ListShape(EvaluatedType::Invalid, 1);
    }

    const ListShape& firstShape = first->getListShape();

    if (shared == EvaluatedType::List) {
        const auto agreesWithFirstShape = [&firstShape](const Expr* element) {
            if (element->getType() == EvaluatedType::Null) {
                return true;
            }

            const ListShape& shape = element->getListShape();
            return shape.getDepth() == firstShape.getDepth() && shape.getLeafType() == firstShape.getLeafType();
        };

        if (!std::ranges::all_of(elements, agreesWithFirstShape)) {
            return ListShape(EvaluatedType::Invalid, 1);
        }
    }

    if (!namesAListElementType(shared)) {
        return ListShape(EvaluatedType::Invalid, 1);
    }

    return ListShape::collecting(shared, firstShape);
}

// The types a scalar operand of '+' joins a list as: whatever an element of a list can be,
// a tagged cell and a null included - appending a null leaves a list with a null in it.
bool joinsAList(EvaluatedType type) {
    return namesAListElementType(type)
        || type == EvaluatedType::Char
        || type == EvaluatedType::Null
        || type == EvaluatedType::ListItem;
}

// The shape of the list appending a scalar makes: the list's own where the scalar is the
// type its elements carry, and a list of tagged scalars where it is not, as a mixed list
// literal is. A null carries no type, so it leaves the shape as it found it.
ListShape appendedListShape(const ListShape& list, EvaluatedType scalar) {
    const bool joinsTheLeaf = list.getDepth() == 1
                              && (scalar == EvaluatedType::Null || list.getLeafType() == scalar);

    if (!joinsTheLeaf) {
        return ListShape(EvaluatedType::Invalid, 1);
    }

    return list;
}

// The shape of the list a concatenation makes: the one both sides carry, or a list of
// tagged scalars when they carry different ones, as a mixed list literal is.
ListShape concatenatedListShape(const ListShape& left, const ListShape& right) {
    const bool sameDepth = left.getDepth() == right.getDepth();
    const bool sameLeafType = left.getLeafType() == right.getLeafType();

    if (!sameDepth || !sameLeafType) {
        return ListShape(EvaluatedType::Invalid, 1);
    }

    return left;
}

bool isEntity(EvaluatedType type) {
    return type == EvaluatedType::NodePattern || type == EvaluatedType::EdgePattern;
}

}

ExprAnalyzer::ExprAnalyzer(CypherAST* ast, const GraphView& graphView)
    : _ast(ast),
    _graphView(graphView),
    _graphMetadata(_graphView.metadata())
{
}

ExprAnalyzer::~ExprAnalyzer() {
}

void ExprAnalyzer::analyzeRootExpr(Expr* expr) {
    // Each root is walked on its own: a SET analyzes its value expression a second time,
    // once the property it assigns has been declared, and that walk has to run again
    _analyzedExprs.clear();

    analyzeExpr(expr);

    if (!expr->getExprVarDecl()) {
        expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, expr->getType()));
    }
}

void ExprAnalyzer::analyzeExpr(Expr* expr) {
    // A comparison chain hands its middle operand to both of its sides, so one walk
    // reaches that expression twice. Analyzing it again declares a second unnamed
    // variable for a value that already has one, and orphans the first
    if (!_analyzedExprs.insert(expr).second) {
        return;
    }

    switch (expr->getKind()) {
        case Expr::Kind::BINARY:
            analyzeBinaryExpr(static_cast<BinaryExpr*>(expr));
        break;
        case Expr::Kind::UNARY:
            analyzeUnaryExpr(static_cast<UnaryExpr*>(expr));
        break;
        case Expr::Kind::STRING:
            analyzeStringExpr(static_cast<StringExpr*>(expr));
        break;
        case Expr::Kind::ENTITY_TYPES:
            analyzeEntityTypeExpr(static_cast<EntityTypeExpr*>(expr));
        break;
        case Expr::Kind::PROPERTY:
            analyzePropertyExpr(static_cast<PropertyExpr*>(expr));
        break;
        case Expr::Kind::PROPERTY_LOOKUP:
            analyzePropertyLookupExpr(static_cast<PropertyLookupExpr*>(expr));
        break;
        case Expr::Kind::SYMBOL:
            analyzeSymbolExpr(static_cast<SymbolExpr*>(expr));
        break;
        case Expr::Kind::LITERAL:
            analyzeLiteralExpr(static_cast<LiteralExpr*>(expr));
        break;
        case Expr::Kind::FUNCTION_INVOCATION:
            analyzeFuncInvocExpr(static_cast<FunctionInvocationExpr*>(expr),
                                _ast->getFunctionDecls());
        break;
        case Expr::Kind::INDEX:
            analyzeIndexExpr(static_cast<IndexExpr*>(expr));
        break;
        case Expr::Kind::LIST:
            analyzeListExpr(static_cast<ListExpr*>(expr));
        break;
        case Expr::Kind::LIST_SLICE:
            analyzeListSliceExpr(static_cast<ListSliceExpr*>(expr));
        break;
        case Expr::Kind::LIST_COMPREHENSION:
            analyzeListComprehensionExpr(static_cast<ListComprehensionExpr*>(expr));
        break;
        case Expr::Kind::PATTERN_COMPREHENSION:
            analyzePatternComprehensionExpr(static_cast<PatternComprehensionExpr*>(expr));
        break;
        case Expr::Kind::CASE:
            analyzeCaseExpr(static_cast<CaseExpr*>(expr));
        break;
        case Expr::Kind::EXISTS:
            analyzeExistsExpr(static_cast<ExistsExpr*>(expr));
        break;

        case Expr::Kind::_SIZE:
            throwError("Unknown expression type in ExprAnalyzer.");
        break;

    }
}

void ExprAnalyzer::analyzeExistsExpr(ExistsExpr* expr) {
    bioassert(_queryAnalyzer, "EXISTS analyzed without a query analyzer.");

    _queryAnalyzer->analyzeExistsBody(expr);

    expr->setType(EvaluatedType::Bool);

    // The body reads the graph over the rows in flight, so the answer is a value per row
    // even where the body names nothing of the scope around it
    expr->setDynamic();
}

void ExprAnalyzer::analyzeBinaryExpr(BinaryExpr* expr) {
    Expr* lhs = expr->getLHS();
    Expr* rhs = expr->getRHS();

    analyzeExpr(lhs);
    analyzeExpr(rhs);

    const EvaluatedType a = lhs->getType();
    const EvaluatedType b = rhs->getType();

    EvaluatedType type = EvaluatedType::Invalid;

    const TypePairBitset pair(a, b);

    switch (expr->getOperator()) {
        case BinaryOperator::Or:
        case BinaryOperator::Xor:
        case BinaryOperator::And: {
            type = EvaluatedType::Bool;

            if (pair == TypePairBitset(EvaluatedType::Bool, EvaluatedType::Bool)
                || pair == TypePairBitset(EvaluatedType::Bool, EvaluatedType::Null)
                || pair == TypePairBitset(EvaluatedType::Null, EvaluatedType::Null)) {
                break;
            }

            const std::string error = fmt::format(
                "Operands must be booleans, not '{}' and '{}'",
                EvaluatedTypeName::value(a),
                EvaluatedTypeName::value(b));

            throwError(error, expr);
        } break;

        case BinaryOperator::NotEqual:
        case BinaryOperator::Equal: {
            type = EvaluatedType::Bool;

            if (pair == TypePairBitset(EvaluatedType::Double, EvaluatedType::Double)) {
                const std::string error = fmt::format(
                    "Equality of types '{}' and '{}' is not encouraged due to "
                    "potential rounding innacuracy. Please constrain with '<' "
                    "and '>' instead.",
                    EvaluatedTypeName::value(a), EvaluatedTypeName::value(b));
                throwError(error, expr);
            }

            if (pair == TypePairBitset(EvaluatedType::Integer, EvaluatedType::Integer)
                || pair == TypePairBitset(EvaluatedType::String, EvaluatedType::String)
                || pair == TypePairBitset(EvaluatedType::String, EvaluatedType::Char)
                || pair == TypePairBitset(EvaluatedType::Char, EvaluatedType::Char)
                || pair == TypePairBitset(EvaluatedType::Bool, EvaluatedType::Bool)
                || pair == TypePairBitset(EvaluatedType::Embedding, EvaluatedType::Embedding)
                || pair == TypePairBitset(EvaluatedType::Label, EvaluatedType::Label)
                || pair == TypePairBitset(EvaluatedType::EdgeType, EvaluatedType::EdgeType)
                || pair == TypePairBitset(EvaluatedType::PropertyType, EvaluatedType::PropertyType)
                || pair == TypePairBitset(EvaluatedType::List, EvaluatedType::List)
                || pair == TypePairBitset(EvaluatedType::DateTime, EvaluatedType::DateTime)) {
                break;
            }

            // Comparing against a null is null, so the pairs a null takes part in are as
            // valid as any other; the two nulls of 'null = null' included
            if (pair == TypePairBitset(EvaluatedType::Null, EvaluatedType::Null)
                || pair == TypePairBitset(EvaluatedType::Integer, EvaluatedType::Null)
                || pair == TypePairBitset(EvaluatedType::Double, EvaluatedType::Null)
                || pair == TypePairBitset(EvaluatedType::String, EvaluatedType::Null)
                || pair == TypePairBitset(EvaluatedType::Char, EvaluatedType::Null)
                || pair == TypePairBitset(EvaluatedType::Bool, EvaluatedType::Null)
                || pair == TypePairBitset(EvaluatedType::Embedding, EvaluatedType::Null)
                || pair == TypePairBitset(EvaluatedType::DateTime, EvaluatedType::Null)
            ) {
                break;
            }

            // A type-erased cell is equal only to a cell holding the same value, so it
            // compares against the types it can hold - a list and a null among them, the
            // null being what IS (NOT) NULL tests a cell for
            const bool comparesListItem =
                pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::ListItem)
                || pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::Integer)
                || pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::String)
                || pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::Char)
                || pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::Bool)
                || pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::Null)
                || pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::List)
                || pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::Map);

            // A stored list compares against another list, and against null for
            // IS (NOT) NULL
            const bool comparesList =
                pair == TypePairBitset(EvaluatedType::List, EvaluatedType::List)
                || pair == TypePairBitset(EvaluatedType::List, EvaluatedType::Null);

            const bool comparesMap =
                pair == TypePairBitset(EvaluatedType::Map, EvaluatedType::Map)
                || pair == TypePairBitset(EvaluatedType::Map, EvaluatedType::Null);

            if (comparesListItem || comparesList || comparesMap) {
                break;
            }

            // Values of two types that can never be equal are not equal, so the comparison
            // answers false rather than turning the query away
            if (comparesAsDisjointTypes(a, b)) {
                break;
            }

            // Allows NodeID <-> NodeID and NodeID <-> Integer comparisons
            if (pair == TypePairBitset(EvaluatedType::NodePattern,
                                       EvaluatedType::NodePattern)
                || pair == TypePairBitset(EvaluatedType::Integer,
                                          EvaluatedType::NodePattern)) {
                break;
            }

            // Allows EdgeID <-> EdgeID and EdgeID <-> Integer comparisons
            if (pair == TypePairBitset(EvaluatedType::EdgePattern,
                                       EvaluatedType::EdgePattern)
                || pair == TypePairBitset(EvaluatedType::Integer,
                                          EvaluatedType::EdgePattern)) {
                break;
            }

            // n IS NULL over a node or an edge, which an OPTIONAL MATCH leaves null when
            // its pattern missed
            const bool comparesEntityToNull =
                pair == TypePairBitset(EvaluatedType::NodePattern, EvaluatedType::Null)
                || pair == TypePairBitset(EvaluatedType::EdgePattern, EvaluatedType::Null);

            if (comparesEntityToNull) {
                break;
            }

            const std::string error = fmt::format(
                "Operands are not valid or compatible types: '{}' and '{}'",
                EvaluatedTypeName::value(a),
                EvaluatedTypeName::value(b));

            throwError(error, expr);
        } break;

        case BinaryOperator::IsNull:
        case BinaryOperator::IsNotNull: {
            type = EvaluatedType::Bool;

            if (b == EvaluatedType::Null) {
                break;
            }

            throwError(fmt::format("IS tests whether its operand is null, so its right side "
                                   "must be NULL, not '{}'",
                                   EvaluatedTypeName::value(b)),
                       expr);
        } break;

        case BinaryOperator::LessThan:
        case BinaryOperator::GreaterThan:
        case BinaryOperator::LessThanOrEqual:
        case BinaryOperator::GreaterThanOrEqual: {
            type = EvaluatedType::Bool;

            if (pair == TypePairBitset(EvaluatedType::Integer, EvaluatedType::Integer)
                || pair == TypePairBitset(EvaluatedType::Double, EvaluatedType::Double)
                || pair == TypePairBitset(EvaluatedType::Integer, EvaluatedType::Double)
                || pair == TypePairBitset(EvaluatedType::String, EvaluatedType::String)
                || pair == TypePairBitset(EvaluatedType::Bool, EvaluatedType::Bool)
                || pair == TypePairBitset(EvaluatedType::DateTime, EvaluatedType::DateTime)) {
                // Valid pair
                break;
            }

            // Ordering a value against a null is null, the same as comparing it against one
            const bool ordersAgainstNull = a == EvaluatedType::Null || b == EvaluatedType::Null;

            if (ordersAgainstNull) {
                break;
            }

            // A type-erased cell is ordered as an element holding the other side would be,
            // so it orders against the scalar types it can hold
            const bool ordersListItem =
                pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::ListItem)
                || pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::Integer)
                || pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::Double)
                || pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::String)
                || pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::Char)
                || pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::Bool);

            if (ordersListItem) {
                break;
            }

            const std::string error = fmt::format(
                "Operands are not valid or compatible numeric types: '{}' and '{}'",
                EvaluatedTypeName::value(a),
                EvaluatedTypeName::value(b));

            throwError(error, expr);
        } break;

        case BinaryOperator::Add: {
            if (pair == TypePairBitset(EvaluatedType::Integer, EvaluatedType::Integer)) {
                type = EvaluatedType::Integer;
                break;
            }

            if (pair == TypePairBitset(EvaluatedType::Double, EvaluatedType::Double)
                || pair == TypePairBitset(EvaluatedType::Double, EvaluatedType::Integer)) {
                type = EvaluatedType::Double;
                break;
            }

            if (pair == TypePairBitset(EvaluatedType::String, EvaluatedType::String)) {
                type = EvaluatedType::String;
                break;
            }

            if (pair == TypePairBitset(EvaluatedType::List, EvaluatedType::List)) {
                type = EvaluatedType::List;
                expr->setListShape(concatenatedListShape(lhs->getListShape(), rhs->getListShape()));
                break;
            }

            // A scalar joins a list as the one-element list it stands for, from either
            // side: [1, 2] + 3 and 1 + [2, 3] are both [1, 2, 3]
            const bool leftIsAList = a == EvaluatedType::List;
            const bool oneSideIsAList = leftIsAList || b == EvaluatedType::List;
            const EvaluatedType scalar = leftIsAList ? b : a;

            if (oneSideIsAList && joinsAList(scalar)) {
                const ListShape& listShape = leftIsAList ? lhs->getListShape() : rhs->getListShape();

                type = EvaluatedType::List;
                expr->setListShape(appendedListShape(listShape, scalar));
                break;
            }

            // A cell holding text concatenates with a string, as the string it holds
            // would: the type it holds is settled row by row, so a cell holding a number
            // joins as the text Cypher writes that number with
            if (concatenatesListItem(pair) || concatenatesAsText(pair)) {
                type = EvaluatedType::String;
                break;
            }

            // Arithmetic over an unknown value is unknown, whatever the other side holds.
            // The list cases come first: [1, 2] + null appends the null instead
            if (a == EvaluatedType::Null || b == EvaluatedType::Null) {
                type = EvaluatedType::Null;
                break;
            }

            if (computesOverListItem(pair)) {
                type = EvaluatedType::Double;
                break;
            }

            const std::string error = fmt::format(
                "Operands are not valid and compatible types for '+': '{}' and '{}'",
                EvaluatedTypeName::value(a),
                EvaluatedTypeName::value(b));

            throwError(error, expr);
        } break;

        case BinaryOperator::Concat: {
            if (pair == TypePairBitset(EvaluatedType::String, EvaluatedType::String)) {
                type = EvaluatedType::String;
                break;
            }

            if (pair == TypePairBitset(EvaluatedType::List, EvaluatedType::List)) {
                type = EvaluatedType::List;
                expr->setListShape(concatenatedListShape(lhs->getListShape(), rhs->getListShape()));
                break;
            }

            if (concatenatesListItem(pair) || concatenatesTwoListItems(pair)) {
                type = EvaluatedType::String;
                break;
            }

            // '||' joins two strings or two lists, so an unknown value is no side of one
            // rather than an element to append: [1, 2] || null is null where [1, 2] + null
            // is [1, 2, null]
            if (a == EvaluatedType::Null || b == EvaluatedType::Null) {
                type = EvaluatedType::Null;
                break;
            }

            const std::string error = fmt::format(
                "Operands are not valid and compatible types for '||': '{}' and '{}'",
                EvaluatedTypeName::value(a),
                EvaluatedTypeName::value(b));

            throwError(error, expr);
        } break;

        case BinaryOperator::Sub:
        case BinaryOperator::Mult:
        case BinaryOperator::Div:
        case BinaryOperator::Mod: {
            if (pair == TypePairBitset(EvaluatedType::Integer, EvaluatedType::Integer)) {
                type = EvaluatedType::Integer;
                break;
            }

            if (pair == TypePairBitset(EvaluatedType::Double, EvaluatedType::Double)
                || pair == TypePairBitset(EvaluatedType::Double, EvaluatedType::Integer)) {
                type = EvaluatedType::Double;
                break;
            }

            if (computesOverListItem(pair)) {
                type = EvaluatedType::Double;
                break;
            }

            if (a == EvaluatedType::Null || b == EvaluatedType::Null) {
                type = EvaluatedType::Null;
                break;
            }

            const std::string error = fmt::format(
                "Operands are not valid and compatible numeric types: '{}' and '{}'",
                EvaluatedTypeName::value(a),
                EvaluatedTypeName::value(b));

            throwError(error, expr);
        } break;

        case BinaryOperator::Pow: {
            const bool bothInteger = pair == TypePairBitset(EvaluatedType::Integer, EvaluatedType::Integer);
            const bool bothDouble = pair == TypePairBitset(EvaluatedType::Double, EvaluatedType::Double);
            const bool mixedNumeric = pair == TypePairBitset(EvaluatedType::Double, EvaluatedType::Integer);

            // As per OpenCypher spec
            if (bothInteger || bothDouble || mixedNumeric) {
                type = EvaluatedType::Double;
                break;
            }

            if (computesOverListItem(pair)) {
                type = EvaluatedType::Double;
                break;
            }

            if (a == EvaluatedType::Null || b == EvaluatedType::Null) {
                type = EvaluatedType::Null;
                break;
            }

            const std::string error = fmt::format(
                "Operands are not valid and compatible numeric types: '{}' and '{}'",
                EvaluatedTypeName::value(a),
                EvaluatedTypeName::value(b));

            throwError(error, expr);
        } break;

        case BinaryOperator::In: {
            type = EvaluatedType::Bool;

            // A type-erased cell holds whatever its row put there, so the test reads the
            // list out of its tag and answers null for a row holding something else
            const bool searchesACell = b == EvaluatedType::ListItem;

            if (b != EvaluatedType::List && b != EvaluatedType::Map && !searchesACell) {
                const std::string error = fmt::format("IN operand must be a list or map, not '{}'",
                                                      EvaluatedTypeName::value(b));
                throwError(error, expr);
            }

            if (a == EvaluatedType::List || a == EvaluatedType::Map) {
                const std::string error = fmt::format("Left operand must be a scalar, not '{}'",
                                                      EvaluatedTypeName::value(a));
                throwError(error, expr);
            }
        } break;

        case BinaryOperator::_SIZE: {
            throwError("Invalid operand in binary expression.");
        }
        break;
    }

    expr->setType(type);

    // Dynamic/Aggregate contamination
    if (lhs->isDynamic() || rhs->isDynamic()) {
        expr->setDynamic();
    }

    if (lhs->isAggregate() || rhs->isAggregate()) {
        expr->setAggregate();
    }

    // Create a variable declaration for the binary expression so that it can be retrieved
    // later (for projection or in an expression / filter), e.g. RETURN COUNT(5 + 5)
    const VarDecl* decl = _ctxt->createUnnamedVariable(_ast, expr->getType());
    expr->setExprVarDecl(decl);
}

void ExprAnalyzer::analyzeUnaryExpr(UnaryExpr* expr) {
    Expr* operand = expr->getSubExpr();
    analyzeExpr(operand);

    EvaluatedType type = EvaluatedType::Invalid;

    switch (expr->getOperator()) {
        case UnaryOperator::Not: {
            const EvaluatedType operandType = operand->getType();
            const bool negatesATruthValue = operandType == EvaluatedType::Bool
                                            || operandType == EvaluatedType::Null;

            if (!negatesATruthValue) {
                const std::string error = fmt::format("NOT operand must be a boolean, not '{}'",
                                                      EvaluatedTypeName::value(operandType));
                throwError(error, expr);
            }

            type = EvaluatedType::Bool;
        } break;

        case UnaryOperator::Minus:
        case UnaryOperator::Plus: {
            const EvaluatedType operandType = operand->getType();
            if (operandType == EvaluatedType::Integer) {
                type = EvaluatedType::Integer;
            } else if (operandType == EvaluatedType::Double) {
                type = EvaluatedType::Double;
            } else if (operandType == EvaluatedType::Null) {
                type = EvaluatedType::Null;
            } else {
                const std::string error = fmt::format("Operand must be an integer or double, not '{}'",
                                                      EvaluatedTypeName::value(operandType));
                throwError(error, expr);
            }

        } break;

        case UnaryOperator::_SIZE: {
            throwError("Invalid operand in unary expression.");
        }
        break;
    }

    expr->setType(type);

    if (operand->isDynamic()) {
        expr->setDynamic();
    }

    if (operand->isAggregate()) {
        expr->setAggregate();
    }
}

VarDecl* ExprAnalyzer::resolveVariable(std::string_view name) {
    if (VarDecl* local = _ctxt->getDecl(name)) {
        return local;
    }

    VarDecl* outer = _ctxt->lookup(name);
    if (outer && _importSink && !std::ranges::contains(*_importSink, outer)) {
        _importSink->push_back(outer);
    }

    return outer;
}

void ExprAnalyzer::analyzeSymbolExpr(SymbolExpr* expr) {
    VarDecl* varDecl = resolveVariable(expr->getSymbol()->getName());
    if (!varDecl) {
        throwError(fmt::format("Variable '{}' not found", expr->getSymbol()->getName()), expr);
    }

    expr->setDecl(varDecl);
    expr->setType(varDecl->getType());
    expr->setListShape(varDecl->getListShape());
    expr->setExprVarDecl(varDecl);

    // For now, variable expressions cannot be evaluated at compile time
    // TODO: We could check if the variable is actually a constexpr
    expr->setDynamic();
}

void ExprAnalyzer::analyzeLiteralExpr(LiteralExpr* expr) {
    const Literal* literal = expr->getLiteral();

    switch (literal->getKind()) {
        case Literal::Kind::NULL_LITERAL: {
            expr->setType(EvaluatedType::Null);
        } break;
        case Literal::Kind::BOOL: {
            expr->setType(EvaluatedType::Bool);
        } break;
        case Literal::Kind::INTEGER: {
            expr->setType(EvaluatedType::Integer);
        } break;
        case Literal::Kind::DOUBLE: {
            expr->setType(EvaluatedType::Double);
        } break;
        case Literal::Kind::STRING: {
            const StringLiteral* strLiteral = static_cast<const StringLiteral*>(literal);
            if (strLiteral->getValue().size() > StringBucket::BUCKET_SIZE) {
                throwError(fmt::format("String literal exceeds maximum size of {} bytes",
                                       StringBucket::BUCKET_SIZE), expr);
            }
            expr->setType(EvaluatedType::String);
        } break;
        case Literal::Kind::CHAR: {
            expr->setType(EvaluatedType::Char);
        } break;
        case Literal::Kind::LIST: {
            expr->setType(EvaluatedType::List);
            ListLiteral* list = static_cast<ListLiteral*>(expr->getLiteral());
            analyzeListElements(expr, list->items());
        } break;
        case Literal::Kind::MAP: {
            expr->setType(EvaluatedType::Map);

            const MapLiteral* map = static_cast<const MapLiteral*>(literal);
            analyzeMapEntries(expr, map);
        } break;
        case Literal::Kind::EMBEDDING: {
            const auto* embLit = static_cast<const EmbeddingLiteral*>(literal);
            constexpr size_t maxDimension = EmbeddingBucket::MIN_BUCKET_BYTES / sizeof(float);
            if (embLit->getDimension() > maxDimension) {
                throwError(fmt::format("Embedding dimension {} exceeds maximum of {}",
                                       embLit->getDimension(), maxDimension), expr);
            }
            expr->setType(EvaluatedType::Embedding);
        } break;
        case Literal::Kind::WILDCARD: {
            expr->setType(EvaluatedType::Wildcard);
        } break;
    }

    expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, expr->getType()));
}

ValueType ExprAnalyzer::analyzePropertyExpr(PropertyExpr* expr, bool allowCreate, ValueType defaultType) {
    const QualifiedName* qualifiedName = expr->getFullName();

    // Two names read a property of an entity or a field of a row; three read a calendar
    // field off a property holding an instant, which is the one chain a value extends
    const bool readsAComponentOfAProperty = qualifiedName->size() == 3;

    if (qualifiedName->size() != 2 && !readsAComponentOfAProperty) {
        throwError("Invalid property expression.", expr);
    }

    const Symbol* varName = qualifiedName->front();
    const Symbol* propName = qualifiedName->get(1);

    // An anonymous pattern's declaration carries no name the context can resolve, so the
    // predicate its inline property map becomes arrives with that declaration already set
    VarDecl* varDecl = expr->getEntityVarDecl();
    if (!varDecl) {
        varDecl = resolveVariable(varName->getName());
    }

    if (!varDecl) {
        throwError(fmt::format("Variable '{}' not found", varName->getName()), expr);
    }

    const EvaluatedType varType = varDecl->getType();

    DateTimePart part {DateTimePart::Year};
    if (readsAComponentOfAProperty) {
        const Symbol* componentName = qualifiedName->back();

        if (!dateTimePartNamed(componentName->getName(), part)) {
            throwError(fmt::format("'{}' is not a component of a datetime", componentName->getName()), expr);
        }

        // A write naming a property the graph does not carry would introduce it, and a
        // component names none: turned away here, before the name is read as a new one
        if (allowCreate) {
            throwError("A datetime component cannot name a property.", expr);
        }
    }

    // d.year, where d was bound to an instant rather than to an entity
    if (varType == EvaluatedType::DateTime && !readsAComponentOfAProperty) {
        if (!dateTimePartNamed(propName->getName(), part)) {
            throwError(fmt::format("'{}' is not a component of a datetime", propName->getName()), expr);
        }

        expr->setEntityVarDecl(varDecl);
        expr->setDateTimePart(part);
        expr->setType(EvaluatedType::Integer);
        expr->setDynamic();

        expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, EvaluatedType::Integer));

        return ValueType::Int64;
    }

    if (varType == EvaluatedType::StringTable) {
        if (readsAComponentOfAProperty) {
            throwError(fmt::format("Field '{}' of '{}' is 'String', only a datetime has components",
                                   propName->getName(), varName->getName()),
                       expr);
        }

        // CSV header access: row.columnName
        expr->setEntityVarDecl(varDecl);
        expr->setPropertyName(propName->getName());
        expr->setStringTableHeaderAccess(true);
        expr->setType(EvaluatedType::String);
        expr->setDynamic();

        LoadCSVStmt* const loadCSV = findCSVSource(varDecl);
        if (loadCSV) {
            // Without a header line the file names no field, so there is nothing for a
            // header access to resolve against - only a position reaches a field
            if (!loadCSV->hasHeaders()) {
                throwError(fmt::format("'{}' was loaded without WITH HEADERS, so it has no field "
                                       "named '{}': read it by position, as {}[<index>]",
                                       varName->getName(), propName->getName(), varName->getName()),
                           expr);
            }

            const size_t slot = loadCSV->declareField(propName->getName());
            expr->setCSVFieldDecl(declareCSVField(*loadCSV, slot));
        }

        expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, EvaluatedType::String));

        return ValueType::String;
    }

    if (varType != EvaluatedType::NodePattern && varType != EvaluatedType::EdgePattern) {
        const std::string error = fmt::format(
            "Variable '{}' is '{}' it must be a node or edge",
            varName->getName(), EvaluatedTypeName::value(varType));

        throwError(error, expr);
    }

    if (varDecl->isQuantifiedPath()) {
        throwError(fmt::format("Variable '{}' binds the list of a variable-length path, "
                               "not a single entity: it has no property '{}'",
                               varName->getName(), propName->getName()),
                   expr);
    }

    const auto propTypeFound = _graphMetadata.propTypes().get(propName->getName());

    ValueType vt = ValueType::Invalid;

    // A name no property in the graph carries has no value on any row and no type: the
    // read is null.
    bool readsAsNull = false;

    if (!propTypeFound) {
        // Property does not exist yet

        const std::string_view name = propName->getName();
        auto it = _toBeCreatedTypes.find(name);

        if (it == _toBeCreatedTypes.end()) {
            if (allowCreate) {
                // Property does not exist but is created
                addToBeCreatedType(propName->getName(), defaultType, expr);
                it = _toBeCreatedTypes.find(name);
            } else {
                readsAsNull = true;
            }
        }

        if (!readsAsNull) {
            // Property is meant to be created in this query
            vt = it->second;
            expr->setCreatedValueType(vt);
        }

        expr->setPropertyName(name);
    } else {
        // Property already exists
        vt = propTypeFound.value()._valueType;
        expr->setPropertyName(propName->getName());
    }

    EvaluatedType type = EvaluatedType::Null;

    if (!readsAsNull) {
        const auto maybeEvalType = toEvaluatedType(vt);
        if (!maybeEvalType.has_value()) {
            const std::string_view name = propName->getName();
            const std::string error = fmt::format("Property type '{}' is invalid", name);
            throwError(error, expr);
        }

        type = *maybeEvalType;
    }

    if (readsAComponentOfAProperty) {
        const bool readsAnInstant = type == EvaluatedType::DateTime;

        // A name no property in the graph carries reads null on every row, and a component
        // of null is null too: there is no instant to turn away, only nothing to read
        if (!readsAnInstant && !readsAsNull) {
            throwError(fmt::format("Property '{}' is '{}', only a datetime has components",
                                   propName->getName(), EvaluatedTypeName::value(type)),
                       expr);
        }

        expr->setDateTimePart(part);

        if (readsAnInstant) {
            type = EvaluatedType::Integer;
            vt = ValueType::Int64;
        }
    }

    expr->setEntityVarDecl(varDecl);
    expr->setType(type);
    expr->setDynamic();

    expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, expr->getType()));

    return vt;
}

void ExprAnalyzer::throwIfReadsADateTimeComponent(const PropertyExpr* expr) {
    if (expr->readsADateTimeComponent()) {
        throwError("A datetime component cannot name a property.", expr);
    }
}

void ExprAnalyzer::analyzePropertyLookupExpr(PropertyLookupExpr* expr) {
    Expr* base = expr->getBase();
    analyzeExpr(base);

    const EvaluatedType baseType = base->getType();
    const std::string_view propName = expr->getPropName();

    const bool readsAnEntity = baseType == EvaluatedType::NodePattern
                            || baseType == EvaluatedType::EdgePattern;

    EvaluatedType type = EvaluatedType::Null;

    if (baseType == EvaluatedType::DateTime) {
        DateTimePart part {DateTimePart::Year};
        if (!dateTimePartNamed(propName, part)) {
            throwError(fmt::format("'{}' is not a component of a datetime", propName), expr);
        }

        expr->setDateTimePart(part);
        type = EvaluatedType::Integer;
    } else if (readsAnEntity) {
        const auto propTypeFound = _graphMetadata.propTypes().get(propName);

        if (propTypeFound) {
            const auto maybeEvalType = toEvaluatedType(propTypeFound.value()._valueType);
            if (!maybeEvalType.has_value()) {
                throwError(fmt::format("Property type '{}' is invalid", propName), expr);
            }

            type = *maybeEvalType;
        }
    } else if (baseType != EvaluatedType::Null) {
        throwError(fmt::format("A value of type '{}' has no property '{}'",
                               EvaluatedTypeName::value(baseType), propName),
                   expr);
    }

    expr->setType(type);

    if (base->isDynamic()) {
        expr->setDynamic();
    }

    if (base->isAggregate()) {
        expr->setAggregate();
    }

    expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, type));
}

void ExprAnalyzer::analyzeIndexExpr(IndexExpr* expr) {
    Expr* base = expr->getBase();
    Expr* indexExpr = expr->getIndexExpr();

    analyzeExpr(base);
    analyzeExpr(indexExpr);

    const EvaluatedType baseType = base->getType();

    const bool indexesAList = baseType == EvaluatedType::List
                           || baseType == EvaluatedType::ListItem;
    const bool indexesACSVRow = baseType == EvaluatedType::StringTable;

    if (!indexesAList && !indexesACSVRow) {
        throwError(fmt::format("Index operator [] can only be applied to a list or a CSV row, not '{}'",
                               EvaluatedTypeName::value(baseType)), expr);
    }

    const EvaluatedType indexType = indexExpr->getType();

    const bool indexesByPosition = indexType == EvaluatedType::Integer;
    const bool indexesByNull = indexesAList && indexType == EvaluatedType::Null;

    if (!indexesByPosition && !indexesByNull) {
        throwError(fmt::format("Index expression must be an integer, not '{}'",
                               EvaluatedTypeName::value(indexType)), expr);
    }

    if (indexesAList) {
        const EvaluatedType elementType = base->getListShape().unwoundType();
        const bool readsAValue = convertibleToValueType(elementType);
        const bool readsAnEntity = elementType == EvaluatedType::NodePattern
                                || elementType == EvaluatedType::EdgePattern;
        const bool readsTheElementType = readsAValue || readsAnEntity;
        const EvaluatedType indexedType = readsTheElementType ? elementType : EvaluatedType::ListItem;

        expr->setType(indexedType);

        if (base->isDynamic() || indexExpr->isDynamic()) {
            expr->setDynamic();
        }

        if (base->isAggregate() || indexExpr->isAggregate()) {
            expr->setAggregate();
        }

        expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, indexedType));

        return;
    }

    // Detect literal index for compile-time optimization
    if (indexExpr->getKind() == Expr::Kind::LITERAL) {
        const LiteralExpr* lit = static_cast<const LiteralExpr*>(indexExpr);
        if (lit->getLiteral()->getKind() == Literal::Kind::INTEGER) {
            const int64_t val = static_cast<const IntegerLiteral*>(lit->getLiteral())->getValue();
            if (val >= 0) {
                expr->setLiteralIndex(static_cast<size_t>(val));
            } else {
                throwError("CSV row index must be non-negative", expr);
            }
        }
    }

    expr->setType(EvaluatedType::String);
    expr->setDynamic();

    // A computed index names no field of the load: which column it reads is only known
    // once the row is in hand, so it keeps an access of its own and the engine that
    // cannot build one reports the gap
    LoadCSVStmt* const loadCSV = findCSVSource(base->getExprVarDecl());
    if (loadCSV && expr->hasLiteralIndex()) {
        const size_t slot = loadCSV->declareField(expr->getLiteralIndex());
        expr->setCSVFieldDecl(declareCSVField(*loadCSV, slot));
    }

    expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, EvaluatedType::String));
}

void ExprAnalyzer::registerCSVSource(const VarDecl* alias, LoadCSVStmt* loadCSV) {
    _csvSources[alias] = loadCSV;
}

LoadCSVStmt* ExprAnalyzer::findCSVSource(const VarDecl* alias) const {
    const auto foundIt = _csvSources.find(alias);
    if (foundIt == end(_csvSources)) {
        return nullptr;
    }

    return foundIt->second;
}

VarDecl* ExprAnalyzer::declareCSVField(LoadCSVStmt& loadCSV, size_t slot) {
    VarDecl* decl = loadCSV.getField(slot)._decl;
    if (decl) {
        return decl;
    }

    decl = _ctxt->createUnnamedVariable(_ast, EvaluatedType::String);
    loadCSV.setFieldDecl(slot, decl);

    return decl;
}

void ExprAnalyzer::analyzeStringExpr(StringExpr* expr) {
    Expr* lhs = expr->getLHS();
    Expr* rhs = expr->getRHS();

    analyzeExpr(lhs);
    analyzeExpr(rhs);

    const EvaluatedType lhsType = lhs->getType();
    const EvaluatedType rhsType = rhs->getType();

    // A type-erased cell carries its own type, so the characters it holds are read row by
    // row, exactly as an equality against one is.
    const bool lhsReadsAsString = lhsType == EvaluatedType::String
                               || lhsType == EvaluatedType::ListItem;
    const bool rhsReadsAsString = rhsType == EvaluatedType::String
                               || rhsType == EvaluatedType::ListItem;

    if (!lhsReadsAsString || !rhsReadsAsString) {
        const std::string error = fmt::format(
            "String expressions operands must be strings, not '{}' and '{}'",
            EvaluatedTypeName::value(lhsType),
            EvaluatedTypeName::value(rhsType));

        throwError(error, expr);
    }

    expr->setType(EvaluatedType::Bool);

    if (lhs->isDynamic() || rhs->isDynamic()) {
        expr->setDynamic();
    }

    if (lhs->isAggregate() || rhs->isAggregate()) {
        expr->setAggregate();
    }

    // Create a variable declaration for the entity type expression
    // so that it can be retrieved later (for projection or in an expression / filter)
    expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, expr->getType()));
}

void ExprAnalyzer::analyzeEntityTypeExpr(EntityTypeExpr* expr) {
    expr->setType(EvaluatedType::Bool);

    VarDecl* decl = resolveVariable(expr->getSymbol()->getName());

    if (!decl) {
        throwError(fmt::format("Variable '{}' not found", expr->getSymbol()->getName()), expr);
    }

    if (decl->getType() != EvaluatedType::NodePattern
        && decl->getType() != EvaluatedType::EdgePattern) {
        const std::string error = fmt::format("Variable '{}' is '{}'. Must be NodePattern or EdgePattern",
                                              decl->getName(), EvaluatedTypeName::value(decl->getType()));

        throwError(error, expr);
    }

    if (decl->isQuantifiedPath()) {
        throwError(fmt::format("Variable '{}' binds the list of a variable-length path, "
                               "not a single entity: it has no label or type",
                               decl->getName()),
                   expr);
    }

    expr->setEntityDecl(decl);
    expr->setDynamic();
    expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, expr->getType()));
}

void ExprAnalyzer::analyzeFuncInvocExpr(FunctionInvocationExpr* expr, FunctionResolver* resolver) {
    const FunctionInvocation* invoc = expr->getFunctionInvocation();
    const std::vector<Symbol*>& names = invoc->getName()->names();

    std::string name;

    for (size_t i = 0; i < names.size(); i++) {
        const Symbol* symbol = names[i];
        name += symbol->getName();

        if (i < names.size() - 1) {
            name += ".";
        }
    }

    const auto signatures = resolver->lookup(name);

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
    for (FunctionSignature* signature : signatures) {
        const auto& expectedArgs = signature->argumentTypes();

        // A signature unifying its arguments declares none of them, so its arity is a
        // minimum alone and there is no expected type to match each against: they are
        // checked against each other once the overload is settled on.
        const bool unifiesArguments = signature->unifiesItsArguments();

        const size_t minArgs = signature->getMinArgCount();
        const size_t maxArgs = expectedArgs.size();

        const bool tooFewArgs = providedArgs.size() < minArgs;
        const bool tooManyArgs = !unifiesArguments && providedArgs.size() > maxArgs;

        if (tooFewArgs || tooManyArgs) {
            // Number of arguments does not match
            continue;
        }

        // A scalar function answers null over a null argument, so a null stands in for
        // whichever type the signature declares. An aggregate declares its own null overload
        const bool answersNullOverNull = !unifiesArguments && !signature->isAggregate();

        if (!unifiesArguments) {
            const bool matchingArgs = std::equal(
                expectedArgs.begin(), expectedArgs.begin() + providedArgs.size(),
                providedArgs.begin(), [answersNullOverNull](const FunctionArgumentType& expected, const Expr* arg) {
                    const EvaluatedType argType = arg->getType();
                    return argType == expected.getType() || (answersNullOverNull && argType == EvaluatedType::Null);
                });

            if (!matchingArgs) {
                // Argument types do not match
                continue;
            }
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

        const bool readsANull = std::any_of(providedArgs.begin(), providedArgs.end(), [](const Expr* arg) {
            return arg->getType() == EvaluatedType::Null;
        });

        // Found a valid signature
        if (answersNullOverNull && readsANull) {
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

        const ListShape& returnedShape = signature->returnedListShape();
        if (returnedShape.isList() && expr->getType() == EvaluatedType::List) {
            expr->setListShape(returnedShape);
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

    // Checked all overloaded signatures, none match: error
    throwError(fmt::format("Invalid arguments for function '{}'", name), expr);
}

EvaluatedType ExprAnalyzer::unifiedArgumentType(std::string_view name,
                                                std::span<Expr* const> args) const {
    EvaluatedType unified = EvaluatedType::Null;

    for (const Expr* arg : args) {
        const EvaluatedType argType = arg->getType();
        const EvaluatedType folded = unifiedBranchType(unified, argType);

        if (folded == EvaluatedType::Invalid) {
            throwError(fmt::format("'{}' answers one column, so its arguments must share a "
                                   "type: '{}' and '{}' cannot be mixed",
                                   name,
                                   EvaluatedTypeName::value(unified),
                                   EvaluatedTypeName::value(argType)),
                       arg);
        }

        unified = folded;
    }

    return unified;
}

void ExprAnalyzer::addToBeCreatedType(std::string_view name, ValueType type, const void* obj) {
    const auto it = _toBeCreatedTypes.find(name);

    if (it != _toBeCreatedTypes.end()) {
        // Type was already registered

        if (it->second == type) {
            // Same types -> this is ok
            return;
        }

        throwError(fmt::format("Property type '{}' already exists with a different type '{}' vs. '{}'",
                               name,
                               ValueTypeName::value(it->second),
                               ValueTypeName::value(type)),
                   obj);
    }

    // Register the new type
    _toBeCreatedTypes[name] = type;
}

bool ExprAnalyzer::propTypeCompatible(ValueType vt, EvaluatedType exprType) {
    switch (exprType) {
        case EvaluatedType::Null:
        case EvaluatedType::NodePattern:
        case EvaluatedType::EdgePattern:
        case EvaluatedType::StringTable:
            return false;
        case EvaluatedType::Integer:
            return vt == ValueType::Int64 || vt == ValueType::UInt64 || vt == ValueType::Double;
        case EvaluatedType::Double:
            return vt == ValueType::Double;
        case EvaluatedType::String:
        case EvaluatedType::Char:
            return vt == ValueType::String;
        case EvaluatedType::Bool:
            return vt == ValueType::Bool;
        case EvaluatedType::Embedding:
            return vt == ValueType::Embedding;
        case EvaluatedType::List:
            return vt == ValueType::List;
        case EvaluatedType::DateTime:
            return vt == ValueType::DateTime;
        case EvaluatedType::Map:
            return vt == ValueType::Map;
        case EvaluatedType::Wildcard:
        case EvaluatedType::Invalid:
        case EvaluatedType::Tuple:
        case EvaluatedType::GraphPath:
        case EvaluatedType::ValueType:
        case EvaluatedType::Label:
        case EvaluatedType::LabelSet:
        case EvaluatedType::PropertyType:
        case EvaluatedType::EdgeType:
        case EvaluatedType::_SIZE:
        case EvaluatedType::ListItem:
            return false;
        break;
    }

    return false;
}

// Used to generate a "fake" variable (n) in index creation queries, such as
// CREATE INDEX _ FOR (n) ON n._
void ExprAnalyzer::registerNodePatternDeclaration(const NodePattern* node) {
    const Symbol* nodeSymbol = node->getSymbol();
    if (!nodeSymbol) {
        throwError("Failed to get symbol to register NodePattern.", node);
    }

    const std::string_view nodeName = nodeSymbol->getName();

    const bool alreadyExists = _ctxt->hasDecl(nodeName);

    if (alreadyExists) {
        throwError("Attempted to register NodePattern which was already defined.", node);
    }

    _ctxt->getOrCreateNamedVariable(_ast, EvaluatedType::NodePattern, nodeName);
}

// Used to generate a "fake" variable [e] in index creation queries, such as
// CREATE INDEX _ FOR [e] ON e._
void ExprAnalyzer::registerEdgePatternDeclaration(const EdgePattern* edge) {
    const Symbol* edgeSymbol = edge->getSymbol();
    if (!edgeSymbol) {
        throwError("Failed to get symbol to register EdgePattern.", edge);
    }

    const std::string_view edgeName = edgeSymbol->getName();

    const bool alreadyExists = _ctxt->hasDecl(edgeName);

    if (alreadyExists) {
        throwError("Attempted to register EdgePattern which was already defined.", edge);
    }

    _ctxt->getOrCreateNamedVariable(_ast, EvaluatedType::EdgePattern, edgeName);
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

    EvaluatedType resultType = EvaluatedType::Null;

    for (const CaseExpr::Branch& branch : expr->getBranches()) {
        for (const CaseExpr::Test& test : branch._tests) {
            analyzeCaseTest(branch, subject, test);

            if (test._value) {
                contaminate(test._value);
            }
        }

        analyzeExpr(branch._then);

        resultType = unifyCaseBranch(resultType, branch._then);

        contaminate(branch._then);
    }

    Expr* const elseExpr = expr->getElseExpr();
    if (elseExpr) {
        analyzeExpr(elseExpr);
        resultType = unifyCaseBranch(resultType, elseExpr);
        contaminate(elseExpr);
    }

    expr->setType(resultType);
}

void ExprAnalyzer::analyzeCaseTest(const CaseExpr::Branch& branch,
                                   const Expr* subject,
                                   const CaseExpr::Test& test) {
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
        if (whenType != EvaluatedType::Bool && whenType != EvaluatedType::Null) {
            throwError(fmt::format("The WHEN condition of a CASE must be a boolean, not '{}'",
                                   EvaluatedTypeName::value(whenType)),
                       test._value);
        }

        return;
    }

    if (testsNull) {
        return;
    }

    analyzeExpr(test._value);

    if (isEntity(subject->getType())) {
        requireComparableToEntity(subject, test);
    } else {
        requireCaseValue(test._value);
    }
}

EvaluatedType ExprAnalyzer::unifyCaseBranch(EvaluatedType carried, const Expr* branch) {
    const EvaluatedType branchType = branch->getType();
    const EvaluatedType unified = unifiedBranchType(carried, branchType);

    if (unified == EvaluatedType::Invalid) {
        throwError(fmt::format("A CASE returns one column, so its branches must share a "
                               "type: '{}' and '{}' cannot be mixed",
                               EvaluatedTypeName::value(carried),
                               EvaluatedTypeName::value(branchType)),
                   branch);
    }

    return unified;
}

void ExprAnalyzer::requireCaseSubject(const Expr* subject) const {
    const EvaluatedType type = subject->getType();

    const bool isScalar = type == EvaluatedType::Null || convertibleToValueType(type);

    if (isScalar || isEntity(type)) {
        return;
    }

    throwError(fmt::format("The subject of a CASE must be a scalar, a node or an edge, not '{}'",
                           EvaluatedTypeName::value(type)),
               subject);
}

void ExprAnalyzer::requireCaseValue(const Expr* value) const {
    const EvaluatedType type = value->getType();

    if (type == EvaluatedType::Null || convertibleToValueType(type)) {
        return;
    }

    throwError(fmt::format("The values a CASE compares its subject against must be scalars, "
                           "not '{}'",
                           EvaluatedTypeName::value(type)),
               value);
}

void ExprAnalyzer::requireComparableToEntity(const Expr* subject, const CaseExpr::Test& test) const {
    const EvaluatedType subjectType = subject->getType();
    const EvaluatedType valueType = test._value->getType();

    const bool comparesForEquality = test._kind == CaseExpr::TestKind::Value
                                     || test._operator == BinaryOperator::Equal;

    if (!comparesForEquality) {
        throwError("A CASE compares a node or an edge for equality, so its branches cannot "
                   "order one",
                   test._value);
    }

    const bool comparable = valueType == EvaluatedType::Null
                            || valueType == EvaluatedType::Integer
                            || valueType == subjectType;

    if (!comparable) {
        throwError(fmt::format("A node or an edge is equal to one of its kind or to an id, "
                               "not to '{}'",
                               EvaluatedTypeName::value(valueType)),
                   test._value);
    }
}

void ExprAnalyzer::analyzeListSliceExpr(ListSliceExpr* expr) {
    Expr* const base = expr->getBase();
    analyzeExpr(base);

    const EvaluatedType baseType = base->getType();

    // A tagged cell names no type until a row is in hand, and a null slices into a null
    const bool slicesAList = baseType == EvaluatedType::List
                          || baseType == EvaluatedType::ListItem
                          || baseType == EvaluatedType::Null;

    if (!slicesAList) {
        throwError(fmt::format("A slice reads a list, not '{}'", EvaluatedTypeName::value(baseType)),
                   expr);
    }

    bool dynamic = base->isDynamic();
    bool aggregate = base->isAggregate();

    for (Expr* const bound : {expr->getFrom(), expr->getTo()}) {
        if (!bound) {
            continue;
        }

        analyzeExpr(bound);

        const EvaluatedType boundType = bound->getType();
        const bool countsPositions = boundType == EvaluatedType::Integer
                                  || boundType == EvaluatedType::ListItem
                                  || boundType == EvaluatedType::Null;

        if (!countsPositions) {
            throwError(fmt::format("A slice counts its bounds in integers, not '{}'",
                                   EvaluatedTypeName::value(boundType)),
                       expr);
        }

        dynamic = dynamic || bound->isDynamic();
        aggregate = aggregate || bound->isAggregate();
    }

    // Slicing an unknown value is unknown, as adding to one is: null[1..2] is null. The
    // elements are otherwise the ones the base holds, so the slice keeps its shape
    const EvaluatedType slicedType = baseType == EvaluatedType::Null ? EvaluatedType::Null
                                                                    : EvaluatedType::List;

    expr->setType(slicedType);
    expr->setListShape(base->getListShape());

    if (dynamic) {
        expr->setDynamic();
    }

    if (aggregate) {
        expr->setAggregate();
    }

    expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, slicedType));
}

void ExprAnalyzer::analyzeListExpr(ListExpr* expr) {
    analyzeListElements(expr, expr->getElements());
}

void ExprAnalyzer::analyzeListComprehensionExpr(ListComprehensionExpr* expr) {
    Expr* const source = expr->getSource();
    analyzeExpr(source);

    const EvaluatedType sourceType = source->getType();

    // A tagged cell names no type until a row is in hand, and a null iterates into a null
    // rather than into a type error - as an UNWIND of one emits no row
    const bool iteratesAList = sourceType == EvaluatedType::List
                            || sourceType == EvaluatedType::ListItem
                            || sourceType == EvaluatedType::Null;

    if (!iteratesAList) {
        throwError(fmt::format("A list comprehension iterates a list, not '{}'",
                               EvaluatedTypeName::value(sourceType)),
                   expr);
    }

    const std::string_view itemName = expr->getSymbol()->getName();

    // The comprehension names a new variable, as an UNWIND does, so a name already in
    // scope would put two of them under one name
    if (_ctxt->hasDecl(itemName)) {
        throwError(fmt::format("Variable '{}' is already declared", itemName), expr);
    }

    const ListShape& sourceShape = source->getListShape();

    VarDecl* const itemDecl = _ctxt->getOrCreateNamedVariable(_ast, sourceShape.unwoundType(), itemName);
    itemDecl->setIsUnwound(true);
    itemDecl->setListShape(sourceShape.unwound());

    expr->setDecl(itemDecl);

    Expr* const predicate = expr->getPredicate();
    if (predicate) {
        analyzeExpr(predicate);
    }

    Expr* const projection = expr->getProjection();
    if (projection) {
        analyzeExpr(projection);
    }

    _ctxt->dropVariable(itemName);

    if (predicate && predicate->getType() != EvaluatedType::Bool) {
        throwError("The WHERE of a list comprehension must be a boolean", predicate);
    }

    const bool aggregatesTheBody = (predicate && predicate->isAggregate())
                                || (projection && projection->isAggregate());

    if (aggregatesTheBody) {
        throwError(fmt::format("Aggregate functions may not be used over the elements of a "
                               "list comprehension: '{}' names one element, not a group",
                               itemName),
                   expr);
    }

    expr->setType(EvaluatedType::List);

    // Filtering leaves the shape the source has; a projection replaces the elements, so
    // the list gathers whatever it computes - what an UNWIND of the comprehension binds
    if (projection) {
        expr->setListShape(ListShape::collecting(projection->getType(), projection->getListShape()));
    } else {
        expr->setListShape(sourceShape);
    }

    // The elements are read row by row, so the list is never the compile-time value an
    // argument declared constant takes
    expr->setDynamic();

    // An aggregated source aggregates the comprehension: the projection reduces the rows
    // to one, and the list is built over that one
    if (source->isAggregate()) {
        expr->setAggregate();
    }

    expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, EvaluatedType::List));
}

void ExprAnalyzer::analyzePatternComprehensionExpr(PatternComprehensionExpr* expr) {
    const Pattern* pattern = expr->getPattern();

    // A name the pattern binds that the scope does not hold yet is the comprehension's
    // own: the WHERE and the projection read it, and nothing outside them does
    std::vector<std::string_view> ownVariables;
    std::vector<const EntityPattern*> ownEntities;

    for (const PatternElement* element : pattern->elements()) {
        for (const EntityPattern* entity : element->getEntities()) {
            const Symbol* symbol = entity->getSymbol();

            if (!symbol || _ctxt->hasDecl(symbol->getName())) {
                continue;
            }

            ownVariables.push_back(symbol->getName());
            ownEntities.push_back(entity);

            // A SET analyzes the expression it assigns twice, and the reads in the body
            // hold the declarations the first pass bound: binding the names back to them
            // leaves the second pass reading the variables the pattern already has, not
            // fresh ones the traversal knows nothing about.
            if (VarDecl* bound = entity->getDecl()) {
                _ctxt->declareAlias(symbol->getName(), bound);
            }
        }
    }

    _readAnalyzer->analyze(pattern);

    PatternComprehensionExpr::OwnDecls ownDecls;
    for (const EntityPattern* entity : ownEntities) {
        ownDecls.push_back(entity->getDecl());
    }

    expr->setOwnDecls(ownDecls);

    Expr* const projection = expr->getProjection();
    analyzeExpr(projection);

    for (const std::string_view name : ownVariables) {
        _ctxt->dropVariable(name);
    }

    if (projection->isAggregate()) {
        throwError("Aggregate functions may not be used over the matches of a pattern "
                   "comprehension: the pattern names one match, not a group",
                   expr);
    }

    expr->setType(EvaluatedType::List);
    expr->setListShape(ListShape::collecting(projection->getType(), projection->getListShape()));

    // The pattern is matched row by row, so the list is never the compile-time value an
    // argument declared constant takes
    expr->setDynamic();

    expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, EvaluatedType::List));
}

void ExprAnalyzer::analyzeListElements(Expr* expr, std::span<Expr* const> elements) {
    for (Expr* element : elements) {
        analyzeExpr(element);

        // An element is an expression of its own, so its flags are the list's: the n of
        // [n, m] reads a row, and so does the {age: n.age} of ORDER BY [{age: n.age}]
        if (element->isDynamic()) {
            expr->setDynamic();
        }

        if (element->isAggregate()) {
            expr->setAggregate();
        }
    }

    expr->setListShape(sharedListShape(elements));
}

void ExprAnalyzer::analyzeMapEntries(Expr* expr, const MapLiteral* map) {
    // The keys of a map are symbols written in the query, so only its values can make it
    // vary or aggregate
    for (const auto& [key, value] : *map) {
        analyzeExpr(value);

        if (value->isDynamic()) {
            expr->setDynamic();
        }

        if (value->isAggregate()) {
            expr->setAggregate();
        }
    }
}

void ExprAnalyzer::throwError(std::string_view msg, const void* obj) const {
    std::string errorStr;
    _ast->getDiagnosticsManager()->createErrorString(msg, obj, errorStr);
    throw AnalyzeException(std::move(errorStr));
}
