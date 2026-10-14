#include "BinaryOperatorTyping.h"

#include <string>

#include <spdlog/fmt/fmt.h>

using namespace db;

namespace {

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

bool computesTimeOverListItem(TypePairBitset pair) {
    return pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::DateTime)
        || pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::Duration);
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

}

void BinaryOperatorTyping::type(BinaryOperator op, const BinaryOperand& lhs, const BinaryOperand& rhs, BinaryTyping& out) {
    switch (op) {
        case BinaryOperator::NotEqual:
        case BinaryOperator::Equal:
            typeEquality(lhs, rhs, out);
        break;
        case BinaryOperator::IsNull:
        case BinaryOperator::IsNotNull:
            typeNullTest(lhs, rhs, out);
        break;
        case BinaryOperator::LessThan:
        case BinaryOperator::GreaterThan:
        case BinaryOperator::LessThanOrEqual:
        case BinaryOperator::GreaterThanOrEqual:
            typeOrdering(lhs, rhs, out);
        break;
        case BinaryOperator::Add:
            typeAddition(lhs, rhs, out);
        break;
        case BinaryOperator::Concat:
            typeConcatenation(lhs, rhs, out);
        break;
        case BinaryOperator::Sub:
        case BinaryOperator::Mult:
        case BinaryOperator::Div:
        case BinaryOperator::Mod:
            typeArithmetic(op, lhs, rhs, out);
        break;
        case BinaryOperator::Pow:
            typePower(lhs, rhs, out);
        break;
        case BinaryOperator::In:
            typeMembership(lhs, rhs, out);
        break;
        case BinaryOperator::_SIZE:
            out._error = "Invalid operand in binary expression.";
        break;
    }
}

void BinaryOperatorTyping::typeEquality(const BinaryOperand& lhs, const BinaryOperand& rhs, BinaryTyping& out) {
    const EvaluatedType a = lhs._type;
    const EvaluatedType b = rhs._type;
    const TypePairBitset pair(a, b);

    out._type = EvaluatedType::Bool;

    if (pair == TypePairBitset(EvaluatedType::Double, EvaluatedType::Double)) {
        const std::string error = fmt::format(
            "Equality of types '{}' and '{}' is not encouraged due to "
            "potential rounding innaccuracy. Please constrain with '<' "
            "and '>' instead.",
            EvaluatedTypeName::value(a), EvaluatedTypeName::value(b));
        out._error = error;
        return;
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
        || pair == TypePairBitset(EvaluatedType::DateTime, EvaluatedType::DateTime)
        || pair == TypePairBitset(EvaluatedType::Duration, EvaluatedType::Duration)) {
        return;
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
        || pair == TypePairBitset(EvaluatedType::Duration, EvaluatedType::Null)
    ) {
        return;
    }

    // A type-erased cell is equal only to a cell holding the same value, so it
    // compares against the types it can hold - a list and a null among them, the
    // null being what IS (NOT) NULL tests a cell for
    const bool comparesListItem =
        pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::ListItem)
        || pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::Integer)
        || pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::Double)
        || pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::String)
        || pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::Char)
        || pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::Bool)
        || pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::DateTime)
        || pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::Duration)
        || pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::Null)
        || pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::List)
        || pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::Map)
        || pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::NodePattern)
        || pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::EdgePattern);

    // A map value is a type-erased cell too, so it compares against the types a
    // map can hold, and against null for IS (NOT) NULL
    const bool comparesMapValue =
        pair == TypePairBitset(EvaluatedType::MapValue, EvaluatedType::MapValue)
        || pair == TypePairBitset(EvaluatedType::MapValue, EvaluatedType::Integer)
        || pair == TypePairBitset(EvaluatedType::MapValue, EvaluatedType::Double)
        || pair == TypePairBitset(EvaluatedType::MapValue, EvaluatedType::String)
        || pair == TypePairBitset(EvaluatedType::MapValue, EvaluatedType::Char)
        || pair == TypePairBitset(EvaluatedType::MapValue, EvaluatedType::Bool)
        || pair == TypePairBitset(EvaluatedType::MapValue, EvaluatedType::Null)
        || pair == TypePairBitset(EvaluatedType::MapValue, EvaluatedType::List)
        || pair == TypePairBitset(EvaluatedType::MapValue, EvaluatedType::Map);

    // A stored list compares against another list, and against null for
    // IS (NOT) NULL
    const bool comparesList =
        pair == TypePairBitset(EvaluatedType::List, EvaluatedType::List)
        || pair == TypePairBitset(EvaluatedType::List, EvaluatedType::Null);

    const bool comparesMap =
        pair == TypePairBitset(EvaluatedType::Map, EvaluatedType::Map)
        || pair == TypePairBitset(EvaluatedType::Map, EvaluatedType::Null);

    if (comparesListItem || comparesMapValue || comparesList || comparesMap) {
        return;
    }

    // Values of two types that can never be equal are not equal, so the comparison
    // answers false rather than turning the query away
    if (comparesAsDisjointTypes(a, b)) {
        return;
    }

    // Allows NodeID <-> NodeID and NodeID <-> Integer comparisons
    if (pair == TypePairBitset(EvaluatedType::NodePattern,
                               EvaluatedType::NodePattern)
        || pair == TypePairBitset(EvaluatedType::Integer,
                                  EvaluatedType::NodePattern)) {
        return;
    }

    // Allows EdgeID <-> EdgeID and EdgeID <-> Integer comparisons
    if (pair == TypePairBitset(EvaluatedType::EdgePattern,
                               EvaluatedType::EdgePattern)
        || pair == TypePairBitset(EvaluatedType::Integer,
                                  EvaluatedType::EdgePattern)) {
        return;
    }

    // n IS NULL over a node or an edge, which an OPTIONAL MATCH leaves null when
    // its pattern missed
    const bool comparesEntityToNull =
        pair == TypePairBitset(EvaluatedType::NodePattern, EvaluatedType::Null)
        || pair == TypePairBitset(EvaluatedType::EdgePattern, EvaluatedType::Null);

    if (comparesEntityToNull) {
        return;
    }

    const std::string error = fmt::format(
        "Operands are not valid or compatible types: '{}' and '{}'",
        EvaluatedTypeName::value(a),
        EvaluatedTypeName::value(b));

    out._error = error;
}

void BinaryOperatorTyping::typeNullTest(const BinaryOperand& lhs, const BinaryOperand& rhs, BinaryTyping& out) {
    const EvaluatedType b = rhs._type;

    out._type = EvaluatedType::Bool;

    if (b == EvaluatedType::Null) {
        return;
    }

    out._error = fmt::format("IS tests whether its operand is null, so its right side "
                             "must be NULL, not '{}'",
                             EvaluatedTypeName::value(b));
}

void BinaryOperatorTyping::typeOrdering(const BinaryOperand& lhs, const BinaryOperand& rhs, BinaryTyping& out) {
    const EvaluatedType a = lhs._type;
    const EvaluatedType b = rhs._type;
    const TypePairBitset pair(a, b);

    out._type = EvaluatedType::Bool;

    if (pair == TypePairBitset(EvaluatedType::Integer, EvaluatedType::Integer)
        || pair == TypePairBitset(EvaluatedType::Double, EvaluatedType::Double)
        || pair == TypePairBitset(EvaluatedType::Integer, EvaluatedType::Double)
        || pair == TypePairBitset(EvaluatedType::String, EvaluatedType::String)
        || pair == TypePairBitset(EvaluatedType::Bool, EvaluatedType::Bool)
        || pair == TypePairBitset(EvaluatedType::DateTime, EvaluatedType::DateTime)
        || pair == TypePairBitset(EvaluatedType::Duration, EvaluatedType::Duration)
        || pair == TypePairBitset(EvaluatedType::List, EvaluatedType::List)) {
        // Valid pair
        return;
    }

    // Ordering a value against a null is null, the same as comparing it against one
    const bool ordersAgainstNull = a == EvaluatedType::Null || b == EvaluatedType::Null;

    if (ordersAgainstNull) {
        return;
    }

    // A type-erased cell is ordered as an element holding the other side would be,
    // so it orders against the scalar types it can hold
    const bool ordersListItem =
        pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::ListItem)
        || pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::Integer)
        || pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::Double)
        || pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::String)
        || pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::Char)
        || pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::Bool)
        || pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::DateTime)
        || pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::Duration)
        || pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::List);

    if (ordersListItem) {
        return;
    }

    const std::string error = fmt::format(
        "Operands are not valid or compatible numeric types: '{}' and '{}'",
        EvaluatedTypeName::value(a),
        EvaluatedTypeName::value(b));

    out._error = error;
}

void BinaryOperatorTyping::typeAddition(const BinaryOperand& lhs, const BinaryOperand& rhs, BinaryTyping& out) {
    const EvaluatedType a = lhs._type;
    const EvaluatedType b = rhs._type;
    const TypePairBitset pair(a, b);

    if (pair == TypePairBitset(EvaluatedType::Integer, EvaluatedType::Integer)) {
        out._type = EvaluatedType::Integer;
        return;
    }

    if (pair == TypePairBitset(EvaluatedType::Double, EvaluatedType::Double)
        || pair == TypePairBitset(EvaluatedType::Double, EvaluatedType::Integer)) {
        out._type = EvaluatedType::Double;
        return;
    }

    if (pair == TypePairBitset(EvaluatedType::String, EvaluatedType::String)) {
        out._type = EvaluatedType::String;
        return;
    }

    if (pair == TypePairBitset(EvaluatedType::List, EvaluatedType::List)) {
        out._type = EvaluatedType::List;
        out._shape = concatenatedListShape(lhs._shape, rhs._shape);
        return;
    }

    // A scalar joins a list as the one-element list it stands for, from either
    // side: [1, 2] + 3 and 1 + [2, 3] are both [1, 2, 3]
    const bool leftIsAList = a == EvaluatedType::List;
    const bool oneSideIsAList = leftIsAList || b == EvaluatedType::List;
    const EvaluatedType scalar = leftIsAList ? b : a;

    if (oneSideIsAList && joinsAList(scalar)) {
        const ListShape& listShape = leftIsAList ? lhs._shape : rhs._shape;

        out._type = EvaluatedType::List;
        out._shape = appendedListShape(listShape, scalar);
        return;
    }

    // A cell holding text concatenates with a string, as the string it holds
    // would: the type it holds is settled row by row, so a cell holding a number
    // joins as the text Cypher writes that number with
    if (concatenatesListItem(pair) || concatenatesAsText(pair)) {
        out._type = EvaluatedType::String;
        return;
    }

    if (pair == TypePairBitset(EvaluatedType::DateTime, EvaluatedType::Duration)) {
        out._type = EvaluatedType::DateTime;
        return;
    } else if (pair == TypePairBitset(EvaluatedType::Duration, EvaluatedType::Duration)) {
        out._type = EvaluatedType::Duration;
        return;
    } else if (computesTimeOverListItem(pair)) {
        out._type = EvaluatedType::ListItem;
        return;
    }

    // Arithmetic over an unknown value is unknown, whatever the other side holds.
    // The list cases come first: [1, 2] + null appends the null instead
    if (a == EvaluatedType::Null || b == EvaluatedType::Null) {
        out._type = EvaluatedType::Null;
        return;
    }

    if (computesOverListItem(pair)) {
        out._type = EvaluatedType::ListItem;
        return;
    }

    const std::string error = fmt::format(
        "Operands are not valid and compatible types for '+': '{}' and '{}'",
        EvaluatedTypeName::value(a),
        EvaluatedTypeName::value(b));

    out._error = error;
}

void BinaryOperatorTyping::typeConcatenation(const BinaryOperand& lhs, const BinaryOperand& rhs, BinaryTyping& out) {
    const EvaluatedType a = lhs._type;
    const EvaluatedType b = rhs._type;
    const TypePairBitset pair(a, b);

    if (pair == TypePairBitset(EvaluatedType::String, EvaluatedType::String)) {
        out._type = EvaluatedType::String;
        return;
    }

    if (pair == TypePairBitset(EvaluatedType::List, EvaluatedType::List)) {
        out._type = EvaluatedType::List;
        out._shape = concatenatedListShape(lhs._shape, rhs._shape);
        return;
    }

    if (concatenatesListItem(pair) || concatenatesTwoListItems(pair)) {
        out._type = EvaluatedType::String;
        return;
    }

    // '||' joins two strings or two lists, so an unknown value is no side of one
    // rather than an element to append: [1, 2] || null is null where [1, 2] + null
    // is [1, 2, null]
    if (a == EvaluatedType::Null || b == EvaluatedType::Null) {
        out._type = EvaluatedType::Null;
        return;
    }

    const std::string error = fmt::format(
        "Operands are not valid and compatible types for '||': '{}' and '{}'",
        EvaluatedTypeName::value(a),
        EvaluatedTypeName::value(b));

    out._error = error;
}

void BinaryOperatorTyping::typeArithmetic(BinaryOperator op, const BinaryOperand& lhs, const BinaryOperand& rhs, BinaryTyping& out) {
    const EvaluatedType a = lhs._type;
    const EvaluatedType b = rhs._type;
    const TypePairBitset pair(a, b);

    const bool isSubtraction = op == BinaryOperator::Sub;
    const bool isMultiplication = op == BinaryOperator::Mult;
    const bool isDivision = op == BinaryOperator::Div;

    if (isSubtraction) {
        const bool subtractsInstants = a == EvaluatedType::DateTime && b == EvaluatedType::DateTime;
        const bool subtractsADuration = a == EvaluatedType::DateTime && b == EvaluatedType::Duration;
        const bool subtractsDurations = a == EvaluatedType::Duration && b == EvaluatedType::Duration;

        if (subtractsInstants || subtractsDurations) {
            out._type = EvaluatedType::Duration;
            return;
        } else if (subtractsADuration) {
            out._type = EvaluatedType::DateTime;
            return;
        } else if (computesTimeOverListItem(pair)) {
            out._type = EvaluatedType::ListItem;
            return;
        }
    } else if (isMultiplication || isDivision) {
        const bool scalesByANumber = pair == TypePairBitset(EvaluatedType::Duration, EvaluatedType::Integer)
                                  || pair == TypePairBitset(EvaluatedType::Duration, EvaluatedType::Double);
        const bool scalesAListItem = pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::Duration);
        const bool scalesTheDuration = isMultiplication || a == EvaluatedType::Duration;

        if (scalesByANumber && scalesTheDuration) {
            out._type = EvaluatedType::Duration;
            return;
        } else if (scalesAListItem) {
            out._type = EvaluatedType::ListItem;
            return;
        }
    }

    if (pair == TypePairBitset(EvaluatedType::Integer, EvaluatedType::Integer)) {
        out._type = EvaluatedType::Integer;
        return;
    }

    if (pair == TypePairBitset(EvaluatedType::Double, EvaluatedType::Double)
        || pair == TypePairBitset(EvaluatedType::Double, EvaluatedType::Integer)) {
        out._type = EvaluatedType::Double;
        return;
    }

    if (computesOverListItem(pair)) {
        out._type = EvaluatedType::ListItem;
        return;
    }

    if (a == EvaluatedType::Null || b == EvaluatedType::Null) {
        out._type = EvaluatedType::Null;
        return;
    }

    const std::string error = fmt::format(
        "Operands are not valid and compatible numeric types: '{}' and '{}'",
        EvaluatedTypeName::value(a),
        EvaluatedTypeName::value(b));

    out._error = error;
}

void BinaryOperatorTyping::typePower(const BinaryOperand& lhs, const BinaryOperand& rhs, BinaryTyping& out) {
    const EvaluatedType a = lhs._type;
    const EvaluatedType b = rhs._type;
    const TypePairBitset pair(a, b);

    const bool bothInteger = pair == TypePairBitset(EvaluatedType::Integer, EvaluatedType::Integer);
    const bool bothDouble = pair == TypePairBitset(EvaluatedType::Double, EvaluatedType::Double);
    const bool mixedNumeric = pair == TypePairBitset(EvaluatedType::Double, EvaluatedType::Integer);

    // As per OpenCypher spec
    if (bothInteger || bothDouble || mixedNumeric) {
        out._type = EvaluatedType::Double;
        return;
    }

    if (computesOverListItem(pair)) {
        out._type = EvaluatedType::Double;
        return;
    }

    if (a == EvaluatedType::Null || b == EvaluatedType::Null) {
        out._type = EvaluatedType::Null;
        return;
    }

    const std::string error = fmt::format(
        "Operands are not valid and compatible numeric types: '{}' and '{}'",
        EvaluatedTypeName::value(a),
        EvaluatedTypeName::value(b));

    out._error = error;
}

void BinaryOperatorTyping::typeMembership(const BinaryOperand& lhs, const BinaryOperand& rhs, BinaryTyping& out) {
    const EvaluatedType b = rhs._type;

    out._type = EvaluatedType::Bool;

    // A type-erased cell holds whatever its row put there, so the test reads the
    // list out of its tag and answers null for a row holding something else
    const bool searchesACell = b == EvaluatedType::ListItem;
    const bool searchesNull = b == EvaluatedType::Null;

    if (b != EvaluatedType::List && b != EvaluatedType::Map && !searchesACell && !searchesNull) {
        const std::string error = fmt::format("IN operand must be a list or map, not '{}'",
                                              EvaluatedTypeName::value(b));
        out._error = error;
    }
}
