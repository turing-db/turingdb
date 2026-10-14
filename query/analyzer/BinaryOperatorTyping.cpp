#include "BinaryOperatorTyping.h"

#include <stdint.h>

#include <string>
#include <string_view>

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


enum class TypingReason : uint8_t {
    Valid,
    DoubleEquality,
    IncompatibleTypes,
    NotANullTest,
    IncompatibleOrderingTypes,
    IncompatibleAdditionTypes,
    IncompatibleConcatenationTypes,
    IncompatibleNumericTypes,
    NotACollection,
    InvalidOperator,
};

struct RuleResult {
    EvaluatedType _type {EvaluatedType::Invalid};
    TypingReason _reason {TypingReason::Valid};
};

RuleResult valid(EvaluatedType type) {
    return RuleResult {type, TypingReason::Valid};
}

RuleResult invalid(TypingReason reason) {
    return RuleResult {EvaluatedType::Invalid, reason};
}

EvaluatedType numericType(EvaluatedType a, EvaluatedType b) {
    const bool bothIntegers = a == EvaluatedType::Integer && b == EvaluatedType::Integer;
    const bool bothNumbers = isNumber(a) && isNumber(b);
    const bool eitherNull = a == EvaluatedType::Null || b == EvaluatedType::Null;

    if (bothIntegers) {
        return EvaluatedType::Integer;
    } else if (bothNumbers) {
        return EvaluatedType::Double;
    } else if (computesOverListItem(TypePairBitset(a, b))) {
        //TODO: Deferred type needed here
        return EvaluatedType::ListItem;
    } else if (eitherNull) {
        return EvaluatedType::Null;
    }

    return EvaluatedType::Invalid;
}

RuleResult numericRule(EvaluatedType a, EvaluatedType b) {
    const EvaluatedType numeric = numericType(a, b);

    if (numeric == EvaluatedType::Invalid) {
        return invalid(TypingReason::IncompatibleNumericTypes);
    }

    return valid(numeric);
}

RuleResult equalityType(EvaluatedType a, EvaluatedType b) {
    const bool bothDoubles = a == EvaluatedType::Double && b == EvaluatedType::Double;
    const bool sameSchemaName = isSchemaName(a) && a == b;
    const bool bothOperands = (isEqualityOperand(a) && isEqualityOperand(b)) || sameSchemaName;

    if (bothDoubles) {
        return invalid(TypingReason::DoubleEquality);
    } else if (!bothOperands || !cellComparesAgainst(a, b)) {
        return invalid(TypingReason::IncompatibleTypes);
    }

    return valid(EvaluatedType::Bool);
}

RuleResult nullTestType(EvaluatedType b) {
    if (b != EvaluatedType::Null) {
        return invalid(TypingReason::NotANullTest);
    }

    return valid(EvaluatedType::Bool);
}

RuleResult orderingType(EvaluatedType a, EvaluatedType b) {
    const TypePairBitset pair(a, b);

    const bool ordersValues = pair == TypePairBitset(EvaluatedType::Integer, EvaluatedType::Integer)
                           || pair == TypePairBitset(EvaluatedType::Double, EvaluatedType::Double)
                           || pair == TypePairBitset(EvaluatedType::Integer, EvaluatedType::Double)
                           || pair == TypePairBitset(EvaluatedType::String, EvaluatedType::String)
                           || pair == TypePairBitset(EvaluatedType::Bool, EvaluatedType::Bool)
                           || pair == TypePairBitset(EvaluatedType::DateTime, EvaluatedType::DateTime)
                           || pair == TypePairBitset(EvaluatedType::Duration, EvaluatedType::Duration)
                           || pair == TypePairBitset(EvaluatedType::List, EvaluatedType::List);

    // Ordering a value against a null is null, the same as comparing it against one
    const bool ordersAgainstNull = a == EvaluatedType::Null || b == EvaluatedType::Null;

    // A type-erased cell is ordered as an element holding the other side would be,
    // so it orders against the scalar types it can hold
    const bool ordersListItem = pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::ListItem)
                             || pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::Integer)
                             || pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::Double)
                             || pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::String)
                             || pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::Char)
                             || pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::Bool)
                             || pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::DateTime)
                             || pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::Duration)
                             || pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::List);

    if (ordersValues || ordersAgainstNull || ordersListItem) {
        return valid(EvaluatedType::Bool);
    }

    return invalid(TypingReason::IncompatibleOrderingTypes);
}

RuleResult additionType(EvaluatedType a, EvaluatedType b) {
    const TypePairBitset pair(a, b);
    const EvaluatedType numeric = numericType(a, b);

    // A scalar joins a list as the one-element list it stands for, from either
    // side: [1, 2] + 3 and 1 + [2, 3] are both [1, 2, 3]
    const bool leftIsAList = a == EvaluatedType::List;
    const bool oneSideIsAList = leftIsAList || b == EvaluatedType::List;
    const EvaluatedType scalar = leftIsAList ? b : a;

    if (pair == TypePairBitset(EvaluatedType::String, EvaluatedType::String)) {
        return valid(EvaluatedType::String);
    } else if (oneSideIsAList && joinsAList(scalar)) {
        return valid(EvaluatedType::List);
    } else if (concatenatesListItem(pair) || concatenatesAsText(pair)) {
        // A cell holding text concatenates with a string, as the string it holds
        // would: the type it holds is settled row by row, so a cell holding a number
        // joins as the text Cypher writes that number with
        return valid(EvaluatedType::String);
    } else if (pair == TypePairBitset(EvaluatedType::DateTime, EvaluatedType::Duration)) {
        return valid(EvaluatedType::DateTime);
    } else if (pair == TypePairBitset(EvaluatedType::Duration, EvaluatedType::Duration)) {
        return valid(EvaluatedType::Duration);
    } else if (computesTimeOverListItem(pair)) {
        //TODO: This needs to be handled as a deferred type
        return valid(EvaluatedType::ListItem);
    } else if (numeric != EvaluatedType::Invalid) {
        return valid(numeric);
    }

    return invalid(TypingReason::IncompatibleAdditionTypes);
}

RuleResult concatenationType(EvaluatedType a, EvaluatedType b) {
    const TypePairBitset pair(a, b);

    if (pair == TypePairBitset(EvaluatedType::String, EvaluatedType::String)) {
        return valid(EvaluatedType::String);
    } else if (pair == TypePairBitset(EvaluatedType::List, EvaluatedType::List)) {
        return valid(EvaluatedType::List);
    } else if (concatenatesListItem(pair) || concatenatesTwoListItems(pair)) {
        return valid(EvaluatedType::String);
    } else if (a == EvaluatedType::Null || b == EvaluatedType::Null) {
        // '||' joins two strings or two lists, so an unknown value is no side of one
        // rather than an element to append: [1, 2] || null is null where [1, 2] + null
        // is [1, 2, null]
        return valid(EvaluatedType::Null);
    }

    return invalid(TypingReason::IncompatibleConcatenationTypes);
}

RuleResult subtractionType(EvaluatedType a, EvaluatedType b) {
    const bool subtractsInstants = a == EvaluatedType::DateTime && b == EvaluatedType::DateTime;
    const bool subtractsADuration = a == EvaluatedType::DateTime && b == EvaluatedType::Duration;
    const bool subtractsDurations = a == EvaluatedType::Duration && b == EvaluatedType::Duration;

    if (subtractsInstants || subtractsDurations) {
        return valid(EvaluatedType::Duration);
    } else if (subtractsADuration) {
        return valid(EvaluatedType::DateTime);
    } else if (computesTimeOverListItem(TypePairBitset(a, b))) {
        //TODO: deferred type needed here.
        return valid(EvaluatedType::ListItem);
    }

    return numericRule(a, b);
}

RuleResult multiplicationType(EvaluatedType a, EvaluatedType b) {
    const TypePairBitset pair(a, b);

    const bool scalesADuration = pair == TypePairBitset(EvaluatedType::Duration, EvaluatedType::Integer)
                              || pair == TypePairBitset(EvaluatedType::Duration, EvaluatedType::Double);
    const bool scalesAListItem = pair == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::Duration);

    if (scalesADuration) {
        return valid(EvaluatedType::Duration);
    } else if (scalesAListItem) {
        //TODO: deferred type needed here.
        return valid(EvaluatedType::ListItem);
    }

    return numericRule(a, b);
}

RuleResult divisionType(EvaluatedType a, EvaluatedType b) {
    const bool dividesADuration = a == EvaluatedType::Duration && isNumber(b);
    const bool dividesAListItem = TypePairBitset(a, b) == TypePairBitset(EvaluatedType::ListItem, EvaluatedType::Duration);

    if (dividesADuration) {
        return valid(EvaluatedType::Duration);
    } else if (dividesAListItem) {
        return valid(EvaluatedType::ListItem);
    }

    return numericRule(a, b);
}

RuleResult powerType(EvaluatedType a, EvaluatedType b) {
    const bool bothNumbers = isNumber(a) && isNumber(b);
    const bool eitherNull = a == EvaluatedType::Null || b == EvaluatedType::Null;

    // As per OpenCypher spec
    if (bothNumbers || computesOverListItem(TypePairBitset(a, b))) {
        return valid(EvaluatedType::Double);
    } else if (eitherNull) {
        return valid(EvaluatedType::Null);
    }

    return invalid(TypingReason::IncompatibleNumericTypes);
}

RuleResult membershipType(EvaluatedType b) {
    // A type-erased cell holds whatever its row put there, so the test reads the
    // list out of its tag and answers null for a row holding something else
    const bool searchesACollection = b == EvaluatedType::List || b == EvaluatedType::Map;
    const bool searchesACell = b == EvaluatedType::ListItem;
    const bool searchesNull = b == EvaluatedType::Null;

    if (!searchesACollection && !searchesACell && !searchesNull) {
        return invalid(TypingReason::NotACollection);
    }

    return valid(EvaluatedType::Bool);
}

RuleResult ruleType(BinaryOperator op, EvaluatedType a, EvaluatedType b) {
    switch (op) {
        case BinaryOperator::NotEqual:
        case BinaryOperator::Equal:
            return equalityType(a, b);
        break;
        case BinaryOperator::IsNull:
        case BinaryOperator::IsNotNull:
            return nullTestType(b);
        break;
        case BinaryOperator::LessThan:
        case BinaryOperator::GreaterThan:
        case BinaryOperator::LessThanOrEqual:
        case BinaryOperator::GreaterThanOrEqual:
            return orderingType(a, b);
        break;
        case BinaryOperator::Add:
            return additionType(a, b);
        break;
        case BinaryOperator::Concat:
            return concatenationType(a, b);
        break;
        case BinaryOperator::Sub:
            return subtractionType(a, b);
        break;
        case BinaryOperator::Mult:
            return multiplicationType(a, b);
        break;
        case BinaryOperator::Div:
            return divisionType(a, b);
        break;
        case BinaryOperator::Mod:
            return numericRule(a, b);
        break;
        case BinaryOperator::Pow:
            return powerType(a, b);
        break;
        case BinaryOperator::In:
            return membershipType(b);
        break;
        case BinaryOperator::_SIZE:
            return invalid(TypingReason::InvalidOperator);
        break;
    }

    return invalid(TypingReason::InvalidOperator);
}

void writeError(TypingReason reason, EvaluatedType a, EvaluatedType b, BinaryTyping& out) {
    const std::string_view left = EvaluatedTypeName::value(a);
    const std::string_view right = EvaluatedTypeName::value(b);

    switch (reason) {
        case TypingReason::Valid:
        break;
        case TypingReason::DoubleEquality:
            out._error = fmt::format("Equality of types '{}' and '{}' is not encouraged due to "
                                     "potential rounding innaccuracy. Please constrain with '<' "
                                     "and '>' instead.",
                                     left,
                                     right);
        break;
        case TypingReason::IncompatibleTypes:
            out._error = fmt::format("Operands are not valid or compatible types: '{}' and '{}'", left, right);
        break;
        case TypingReason::NotANullTest:
            out._error = fmt::format("IS tests whether its operand is null, so its right side "
                                     "must be NULL, not '{}'",
                                     right);
        break;
        case TypingReason::IncompatibleOrderingTypes:
            out._error = fmt::format("Operands are not valid or compatible numeric types: '{}' and '{}'", left, right);
        break;
        case TypingReason::IncompatibleAdditionTypes:
            out._error = fmt::format("Operands are not valid and compatible types for '+': '{}' and '{}'", left, right);
        break;
        case TypingReason::IncompatibleConcatenationTypes:
            out._error = fmt::format("Operands are not valid and compatible types for '||': '{}' and '{}'", left, right);
        break;
        case TypingReason::IncompatibleNumericTypes:
            out._error = fmt::format("Operands are not valid and compatible numeric types: '{}' and '{}'", left, right);
        break;
        case TypingReason::NotACollection:
            out._error = fmt::format("IN operand must be a list or map, not '{}'", right);
        break;
        case TypingReason::InvalidOperator:
            out._error = "Invalid operand in binary expression.";
        break;
    }
}

void writeListShape(const BinaryOperand& lhs, const BinaryOperand& rhs, BinaryTyping& out) {
    const EvaluatedType a = lhs._type;
    const EvaluatedType b = rhs._type;

    const bool leftIsAList = a == EvaluatedType::List;
    const bool bothLists = leftIsAList && b == EvaluatedType::List;

    if (bothLists) {
        out._shape = concatenatedListShape(lhs._shape, rhs._shape);
    } else {
        const ListShape& listShape = leftIsAList ? lhs._shape : rhs._shape;
        const EvaluatedType scalar = leftIsAList ? b : a;

        out._shape = appendedListShape(listShape, scalar);
    }
}

}

void BinaryOperatorTyping::type(BinaryOperator op, const BinaryOperand& lhs, const BinaryOperand& rhs, BinaryTyping& out) {
    const EvaluatedType a = lhs._type;
    const EvaluatedType b = rhs._type;
    const RuleResult rule = ruleType(op, a, b);

    if (rule._type == EvaluatedType::Invalid) {
        writeError(rule._reason, a, b, out);
        return;
    }

    out._type = rule._type;

    if (rule._type == EvaluatedType::List) {
        writeListShape(lhs, rhs, out);
    }
}
