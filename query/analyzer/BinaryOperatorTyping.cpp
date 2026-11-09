#include "BinaryOperatorTyping.h"

#include <stdint.h>

#include <array>
#include <optional>
#include <string>
#include <string_view>

#include <spdlog/fmt/fmt.h>

using namespace db;

namespace {

// A number or a boolean joins a string as the text Cypher writes it with: 1 + ' apples' is
// '1 apples' and true + 'x' is 'truex'.
bool concatenatesAsText(TypePairBitset pair) {
    return pair == TypePairBitset(EvaluatedType::Integer, EvaluatedType::String)
        || pair == TypePairBitset(EvaluatedType::Double, EvaluatedType::String)
        || pair == TypePairBitset(EvaluatedType::Bool, EvaluatedType::String);
}

// Whether an arithmetic operator's operands are a type-erased cell and a number, or two
// cells. A cell is numeric only once read, and its tag names a type per row rather than
// one for the column, so the answer is the double a reduction over cells lands on too.
bool computesOverAny(TypePairBitset pair) {
    return pair == TypePairBitset(EvaluatedType::Any, EvaluatedType::Any)
        || pair == TypePairBitset(EvaluatedType::Any, EvaluatedType::Integer)
        || pair == TypePairBitset(EvaluatedType::Any, EvaluatedType::Double);
}

bool computesTimeOverAny(TypePairBitset pair) {
    return pair == TypePairBitset(EvaluatedType::Any, EvaluatedType::DateTime)
        || pair == TypePairBitset(EvaluatedType::Any, EvaluatedType::Duration);
}

// The types a scalar operand of '+' joins a list as: whatever an element of a list can be,
// a tagged cell included
bool joinsAList(EvaluatedType type) {
    return namesAListElementType(type)
        || type == EvaluatedType::Char
        || type == EvaluatedType::Any;
}

// A scalar joins a list as the one-element list it stands for
ListShape appendedListShape(const ListShape& list, EvaluatedType scalar) {
    return ListShape::joined(list, ListShape::singleton(scalar, ListShape()));
}

enum class TypingReason : uint8_t {
    Valid,
    DoubleEquality,
    IncompatibleTypes,
    IncompatibleOrderingTypes,
    IncompatibleAdditionTypes,
    IncompatibleConcatenationTypes,
    IncompatibleNumericTypes,
    NotACollection,
    IncompatibleMemberTypes,
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
    } else if (computesOverAny(TypePairBitset(a, b))) {
        //TODO: Deferred type needed here
        return EvaluatedType::Any;
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

// What = and <> answer over two types: null against a null, a boolean between two operands
// an equality takes. Two doubles are not tested for equality, which rounding makes
// unreliable, and a schema ID is equal only to one of its kind.
RuleResult equalityType(EvaluatedType a, EvaluatedType b) {
    const bool equatesTwoDoubles = a == EvaluatedType::Double && b == EvaluatedType::Double;
    const bool equatesASchemaIDAcrossKinds = (namesASchemaID(a) || namesASchemaID(b))
                                             && a != b
                                             && a != EvaluatedType::Null
                                             && b != EvaluatedType::Null;
    const bool takesA = a == EvaluatedType::Null || namesAnEqualityOperand(a);
    const bool takesB = b == EvaluatedType::Null || namesAnEqualityOperand(b);

    if (equatesTwoDoubles) {
        return invalid(TypingReason::DoubleEquality);
    } else if (!takesA || !takesB || equatesASchemaIDAcrossKinds) {
        return invalid(TypingReason::IncompatibleTypes);
    } else if (a == EvaluatedType::Null || b == EvaluatedType::Null) {
        return valid(EvaluatedType::Null);
    }

    return valid(EvaluatedType::Bool);
}

// What <, <=, > and >= answer over two types: null against a null or across order classes,
// a boolean within one
RuleResult orderingType(EvaluatedType a, EvaluatedType b) {
    if (a == EvaluatedType::Null || b == EvaluatedType::Null) {
        return valid(EvaluatedType::Null);
    } else if (!namesAnOrderingOperand(a) || !namesAnOrderingOperand(b)) {
        return invalid(TypingReason::IncompatibleOrderingTypes);
    } else if (ordersAsDisjointTypes(a, b)) {
        return valid(EvaluatedType::Null);
    }

    return valid(EvaluatedType::Bool);
}

RuleResult additionType(EvaluatedType a, EvaluatedType b) {
    const TypePairBitset pair(a, b);
    const EvaluatedType numeric = numericType(a, b);

    // A scalar joins a list as the one-element list it stands for, from either
    // side: [1, 2] + 3 and 1 + [2, 3] are both [1, 2, 3]
    const bool leftIsAList = a == EvaluatedType::List;
    const bool oneSideIsAList = leftIsAList || b == EvaluatedType::List;
    const EvaluatedType scalar = leftIsAList ? b : a;

    // A cell meets a string or a boolean row by row: holding text or a number it joins it
    // as text, holding a list it takes it as an element
    const bool addsToAny = pair == TypePairBitset(EvaluatedType::Any, EvaluatedType::String)
                        || pair == TypePairBitset(EvaluatedType::Any, EvaluatedType::Char)
                        || pair == TypePairBitset(EvaluatedType::Any, EvaluatedType::Bool);

    const bool eitherNull = a == EvaluatedType::Null || b == EvaluatedType::Null;

    if (eitherNull) {
        return valid(EvaluatedType::Null);
    } else if (pair == TypePairBitset(EvaluatedType::String, EvaluatedType::String)) {
        return valid(EvaluatedType::String);
    } else if (oneSideIsAList && joinsAList(scalar)) {
        return valid(EvaluatedType::List);
    } else if (concatenatesAsText(pair)) {
        return valid(EvaluatedType::String);
    } else if (addsToAny) {
        return valid(EvaluatedType::Any);
    } else if (pair == TypePairBitset(EvaluatedType::DateTime, EvaluatedType::Duration)) {
        return valid(EvaluatedType::DateTime);
    } else if (pair == TypePairBitset(EvaluatedType::Duration, EvaluatedType::Duration)) {
        return valid(EvaluatedType::Duration);
    } else if (computesTimeOverAny(pair)) {
        //TODO: This needs to be handled as a deferred type
        return valid(EvaluatedType::Any);
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
    } else if (pair == TypePairBitset(EvaluatedType::Any, EvaluatedType::String)) {
        // A type-erased cell joins a string where its row holds a string, a list where
        // it holds a list, and is a type error where it holds anything else
        return valid(EvaluatedType::String);
    } else if (pair == TypePairBitset(EvaluatedType::Any, EvaluatedType::List)) {
        return valid(EvaluatedType::List);
    } else if (pair == TypePairBitset(EvaluatedType::Any, EvaluatedType::Any)) {
        return valid(EvaluatedType::Any);
    } else if (a == EvaluatedType::Null || b == EvaluatedType::Null) {
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
    } else if (computesTimeOverAny(TypePairBitset(a, b))) {
        //TODO: deferred type needed here.
        return valid(EvaluatedType::Any);
    }

    return numericRule(a, b);
}

RuleResult multiplicationType(EvaluatedType a, EvaluatedType b) {
    const TypePairBitset pair(a, b);

    const bool scalesADuration = pair == TypePairBitset(EvaluatedType::Duration, EvaluatedType::Integer)
                              || pair == TypePairBitset(EvaluatedType::Duration, EvaluatedType::Double);
    const bool scalesAnAny = pair == TypePairBitset(EvaluatedType::Any, EvaluatedType::Duration);

    if (scalesADuration) {
        return valid(EvaluatedType::Duration);
    } else if (scalesAnAny) {
        //TODO: deferred type needed here.
        return valid(EvaluatedType::Any);
    }

    return numericRule(a, b);
}

RuleResult divisionType(EvaluatedType a, EvaluatedType b) {
    const bool dividesADuration = a == EvaluatedType::Duration && isNumber(b);
    const bool dividesADurationByAnAny = a == EvaluatedType::Duration && b == EvaluatedType::Any;

    if (dividesADuration) {
        return valid(EvaluatedType::Duration);
    } else if (dividesADurationByAnAny) {
        return valid(EvaluatedType::Any);
    }

    return numericRule(a, b);
}

RuleResult powerType(EvaluatedType a, EvaluatedType b) {
    const bool bothNumbers = isNumber(a) && isNumber(b);
    const bool eitherNull = a == EvaluatedType::Null || b == EvaluatedType::Null;

    // As per OpenCypher spec
    if (bothNumbers || computesOverAny(TypePairBitset(a, b))) {
        return valid(EvaluatedType::Double);
    } else if (eitherNull) {
        return valid(EvaluatedType::Null);
    }

    return invalid(TypingReason::IncompatibleNumericTypes);
}

EvaluatedType memberType(const ExprType& collection) {
    return collection._shape.unwoundType();
}

// x IN c asks x = e of each element e, so it turns away the operand types equality does
RuleResult membershipType(EvaluatedType a, const ExprType& rhs) {
    const EvaluatedType b = rhs._type;

    // A type-erased cell holds whatever its row put there, so the test reads the list
    // it holds, or a list of one where it holds something else
    const bool searchesACollection = b == EvaluatedType::List || b == EvaluatedType::Map;
    const bool searchesACell = b == EvaluatedType::Any;
    const bool searchesNull = b == EvaluatedType::Null;
    const bool noElementCanEqual = equalityType(a, memberType(rhs))._reason == TypingReason::IncompatibleTypes;

    if (!searchesACollection && !searchesACell && !searchesNull) {
        return invalid(TypingReason::NotACollection);
    } else if (searchesNull) {
        return valid(EvaluatedType::Null);
    } else if (noElementCanEqual) {
        return invalid(TypingReason::IncompatibleMemberTypes);
    }

    return valid(EvaluatedType::Bool);
}

RuleResult operatorType(BinaryOperator op, const ExprType& lhs, const ExprType& rhs) {
    const EvaluatedType a = lhs._type;
    const EvaluatedType b = rhs._type;

    switch (op) {
        case BinaryOperator::NotEqual:
        case BinaryOperator::Equal:
            return equalityType(a, b);
        break;
        case BinaryOperator::IsNull:
        case BinaryOperator::IsNotNull:
            return valid(EvaluatedType::Bool);
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
            return membershipType(a, rhs);
        break;
        case BinaryOperator::_SIZE:
            return invalid(TypingReason::InvalidOperator);
        break;
    }

    return invalid(TypingReason::InvalidOperator);
}

std::optional<TypingReason> valueOperatorReason(BinaryOperator op) {
    switch (op) {
        case BinaryOperator::Add:
            return TypingReason::IncompatibleAdditionTypes;
        break;
        case BinaryOperator::Concat:
            return TypingReason::IncompatibleConcatenationTypes;
        break;
        case BinaryOperator::Sub:
        case BinaryOperator::Mult:
        case BinaryOperator::Div:
        case BinaryOperator::Mod:
        case BinaryOperator::Pow:
            return TypingReason::IncompatibleNumericTypes;
        break;
        default:
            return std::nullopt;
        break;
    }
}

// The types a value has, so the ones a null can stand for
constexpr std::array<EvaluatedType, 9> VALUE_TYPES {
    EvaluatedType::Integer,
    EvaluatedType::Double,
    EvaluatedType::String,
    EvaluatedType::Bool,
    EvaluatedType::Embedding,
    EvaluatedType::List,
    EvaluatedType::Map,
    EvaluatedType::DateTime,
    EvaluatedType::Duration,
};

bool takesAValueBeside(BinaryOperator op, const ExprType& known, bool knownIsLeft) {
    for (const EvaluatedType value : VALUE_TYPES) {
        const ExprType valueOperand {value};
        const ExprType& left = knownIsLeft ? known : valueOperand;
        const ExprType& right = knownIsLeft ? valueOperand : known;

        if (operatorType(op, left, right)._type != EvaluatedType::Invalid) {
            return true;
        }
    }

    return false;
}

// A null stands for a value of any type, so it answers null only beside an operand some
// value combines with: null / duration(1) is a type error, as 5 / duration(1) is
RuleResult ruleType(BinaryOperator op, const ExprType& lhs, const ExprType& rhs) {
    const RuleResult result = operatorType(op, lhs, rhs);
    const std::optional<TypingReason> valueReason = valueOperatorReason(op);

    const bool leftIsNull = lhs._type == EvaluatedType::Null;
    const bool pairsOneNull = leftIsNull != (rhs._type == EvaluatedType::Null);
    const bool checksTheNull = result._type != EvaluatedType::Invalid && pairsOneNull && valueReason.has_value();

    if (checksTheNull && !takesAValueBeside(op, leftIsNull ? rhs : lhs, !leftIsNull)) {
        return invalid(*valueReason);
    }

    return result;
}

void writeError(TypingReason reason, const ExprType& lhs, const ExprType& rhs, BinaryTyping& out) {
    const std::string_view left = EvaluatedTypeName::value(lhs._type);
    const std::string_view right = EvaluatedTypeName::value(rhs._type);

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
        case TypingReason::IncompatibleOrderingTypes:
            out._error = fmt::format("Operands are not valid or compatible types for an ordering: '{}' and '{}'", left, right);
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
        case TypingReason::IncompatibleMemberTypes:
            out._error = fmt::format("IN cannot compare '{}' with an element of type '{}'",
                                     left,
                                     EvaluatedTypeName::value(memberType(rhs)));
        break;
        case TypingReason::InvalidOperator:
            out._error = "Invalid operand in binary expression.";
        break;
    }
}

void writeListShape(const ExprType& lhs, const ExprType& rhs, BinaryTyping& out) {
    const EvaluatedType a = lhs._type;
    const EvaluatedType b = rhs._type;

    const bool leftIsAList = a == EvaluatedType::List;
    const bool bothLists = leftIsAList && b == EvaluatedType::List;

    if (bothLists) {
        out._result._shape = ListShape::joined(lhs._shape, rhs._shape);
    } else {
        const ListShape& listShape = leftIsAList ? lhs._shape : rhs._shape;
        const EvaluatedType scalar = leftIsAList ? b : a;

        out._result._shape = appendedListShape(listShape, scalar);
    }
}

}

void BinaryOperatorTyping::type(BinaryOperator op, const ExprType& lhs, const ExprType& rhs, BinaryTyping& out) {
    const RuleResult rule = ruleType(op, lhs, rhs);

    if (rule._type == EvaluatedType::Invalid) {
        writeError(rule._reason, lhs, rhs, out);
        return;
    }

    out._result._type = rule._type;

    if (rule._type == EvaluatedType::List) {
        writeListShape(lhs, rhs, out);
    }
}
