#include "BinaryOperatorTyping.h"

#include <stdint.h>

#include <string>
#include <string_view>

#include <spdlog/fmt/fmt.h>

#include "ParameterProbe.h"

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
// a tagged cell and a null included - appending a null leaves a list with a null in it.
bool joinsAList(EvaluatedType type) {
    return namesAListElementType(type)
        || type == EvaluatedType::Char
        || type == EvaluatedType::Null
        || type == EvaluatedType::Any;
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
// unreliable, and a label, an edge type or a property type is the id of one, equal only to
// an id of its kind.
RuleResult equalityType(EvaluatedType a, EvaluatedType b) {
    const bool equatesTwoDoubles = a == EvaluatedType::Double && b == EvaluatedType::Double;
    const bool equatesMetadataIDs = a == b
                                 && (a == EvaluatedType::Label
                                     || a == EvaluatedType::EdgeType
                                     || a == EvaluatedType::PropertyType);
    const bool takesA = a == EvaluatedType::Null || namesAnEqualityOperand(a);
    const bool takesB = b == EvaluatedType::Null || namesAnEqualityOperand(b);

    if (equatesMetadataIDs) {
        return valid(EvaluatedType::Bool);
    } else if (equatesTwoDoubles) {
        return invalid(TypingReason::DoubleEquality);
    } else if (!takesA || !takesB) {
        return invalid(TypingReason::IncompatibleTypes);
    } else if (a == EvaluatedType::Null || b == EvaluatedType::Null) {
        return valid(EvaluatedType::Null);
    }

    return valid(EvaluatedType::Bool);
}

RuleResult nullTestType(EvaluatedType b) {
    if (b != EvaluatedType::Null) {
        return invalid(TypingReason::NotANullTest);
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

    // A cell meets a string, a boolean or a null row by row: holding text or a number it
    // joins it as text, holding a list it takes it as an element
    const bool addsToAny = pair == TypePairBitset(EvaluatedType::Any, EvaluatedType::String)
                        || pair == TypePairBitset(EvaluatedType::Any, EvaluatedType::Char)
                        || pair == TypePairBitset(EvaluatedType::Any, EvaluatedType::Bool)
                        || pair == TypePairBitset(EvaluatedType::Any, EvaluatedType::Null);

    if (pair == TypePairBitset(EvaluatedType::String, EvaluatedType::String)) {
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
    const bool dividesAnAny = TypePairBitset(a, b) == TypePairBitset(EvaluatedType::Any, EvaluatedType::Duration);

    if (dividesADuration) {
        return valid(EvaluatedType::Duration);
    } else if (dividesAnAny) {
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

RuleResult membershipType(EvaluatedType b) {
    // A type-erased cell holds whatever its row put there, so the test reads the
    // list out of its tag and answers null for a row holding something else
    const bool searchesACollection = b == EvaluatedType::List || b == EvaluatedType::Map;
    const bool searchesACell = b == EvaluatedType::Any;
    const bool searchesNull = b == EvaluatedType::Null;

    if (!searchesACollection && !searchesACell && !searchesNull) {
        return invalid(TypingReason::NotACollection);
    } else if (searchesNull) {
        return valid(EvaluatedType::Null);
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

void probeType(BinaryOperator op, const ExprType& lhs, const ExprType& rhs, BinaryTyping& out) {
    const auto rule = [op](EvaluatedType left, EvaluatedType right) {
        return ruleType(op, left, right)._type;
    };

    ParameterProbe probe;
    ParameterProbe::probe(lhs, rhs, rule, probe);

    out._leftAccepted = probe._leftAccepted;
    out._rightAccepted = probe._rightAccepted;

    // A boolean is three-valued, so null is one of its values rather than a type of its own
    if (probe._results == EvaluatedTypeSet {EvaluatedType::Bool, EvaluatedType::Null}) {
        probe._results = EvaluatedTypeSet {EvaluatedType::Bool};
    }

    probe.writeResult(lhs, rhs, out._result);
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
        case TypingReason::InvalidOperator:
            out._error = "Invalid operand in binary expression.";
        break;
    }
}

// A parameter's list has no shape until it is bound, so a list it takes part in mixes
// whatever the bind brings
void writeListShape(const ExprType& lhs, const ExprType& rhs, BinaryTyping& out) {
    const EvaluatedType a = lhs._type;
    const EvaluatedType b = rhs._type;

    const bool readsAParameter = a == EvaluatedType::Parameter || b == EvaluatedType::Parameter;
    const bool leftIsAList = a == EvaluatedType::List;
    const bool bothLists = leftIsAList && b == EvaluatedType::List;

    if (readsAParameter) {
        out._result._shape = ListShape(EvaluatedType::Invalid, 1);
    } else if (bothLists) {
        out._result._shape = concatenatedListShape(lhs._shape, rhs._shape);
    } else {
        const ListShape& listShape = leftIsAList ? lhs._shape : rhs._shape;
        const EvaluatedType scalar = leftIsAList ? b : a;

        out._result._shape = appendedListShape(listShape, scalar);
    }
}

}

void BinaryOperatorTyping::type(BinaryOperator op, const ExprType& lhs, const ExprType& rhs, BinaryTyping& out) {
    const EvaluatedType a = lhs._type;
    const EvaluatedType b = rhs._type;
    probeType(op, lhs, rhs, out);

    if (out._result._type == EvaluatedType::Invalid) {
        writeError(ruleType(op, a, b)._reason, a, b, out);
        return;
    }

    if (out._result._type == EvaluatedType::List) {
        writeListShape(lhs, rhs, out);
    }
}
