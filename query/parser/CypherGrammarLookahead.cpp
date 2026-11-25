#include "CypherGrammar.h"

#include "BioAssert.h"

using namespace db;

namespace {

bool isComparisonSign(CypherTokenKind kind) {
    return kind == CypherTokenKind::ASSIGN
           || kind == CypherTokenKind::LE
           || kind == CypherTokenKind::GE
           || kind == CypherTokenKind::GT
           || kind == CypherTokenKind::LT
           || kind == CypherTokenKind::NOT_EQUAL;
}

}

bool CypherGrammar::opensConstruct(size_t offset, bool aliasMayFollow) {
    const CypherTokenKind keyword = peek(offset)._kind;
    const CypherTokenKind next = peek(offset + 1)._kind;

    switch (keyword) {
        case CypherTokenKind::COLLECT:
        case CypherTokenKind::EXISTS:
        case CypherTokenKind::COUNT: {
            const bool opensSubquery = next == CypherTokenKind::OBRACE && !braceOpensMapProjection(offset + 1);

            return next == CypherTokenKind::OPAREN || opensSubquery;
        }
        break;
        case CypherTokenKind::SINGLE:
        case CypherTokenKind::REDUCE:
        case CypherTokenKind::ALLREDUCE:
        case CypherTokenKind::NORMALIZE:
        case CypherTokenKind::NONE:
        case CypherTokenKind::ANY:
        case CypherTokenKind::ALL:
            return next == CypherTokenKind::OPAREN;
        break;
        case CypherTokenKind::CASE:
            return startsOperandOfKeyword(next, aliasMayFollow);
        break;
        default:
            return false;
        break;
    }
}

// A subquery body opens with a clause, a pattern or a path name, never with what opens a
// map projection body: }, a dot, or a name before :, a comma or }
bool CypherGrammar::braceOpensMapProjection(size_t offset) {
    const CypherTokenKind first = peek(offset + 1)._kind;

    if (first == CypherTokenKind::CBRACE || first == CypherTokenKind::DOT) {
        return true;
    } else if (!CypherToken::isName(first)) {
        return false;
    }

    const CypherTokenKind second = peek(offset + 2)._kind;

    return second == CypherTokenKind::COLON || second == CypherTokenKind::COMMA || second == CypherTokenKind::CBRACE;
}

bool CypherGrammar::notOpensNegation(size_t offset, bool aliasMayFollow) {
    return startsOperandOfKeyword(peek(offset + 1)._kind, aliasMayFollow);
}

bool CypherGrammar::distinctOpensProjection() {
    const CypherTokenKind next = peek(1)._kind;
    const bool startsItems = startsExpr(next) || next == CypherTokenKind::MULT;

    return startsItems && !isInfixWord(next) && next != CypherTokenKind::AS;
}

bool CypherGrammar::distinctOpensArgument(bool closeKeepsKeyword) {
    const CypherTokenKind next = peek(1)._kind;
    if (closeKeepsKeyword && next == CypherTokenKind::CPAREN) {
        return true;
    }

    return startsExpr(next) && !isInfixWord(next);
}

bool CypherGrammar::whenOpensBranch() {
    const CypherTokenKind next = peek(1)._kind;

    return startsExpr(next) || isComparisonSign(next) || next == CypherTokenKind::IS_VALUE_TEST;
}

bool CypherGrammar::whereOpensPredicate(size_t offset) {
    const CypherTokenKind next = peek(offset + 1)._kind;
    const bool continuesAName = next == CypherTokenKind::OPAREN
                                || next == CypherTokenKind::OBRACK
                                || next == CypherTokenKind::OBRACE
                                || next == CypherTokenKind::PLUS
                                || next == CypherTokenKind::SUB
                                || next == CypherTokenKind::WHERE
                                || isInfixWord(next);

    return startsExpr(next) && !continuesAName;
}

bool CypherGrammar::whereOpensDetailPredicate(size_t offset) {
    const CypherTokenKind next = peek(offset + 1)._kind;
    const bool continuesAName = next == CypherTokenKind::OBRACE
                                || next == CypherTokenKind::WHERE
                                || next == CypherTokenKind::IS;

    return startsExpr(next) && !continuesAName;
}

bool CypherGrammar::isOpensPredicateLabels(size_t offset) {
    const CypherTokenKind next = peek(offset + 1)._kind;

    return startsLabelAtom(next) && !isInfixWord(next);
}

bool CypherGrammar::isOpensDetailLabels(size_t offset) {
    const CypherTokenKind next = peek(offset + 1)._kind;

    return startsLabelAtom(next) && next != CypherTokenKind::IS;
}

bool CypherGrammar::pipeDisjoinsLabels() {
    if (!isFollowedBy(ExprFollower::PIPE)) {
        return true;
    } else if (peek(0)._kind != CypherTokenKind::PIPE) {
        return false;
    }

    const auto reading = _pipeReadings.find(_consumedCount);
    if (reading != _pipeReadings.end()) {
        return reading->second == PipeReading::DISJUNCTION;
    }

    bioassert(!_speculations.empty(), "A bar may follow a label outside parseChoosingBar");
    _speculations.back()._tentativePipes.push_back(_consumedCount);

    return true;
}

CypherGrammar::ParenthesizedAtom CypherGrammar::classifyParenthesis() {
    const CypherTokenKind first = peek(1)._kind;

    if (first == CypherTokenKind::CPAREN || first == CypherTokenKind::COLON) {
        return ParenthesizedAtom::PREDICATE_ROOT;
    } else if (first == CypherTokenKind::WHERE && whereOpensPredicate(1)) {
        return ParenthesizedAtom::PREDICATE_ROOT;
    } else if (first == CypherTokenKind::IS && isOpensPredicateLabels(1)) {
        return ParenthesizedAtom::PREDICATE_ROOT;
    } else if (first == CypherTokenKind::OBRACE) {
        const size_t afterProperties = skipBalanced(1);
        const bool filters = afterProperties > 0 && peek(afterProperties)._kind == CypherTokenKind::WHERE;

        return filters ? ParenthesizedAtom::PREDICATE_ROOT : ParenthesizedAtom::EXPRESSION;
    }

    const bool opensAnExpression = opensConstruct(1, false)
                                   || (first == CypherTokenKind::NOT && notOpensNegation(1, false));

    if (CypherToken::isSymbol(first) && !opensAnExpression) {
        const CypherTokenKind second = peek(2)._kind;

        if (second == CypherTokenKind::WHERE) {
            return ParenthesizedAtom::PREDICATE_ROOT;
        } else if (second == CypherTokenKind::OBRACE) {
            const size_t afterBody = skipBalanced(2);
            const bool filters = afterBody > 0 && peek(afterBody)._kind == CypherTokenKind::WHERE;

            return filters ? ParenthesizedAtom::PREDICATE_ROOT : ParenthesizedAtom::EXPRESSION;
        } else if (second == CypherTokenKind::COLON || second == CypherTokenKind::IS) {
            const size_t afterLabels = skipLabelExpression(2);
            const CypherTokenKind following = afterLabels > 0 ? peek(afterLabels)._kind : CypherTokenKind::PROG_END;
            const bool rootFollows = following == CypherTokenKind::WHERE || following == CypherTokenKind::OBRACE;

            return rootFollows ? ParenthesizedAtom::PREDICATE_ROOT : ParenthesizedAtom::EXPRESSION;
        }

        return ParenthesizedAtom::EXPRESSION;
    }

    const size_t numberOffset = first == CypherTokenKind::SUB ? 2 : 1;
    const CypherTokenKind number = peek(numberOffset)._kind;
    const bool startsItem = number == CypherTokenKind::DIGIT || number == CypherTokenKind::DOUBLE;

    if (startsItem && peek(numberOffset + 1)._kind == CypherTokenKind::COMMA) {
        return ParenthesizedAtom::EMBEDDING;
    }

    return ParenthesizedAtom::EXPRESSION;
}

size_t CypherGrammar::skipLabelExpression(size_t offset) {
    size_t position = skipLabelConjunction(offset + 1);

    while (position != 0 && peek(position)._kind == CypherTokenKind::PIPE) {
        position++;
        if (peek(position)._kind == CypherTokenKind::COLON) {
            position++;
        }

        position = skipLabelConjunction(position);
    }

    return position;
}

size_t CypherGrammar::skipLabelConjunction(size_t offset) {
    size_t position = skipLabelAtom(offset);

    while (position != 0) {
        const CypherTokenKind joint = peek(position)._kind;
        if (joint != CypherTokenKind::AMPERSAND && joint != CypherTokenKind::COLON) {
            return position;
        }

        position = skipLabelAtom(position + 1);
    }

    return 0;
}

size_t CypherGrammar::skipLabelAtom(size_t offset) {
    size_t position = offset;
    while (peek(position)._kind == CypherTokenKind::EXCLAMATION) {
        position++;
    }

    const CypherTokenKind atom = peek(position)._kind;
    if (CypherToken::isName(atom)) {
        return position + 1;
    } else if (atom == CypherTokenKind::DOLLAR) {
        const CypherTokenKind parameter = peek(position + 1)._kind;
        const bool namesAParameter = CypherToken::isName(parameter) || parameter == CypherTokenKind::DIGIT;

        return namesAParameter ? position + 2 : 0;
    } else if (atom == CypherTokenKind::OPAREN) {
        return skipBalanced(position);
    }

    return 0;
}

bool CypherGrammar::startsExpr(CypherTokenKind kind) {
    return CypherToken::isName(kind)
           || kind == CypherTokenKind::OPAREN
           || kind == CypherTokenKind::OBRACK
           || kind == CypherTokenKind::OBRACE
           || kind == CypherTokenKind::DOLLAR
           || kind == CypherTokenKind::PLUS
           || kind == CypherTokenKind::SUB
           || kind == CypherTokenKind::STRING_LITERAL
           || kind == CypherTokenKind::DIGIT
           || kind == CypherTokenKind::DOUBLE;
}

bool CypherGrammar::startsLabelAtom(CypherTokenKind kind) {
    return CypherToken::isName(kind)
           || kind == CypherTokenKind::OPAREN
           || kind == CypherTokenKind::DOLLAR
           || kind == CypherTokenKind::EXCLAMATION;
}

bool CypherGrammar::isInfixWord(CypherTokenKind kind) {
    return kind == CypherTokenKind::AND
           || kind == CypherTokenKind::OR
           || kind == CypherTokenKind::XOR
           || kind == CypherTokenKind::IN
           || kind == CypherTokenKind::IS
           || kind == CypherTokenKind::STARTS
           || kind == CypherTokenKind::ENDS
           || kind == CypherTokenKind::CONTAINS;
}

bool CypherGrammar::startsOperandOfKeyword(CypherTokenKind next, bool aliasMayFollow) {
    const bool namesAnAlias = aliasMayFollow && next == CypherTokenKind::AS;

    return startsExpr(next) && !isInfixWord(next) && !namesAnAlias;
}
