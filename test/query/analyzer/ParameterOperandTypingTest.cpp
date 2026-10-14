#include <gtest/gtest.h>

#include <string>

#include "TuringTest.h"

#include "BinaryOperatorTyping.h"

using namespace db;

class ParameterOperandTypingTest : public turing::test::TuringTest {
protected:
    static ExprType operand(EvaluatedType type) {
        const EvaluatedTypeSet candidates = type == EvaluatedType::Parameter ? PARAMETER_VALUE_TYPES : EvaluatedTypeSet {};
        return ExprType {type, candidates, {}};
    }

    static void typePair(EvaluatedType a, BinaryOperator op, EvaluatedType b, BinaryTyping& out) {
        BinaryOperatorTyping::type(op, operand(a), operand(b), out);
    }

    static void expectType(EvaluatedType a, BinaryOperator op, EvaluatedType b, EvaluatedType expected) {
        BinaryTyping typing;
        typePair(a, op, b, typing);

        EXPECT_TRUE(typing.isValid()) << typing._error;
        EXPECT_EQ(typing._result._type, expected) << EvaluatedTypeName::value(typing._result._type);
    }
};

TEST_F(ParameterOperandTypingTest, aComparisonIsABooleanWhateverTheBind) {
    expectType(EvaluatedType::Integer, BinaryOperator::LessThan, EvaluatedType::Parameter, EvaluatedType::Bool);
    expectType(EvaluatedType::Parameter, BinaryOperator::Equal, EvaluatedType::Parameter, EvaluatedType::Bool);
    expectType(EvaluatedType::Parameter, BinaryOperator::IsNull, EvaluatedType::Null, EvaluatedType::Bool);
    expectType(EvaluatedType::Integer, BinaryOperator::In, EvaluatedType::Parameter, EvaluatedType::Bool);
}

TEST_F(ParameterOperandTypingTest, aComparisonAgainstNullIsNullWhateverTheBind) {
    expectType(EvaluatedType::Parameter, BinaryOperator::Equal, EvaluatedType::Null, EvaluatedType::Null);
}

TEST_F(ParameterOperandTypingTest, bindsThatAgreeGiveTheirType) {
    expectType(EvaluatedType::Parameter, BinaryOperator::Concat, EvaluatedType::String, EvaluatedType::String);
    expectType(EvaluatedType::Parameter, BinaryOperator::Pow, EvaluatedType::Integer, EvaluatedType::Double);
    expectType(EvaluatedType::Parameter, BinaryOperator::Sub, EvaluatedType::DateTime, EvaluatedType::Duration);
}

TEST_F(ParameterOperandTypingTest, bindsThatDisagreeLeaveTheTypeToTheBind) {
    expectType(EvaluatedType::Parameter, BinaryOperator::Add, EvaluatedType::Integer, EvaluatedType::Parameter);
    expectType(EvaluatedType::DateTime, BinaryOperator::Sub, EvaluatedType::Parameter, EvaluatedType::Parameter);
    expectType(EvaluatedType::Parameter, BinaryOperator::Mod, EvaluatedType::Integer, EvaluatedType::Parameter);
    expectType(EvaluatedType::Parameter, BinaryOperator::Add, EvaluatedType::Parameter, EvaluatedType::Parameter);
}

TEST_F(ParameterOperandTypingTest, aCellSettlesTheTypeOnTheRow) {
    expectType(EvaluatedType::Parameter, BinaryOperator::Sub, EvaluatedType::Any, EvaluatedType::Any);
}

TEST_F(ParameterOperandTypingTest, aListAParameterJoinsMixesItsElements) {
    BinaryTyping typing;
    typePair(EvaluatedType::Parameter, BinaryOperator::Add, EvaluatedType::Embedding, typing);

    ASSERT_TRUE(typing.isValid()) << typing._error;
    EXPECT_EQ(typing._result._type, EvaluatedType::List);
    EXPECT_EQ(typing._result._shape.getLeafType(), EvaluatedType::Invalid);
    EXPECT_EQ(typing._result._shape.getDepth(), size_t {1});
}

TEST_F(ParameterOperandTypingTest, aNullResultGivesWayToAnyOther) {
    expectType(EvaluatedType::Any, BinaryOperator::Pow, EvaluatedType::Integer, EvaluatedType::Double);
    expectType(EvaluatedType::Parameter, BinaryOperator::Sub, EvaluatedType::Null, EvaluatedType::Null);
}

TEST_F(ParameterOperandTypingTest, noValidBindIsRejectedWithTheRuleReason) {
    BinaryTyping typing;
    typePair(EvaluatedType::Parameter, BinaryOperator::Concat, EvaluatedType::Bool, typing);

    EXPECT_FALSE(typing.isValid());
    EXPECT_NE(typing._error.find("for '||': 'Parameter' and 'Bool'"), std::string::npos) << typing._error;
}

TEST_F(ParameterOperandTypingTest, bindsThatDisagreeKeepTheirResultTypes) {
    BinaryTyping typing;
    typePair(EvaluatedType::Parameter, BinaryOperator::Sub, EvaluatedType::Integer, typing);

    ASSERT_TRUE(typing.isValid()) << typing._error;
    EXPECT_EQ(typing._result._type, EvaluatedType::Parameter);
    EXPECT_EQ(typing._result._candidates, (EvaluatedTypeSet {EvaluatedType::Integer, EvaluatedType::Double}));
}

TEST_F(ParameterOperandTypingTest, anOperandOfAParameterIsProbedOverItsCandidates) {
    const ExprType time {EvaluatedType::Parameter, {EvaluatedType::DateTime, EvaluatedType::Duration}, {}};
    const ExprType number {EvaluatedType::Parameter, {EvaluatedType::Integer, EvaluatedType::String}, {}};

    BinaryTyping typing;
    BinaryOperatorTyping::type(BinaryOperator::Sub, time, operand(EvaluatedType::Duration), typing);

    ASSERT_TRUE(typing.isValid()) << typing._error;
    EXPECT_EQ(typing._result._type, EvaluatedType::Parameter);
    EXPECT_EQ(typing._result._candidates, (EvaluatedTypeSet {EvaluatedType::DateTime, EvaluatedType::Duration}));

    BinaryTyping narrowed;
    BinaryOperatorTyping::type(BinaryOperator::Sub, number, operand(EvaluatedType::Integer), narrowed);

    ASSERT_TRUE(narrowed.isValid()) << narrowed._error;
    EXPECT_EQ(narrowed._result._type, EvaluatedType::Integer);
    EXPECT_TRUE(narrowed._result._candidates.isEmpty());
}

TEST_F(ParameterOperandTypingTest, aComparisonKeepsNoCandidates) {
    BinaryTyping typing;
    typePair(EvaluatedType::Parameter, BinaryOperator::LessThan, EvaluatedType::Integer, typing);

    EXPECT_EQ(typing._result._type, EvaluatedType::Bool);
    EXPECT_TRUE(typing._result._candidates.isEmpty());
}

TEST_F(ParameterOperandTypingTest, aParameterOperandIsNarrowedToTheBindsThatType) {
    BinaryTyping typing;
    typePair(EvaluatedType::Parameter, BinaryOperator::Sub, EvaluatedType::Integer, typing);

    ASSERT_TRUE(typing.isValid()) << typing._error;
    EXPECT_EQ(typing._leftAccepted, (EvaluatedTypeSet {EvaluatedType::Integer, EvaluatedType::Double}));
    EXPECT_EQ(typing._rightAccepted, EvaluatedTypeSet {EvaluatedType::Integer});
}

TEST_F(ParameterOperandTypingTest, aNullResultIsOneOfTheBindsOutcomes) {
    BinaryTyping typing;
    typePair(EvaluatedType::Parameter, BinaryOperator::Add, EvaluatedType::Null, typing);

    ASSERT_TRUE(typing.isValid()) << typing._error;
    EXPECT_EQ(typing._result._type, EvaluatedType::Parameter);
    EXPECT_EQ(typing._result._candidates, (EvaluatedTypeSet {EvaluatedType::List, EvaluatedType::Null}));
    EXPECT_EQ(typing._leftAccepted, PARAMETER_VALUE_TYPES);
}
