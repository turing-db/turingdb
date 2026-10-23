#include <gtest/gtest.h>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace turing::test;

class CaseEqualityRuleTest : public WriteQueryTest {
};

TEST_F(CaseEqualityRuleTest, comparesAnIntegerSubjectToANode) {
    expectRows("MATCH (n:Person {name: 'Remy'}) UNWIND [0, 1] AS x "
               "RETURN x, CASE x WHEN n THEN 'y' ELSE 'n' END",
               {{"0", "y"}, {"1", "n"}});
}

TEST_F(CaseEqualityRuleTest, comparesAnIntegerSubjectToAnEdge) {
    expectRows("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->() UNWIND [0, 1] AS x "
               "RETURN x, CASE x WHEN e THEN 'y' ELSE 'n' END, x = e",
               {{"0", "y", "true"}, {"1", "n", "false"}});
}

TEST_F(CaseEqualityRuleTest, aNodeIsNotEqualToAString) {
    expectRows("MATCH (n:Person) WHERE n.name IN ['Remy', 'Adam'] "
               "RETURN n.name, CASE n WHEN <> 'a' THEN 1 ELSE 0 END, CASE n WHEN 'a' THEN 1 ELSE 0 END, n = 'a', n <> 'a'",
               {{"Adam", "1", "0", "false", "true"}, {"Remy", "1", "0", "false", "true"}});
}

TEST_F(CaseEqualityRuleTest, aNodeIsNotEqualToAnEdge) {
    expectRows("MATCH (n:Person {name: 'Remy'})-[e:KNOWS_WELL]->() "
               "RETURN n = e, CASE n WHEN e THEN 'y' ELSE 'n' END",
               {{"false", "n"}});
}

TEST_F(CaseEqualityRuleTest, aNodeIsNotEqualToAValueOfAnotherType) {
    expectRows("MATCH (n:Person {name: 'Remy'}) "
               "RETURN n = true, n = [0], n = {k: 0}, n = 1.5, n = datetime('2020-01-01T00:00:00Z'), n = duration(5), n = (1, 5)",
               {{"false", "false", "false", "false", "false", "false", "false"}});
}

TEST_F(CaseEqualityRuleTest, aNullSubjectNeverEqualsANode) {
    expectRows("MATCH (n:Person {name: 'Remy'}) RETURN CASE null WHEN n THEN 'y' ELSE 'n' END",
               {{"n"}});
}

TEST_F(CaseEqualityRuleTest, comparesAMapValueToANode) {
    expectRows("MATCH (n:Person {name: 'Remy'}), (m:Person {name: 'Adam'}) WITH n, m, {k: n} AS map "
               "RETURN map.k = n, map.k = m, CASE map.k WHEN n THEN 'y' ELSE 'n' END, CASE n WHEN map.k THEN 'y' ELSE 'n' END",
               {{"true", "false", "y", "y"}});
}

TEST_F(CaseEqualityRuleTest, comparesAMapValueToAnEmbedding) {
    expectRows("UNWIND [(1, 5), (2, 3)] AS embedding WITH embedding, {k: (1, 5)} AS map "
               "RETURN map.k = embedding, CASE map.k WHEN embedding THEN 'y' ELSE 'n' END",
               {{"false", "n"}, {"true", "y"}});
}

TEST_F(CaseEqualityRuleTest, comparesAListElementToAnEmbedding) {
    expectRows("UNWIND [(1, 5), 1] AS element "
               "RETURN element = (1, 5), CASE element WHEN (1, 5) THEN 'y' ELSE 'n' END",
               {{"false", "n"}, {"true", "y"}});
}

TEST_F(CaseEqualityRuleTest, rejectsTheEqualityOfTwoDoublesAsTheBinaryEqualityDoes) {
    expectError("UNWIND [1.5, 2.5] AS d RETURN CASE d WHEN 1.5 THEN 'y' ELSE 'n' END",
                "not encouraged due to potential rounding");
}

TEST_F(CaseEqualityRuleTest, rejectsADoubleAgainstAnIntegerAsTheBinaryEqualityDoes) {
    expectError("UNWIND [1, 2] AS x RETURN CASE x WHEN 1.5 THEN 'y' ELSE 'n' END",
                "Operands are not valid or compatible types: 'Integer' and 'Double'");
}

TEST_F(CaseEqualityRuleTest, rejectsALabelAgainstAnIntegerAsTheBinaryEqualityDoes) {
    expectError("CALL db.labels() YIELD id RETURN CASE 1 WHEN id THEN 'y' ELSE 'n' END",
                "Operands are not valid or compatible types: 'Integer' and 'Label'");
}
