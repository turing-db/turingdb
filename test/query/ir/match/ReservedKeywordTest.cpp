#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

namespace {

// openCypher reserves these words, and Neo4j 5.26 accepts every one of them as a name all
// the same. The expected rows are Neo4j's
constexpr std::string_view RESERVED_WORDS[] = {
    "add", "all", "and", "as", "asc", "ascending", "by", "case", "constraint", "contains",
    "create", "delete", "desc", "descending", "detach", "distinct", "do", "drop", "else",
    "end", "ends", "exists", "for", "in", "is", "limit", "mandatory", "match", "merge",
    "not", "of", "on", "optional", "or", "order", "remove", "require", "return", "scalar",
    "set", "skip", "starts", "then", "union", "unique", "unwind", "when", "where", "with",
    "xor",
};

void replaceName(std::string_view pattern, std::string_view name, std::string& query) {
    query.clear();

    for (const char c : pattern) {
        if (c == '@') {
            query += name;
        } else {
            query += c;
        }
    }
}

}

class ReservedKeywordTest : public CallV3Test {
protected:
    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << query;
    }

    void expectRowsForEveryWord(std::string_view pattern, const std::vector<StringRowSink::Row>& expected) {
        std::string query;
        for (const std::string_view word : RESERVED_WORDS) {
            replaceName(pattern, word, query);
            expectRows(query, expected);
        }
    }
};

TEST_F(ReservedKeywordTest, namesANode) {
    expectRowsForEveryWord("MATCH (@:Person) WHERE @.age = 32 RETURN @.name", {{"Adam"}, {"Remy"}});
}

TEST_F(ReservedKeywordTest, projectsANode) {
    expectRowsForEveryWord("MATCH (@:Person) WHERE @.age = 32 WITH @, 1 AS one RETURN @.name", {{"Adam"}, {"Remy"}});
}

// Neo4j reads all and distinct in count(all) and count(distinct) as the modifiers of count
TEST_F(ReservedKeywordTest, countsANode) {
    std::string query;
    for (const std::string_view word : RESERVED_WORDS) {
        if (word == "all" || word == "distinct") {
            continue;
        }

        replaceName("MATCH (@:Person) RETURN count(@)", word, query);
        expectRows(query, {{"8"}});
    }
}

TEST_F(ReservedKeywordTest, unwindsIntoAVariable) {
    expectRowsForEveryWord("UNWIND [1, 2] AS @ RETURN @", {{"1"}, {"2"}});
}

TEST_F(ReservedKeywordTest, readsAnAlias) {
    expectRowsForEveryWord("WITH 1 AS @ RETURN 10 * @", {{"10"}});
}

TEST_F(ReservedKeywordTest, namesAnEdge) {
    expectRowsForEveryWord("MATCH (a:Person {name: 'Remy'})-[@:KNOWS_WELL]->(b) RETURN @.duration", {{"20"}});
}

TEST_F(ReservedKeywordTest, namesAPath) {
    expectRowsForEveryWord("MATCH @ = ()-[:KNOWS_WELL]->() RETURN count(*)", {{"3"}});
}

TEST_F(ReservedKeywordTest, iteratesAComprehension) {
    expectRowsForEveryWord("RETURN [@ IN [1, 2, 3] WHERE @ > 1 | @ * 10]", {{"20, 30"}});
}

TEST_F(ReservedKeywordTest, accumulatesAReduce) {
    expectRowsForEveryWord("RETURN reduce(@ = 0, x IN [1, 2, 3] | x + @)", {{"6"}});
}

TEST_F(ReservedKeywordTest, keysAMap) {
    expectRowsForEveryWord("WITH {@: 1} AS m RETURN m.@", {{"1"}});
}

TEST_F(ReservedKeywordTest, readsLabelsAndPropertiesNamedAfterReservedWords) {
    runWrite("CREATE (:Order:Return {where: 1, union: 2})");

    expectRows("MATCH (order:Order:Return) RETURN order.where, order.union", {{"1", "2"}});
}

TEST_F(ReservedKeywordTest, aliasesAVariableNot) {
    expectRows("WITH 1 AS not RETURN not AS x", {{"1"}});
}

TEST_F(ReservedKeywordTest, testsAVariableNotForNull) {
    expectRows("WITH 1 AS not RETURN not IS NULL", {{"false"}});
}

TEST_F(ReservedKeywordTest, negatesAVariableMatch) {
    expectRows("WITH true AS match RETURN NOT match", {{"false"}});
}

TEST_F(ReservedKeywordTest, projectsADistinctVariableOrder) {
    expectRows("WITH 1 AS order RETURN DISTINCT order", {{"1"}});
}

TEST_F(ReservedKeywordTest, projectsADistinctNegation) {
    expectRows("WITH 1 AS distinct RETURN distinct - 1", {{"-1"}});
}

TEST_F(ReservedKeywordTest, aliasesAVariableCase) {
    expectRows("WITH 1 AS case RETURN case AS x", {{"1"}});
}

TEST_F(ReservedKeywordTest, testsAVariableCaseForMembership) {
    expectRows("WITH 1 AS case RETURN case IN [1]", {{"true"}});
}

TEST_F(ReservedKeywordTest, switchesOnAVariableOrder) {
    expectRows("WITH 'paid' AS order RETURN CASE order WHEN 'paid' THEN 1 ELSE 0 END", {{"1"}});
}

TEST_F(ReservedKeywordTest, comparesAVariableWhen) {
    expectRows("WITH 1 AS when RETURN CASE WHEN when = 1 THEN 'one' ELSE 'other' END", {{"one"}});
}

TEST_F(ReservedKeywordTest, quantifiesOverAVariableAll) {
    expectRows("WITH [1] AS all RETURN all(x IN all WHERE x > 0)", {{"true"}});
}

TEST_F(ReservedKeywordTest, parenthesizesAVariableWhere) {
    expectRows("WITH 1 AS where RETURN (where + 1)", {{"2"}});
}

TEST_F(ReservedKeywordTest, matchesANodeWhereByItsProperties) {
    expectRows("MATCH (where {name: 'Remy'}) RETURN where.name", {{"Remy"}});
}

TEST_F(ReservedKeywordTest, filtersANodeWhereInItsPattern) {
    expectRows("MATCH (where WHERE where.name = 'Remy') RETURN where.name", {{"Remy"}});
}

TEST_F(ReservedKeywordTest, countsAVariableExists) {
    expectRows("MATCH (exists:Person) RETURN count(exists)", {{"8"}});
}

TEST_F(ReservedKeywordTest, matchesANodeCountByItsProperties) {
    expectRows("MATCH (count {name: 'Remy'}) RETURN count.name", {{"Remy"}});
}

// A literal keyword binds a name that an expression cannot read: true, false and null in an
// expression are the literals
TEST_F(ReservedKeywordTest, unwindsIntoTrue) {
    expectRows("UNWIND [1, 2] AS true RETURN true", {{"true"}, {"true"}});
}

TEST_F(ReservedKeywordTest, aliasesNull) {
    expectRows("WITH 1 AS null RETURN null", {{"null"}});
}

TEST_F(ReservedKeywordTest, namesANodeTrue) {
    expectRows("MATCH (true:Person) RETURN count(*)", {{"8"}});
}

TEST_F(ReservedKeywordTest, namesAnEdgeNull) {
    expectRows("MATCH ()-[null:KNOWS_WELL]->() RETURN count(*)", {{"3"}});
}

TEST_F(ReservedKeywordTest, namesAPathFalse) {
    expectRows("MATCH false = ()-[:KNOWS_WELL]->() RETURN count(*)", {{"3"}});
}
