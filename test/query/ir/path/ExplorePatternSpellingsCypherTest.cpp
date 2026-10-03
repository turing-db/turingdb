#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;

}

// Spellings of a variable-length pattern no other test writes, against a spelling of the
// same walk they do
class ExplorePatternSpellingsCypherTest : public CallV3Test {
protected:
    void collectRows(std::string_view query, Rows& rows) {
        StringRowSink sink;
        runQuery(query, sink);
        sink.sortedRows(rows);
    }

    void expectSameRows(std::string_view spelling, std::string_view reference) {
        Rows fromSpelling;
        collectRows(spelling, fromSpelling);

        Rows fromReference;
        collectRows(reference, fromReference);

        EXPECT_FALSE(fromSpelling.empty()) << spelling;
        EXPECT_EQ(fromSpelling, fromReference) << spelling << "\n" << reference;
    }
};

TEST_F(ExplorePatternSpellingsCypherTest, walksARangeWithNoVariable) {
    expectSameRows("MATCH (n:Person)-[*1..3]->(m) RETURN n.name, m.name",
                   "MATCH (n:Person)-[e*1..3]->(m) RETURN n.name, m.name");
}

TEST_F(ExplorePatternSpellingsCypherTest, walksATypedStarWithNoVariable) {
    expectSameRows("MATCH (n:Person)-[:KNOWS_WELL*]-(m) RETURN n.name, m.name",
                   "MATCH (n:Person)-[e:KNOWS_WELL*]-(m) RETURN n.name, m.name");
}

TEST_F(ExplorePatternSpellingsCypherTest, walksTheForwardArrowShorthand) {
    expectSameRows("MATCH (n:Person)-->+(m:Interest) RETURN n.name, m.name",
                   "MATCH (n:Person)-[e]->+(m:Interest) RETURN n.name, m.name");
}

TEST_F(ExplorePatternSpellingsCypherTest, walksTheBackwardArrowShorthand) {
    expectSameRows("MATCH (n:Interest)<--{1,3}(m) RETURN n.name, m.name",
                   "MATCH (n:Interest)<-[e]-{1,3}(m) RETURN n.name, m.name");
}

TEST_F(ExplorePatternSpellingsCypherTest, walksTheUndirectedShorthand) {
    expectSameRows("MATCH (n:Person)--{1,2}(m) RETURN n.name, m.name",
                   "MATCH (n:Person)-[e]-{1,2}(m) RETURN n.name, m.name");
}

TEST_F(ExplorePatternSpellingsCypherTest, walksAParenthesizedPatternOneOrMoreTimes) {
    expectSameRows("MATCH (n:Person)((a)-[e]->(b))+(m) RETURN n.name, m.name",
                   "MATCH (n:Person)-[e]->+(m) RETURN n.name, m.name");
}

TEST_F(ExplorePatternSpellingsCypherTest, namesAParenthesizedPattern) {
    expectSameRows("MATCH p = (n:Person)((a)-[e:KNOWS_WELL]->(b)){1,3}(m) RETURN p",
                   "MATCH p = (n:Person)-[e:KNOWS_WELL]->{1,3}(m) RETURN p");
}

TEST_F(ExplorePatternSpellingsCypherTest, walksTwoRelationshipsOfOneChain) {
    expectSameRows("MATCH (n:Person)-[e:KNOWS_WELL]->+(m)-[f:INTERESTED_IN]->{1,2}(i) RETURN n.name, m.name, i.name",
                   "MATCH (n:Person)-[e:KNOWS_WELL]->+(m) MATCH (m)-[f:INTERESTED_IN]->{1,2}(i) RETURN n.name, m.name, i.name");
}

TEST_F(ExplorePatternSpellingsCypherTest, walksInsideAPatternComprehension) {
    expectSameRows("MATCH (n:Person) RETURN n.name, size([(n)-[e]->+(m:Person) | m.name])",
                   "MATCH (n:Person) OPTIONAL MATCH (n)-[e]->+(m:Person) RETURN n.name, count(m)");
}

TEST_F(ExplorePatternSpellingsCypherTest, walksInsideAnExistsSubquery) {
    expectSameRows("MATCH (n:Person) WHERE EXISTS { (n)-[*1..2]->(:Interest) } RETURN n.name",
                   "MATCH (n:Person)-[e*1..2]->(:Interest) RETURN DISTINCT n.name");
}

TEST_F(ExplorePatternSpellingsCypherTest, walksInsideAPatternPredicate) {
    expectSameRows("MATCH (n:Person), (m:Person) WHERE (n)-[:KNOWS_WELL*1..3]->(m) RETURN n.name, m.name",
                   "MATCH (n:Person)-[e:KNOWS_WELL*1..3]->(m:Person) RETURN DISTINCT n.name, m.name");
}
