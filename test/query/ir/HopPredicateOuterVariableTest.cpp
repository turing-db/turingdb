#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace turing::test;

// A predicate on a hop is written where the pattern is, so it reads the variables the query
// has already bound as well as the hop's own source, edge and end. A bound variable holds
// one value for the whole walk leaving a seed, and every hop of that walk is measured
// against it.
//
// The fixture's two aged people are both 32 and every KNOWS_WELL edge between them lasts 20,
// so nothing there tells one seed from another. The test writes a pair that does: Kid at 5
// knows Pal at 9 for 20, which is the only edge whose duration clears its source's age.
class HopPredicateOuterVariableTest : public CallV3Test {
protected:
    void writeThePairThatClearsItsAge() {
        runWrite("CREATE (k:Person {name: 'Kid', age: 5})-[:KNOWS_WELL {duration: 20}]->(p:Person {name: 'Pal', age: 9})");
    }

    void rows(const std::string& query, std::vector<StringRowSink::Row>& out) {
        StringRowSink sink;
        runQuery(query, sink);
        sink.sortedRows(out);
    }
};

TEST_F(HopPredicateOuterVariableTest, aOneHopPredicateOnTheSeedIsTheSameAsAWhereOnTheEdge) {
    writeThePairThatClearsItsAge();

    std::vector<StringRowSink::Row> onTheHop;
    rows("MATCH (n:Person)-[e:KNOWS_WELL*1..1 WHERE e.duration > n.age]->(m) RETURN n.name, m.name", onTheHop);

    std::vector<StringRowSink::Row> onTheEdge;
    rows("MATCH (n:Person)-[e:KNOWS_WELL]->(m) WHERE e.duration > n.age RETURN n.name, m.name", onTheEdge);

    const std::vector<StringRowSink::Row> expected {{"Kid", "Pal"}};
    EXPECT_EQ(onTheHop, expected);
    EXPECT_EQ(onTheEdge, expected);
}

TEST_F(HopPredicateOuterVariableTest, eachSeedMeasuresItsHopsAgainstItsOwnValue) {
    writeThePairThatClearsItsAge();

    std::vector<StringRowSink::Row> reached;
    rows("MATCH (n:Person)-[e:KNOWS_WELL*1..2 WHERE e.duration > n.age]->(m) RETURN n.name, m.name", reached);

    // Remy and Adam are 32 and every edge they can walk lasts 20, so neither takes a hop
    const std::vector<StringRowSink::Row> expected {{"Kid", "Pal"}};
    EXPECT_EQ(reached, expected);
}

TEST_F(HopPredicateOuterVariableTest, aQuantifiedHopPredicateReadsTheSeedToo) {
    writeThePairThatClearsItsAge();

    std::vector<StringRowSink::Row> reached;
    rows("MATCH (n:Person)((a)-[e:KNOWS_WELL]->(b) WHERE b.age > n.age){1,2}(m) RETURN n.name, m.name", reached);

    // Pal at 9 is the only end older than the seed it was reached from
    const std::vector<StringRowSink::Row> expected {{"Kid", "Pal"}};
    EXPECT_EQ(reached, expected);
}

TEST_F(HopPredicateOuterVariableTest, aHopPredicateStillReadsOnlyTheHopWhenItNamesNothingElse) {
    writeThePairThatClearsItsAge();

    std::vector<StringRowSink::Row> reached;
    rows("MATCH (n:Person)-[e:KNOWS_WELL*1..1 WHERE e.duration > 100]->(m) RETURN n.name, m.name", reached);

    EXPECT_TRUE(reached.empty());
}

TEST_F(HopPredicateOuterVariableTest, distinctOverAPredicateThatReadsTheSeed) {
    writeThePairThatClearsItsAge();

    // The distinct search expands a batch of seeds one level at a time, so it has no row to
    // read the seed's age at. The walk answers this one instead, and still deduplicates.
    std::vector<StringRowSink::Row> reached;
    rows("MATCH (n:Person)-[e:KNOWS_WELL*1..2 WHERE e.duration > n.age]->(m) RETURN DISTINCT m.name", reached);

    const std::vector<StringRowSink::Row> expected {{"Pal"}};
    EXPECT_EQ(reached, expected);
}
