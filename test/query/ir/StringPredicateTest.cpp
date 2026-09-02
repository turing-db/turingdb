#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "StringRowSink.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

class StringPredicateTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());
    }

protected:
    void expectRows(std::string_view query, std::vector<StringRowSink::Row> expected) {
        StringRowSink sink;
        QueryStatus status;

        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);

        std::sort(expected.begin(), expected.end());
        EXPECT_EQ(rows, expected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(StringPredicateTest, startsWithLiteralPrefix) {
    expectRows("MATCH (n) WHERE n.name STARTS WITH 'C' RETURN n.name",
               {{"Computers"}, {"Cooking"}, {"Cyrus"}});

    expectRows("MATCH (n) WHERE n.name STARTS WITH 'A' RETURN n.name",
               {{"Adam"}, {"Animals"}});

    expectRows("MATCH (n) WHERE n.name STARTS WITH 'Rem' RETURN n.name",
               {{"Remy"}});

    expectRows("MATCH (n) WHERE n.name STARTS WITH 'Z' RETURN n.name",
               {});
}

TEST_F(StringPredicateTest, endsWithLiteralSuffix) {
    expectRows("MATCH (n) WHERE n.name ENDS WITH 'o' RETURN n.name",
               {{"Bio"}});

    expectRows("MATCH (n) WHERE n.name ENDS WITH 'm' RETURN n.name",
               {{"Adam"}, {"Gym"}});

    expectRows("MATCH (n) WHERE n.name ENDS WITH 'ties' RETURN n.name",
               {{"Eighties"}});
}

TEST_F(StringPredicateTest, containsLiteralSubstring) {
    expectRows("MATCH (n) WHERE n.name CONTAINS 'oo' RETURN n.name",
               {{"Cooking"}});

    expectRows("MATCH (n) WHERE n.name CONTAINS 'us' RETURN n.name",
               {{"Cyrus"}});

    expectRows("MATCH (n) WHERE n.name CONTAINS 'o' RETURN n.name",
               {{"Computers"}, {"Bio"}, {"Cooking"}, {"Ghosts"}, {"Doruk"}});
}

TEST_F(StringPredicateTest, nonLiteralPropertyOperands) {
    expectRows("MATCH (n:Person) WHERE n.name CONTAINS n.name RETURN n.name",
               {{"Remy"}, {"Adam"}, {"Maxime"}, {"Luc"}, {"Martina"}, {"Suhas"}, {"Cyrus"}, {"Doruk"}});

    expectRows("MATCH (a)-[e]->(b) WHERE a.name = 'Remy' AND e.name STARTS WITH a.name RETURN e.name",
               {{"Remy -> Adam"}, {"Remy -> Ghosts"}, {"Remy -> Computers"}, {"Remy -> Eighties"}});
}

TEST_F(StringPredicateTest, combinedWithBooleanPredicates) {
    expectRows("MATCH (n) WHERE n.name STARTS WITH 'C' AND n.name ENDS WITH 's' RETURN n.name",
               {{"Computers"}, {"Cyrus"}});

    expectRows("MATCH (n) WHERE n.name STARTS WITH 'A' OR n.name STARTS WITH 'G' RETURN n.name",
               {{"Adam"}, {"Animals"}, {"Ghosts"}, {"Gym"}});
}

TEST_F(StringPredicateTest, combinedWithPropertyPredicates) {
    expectRows("MATCH (n:Person) WHERE n.name STARTS WITH 'M' AND n.isFrench = true RETURN n.name",
               {{"Maxime"}});

    expectRows("MATCH (n) WHERE n.name STARTS WITH 'A' AND n.age = 32 RETURN n.name",
               {{"Adam"}});
}

TEST_F(StringPredicateTest, countOverOrOfStringPredicates) {
    expectRows("MATCH (n) WHERE n.name STARTS WITH 'C' OR n.name STARTS WITH 'A' RETURN count(n)",
               {{"5"}});

    expectRows("MATCH (n) WHERE n.name ENDS WITH 's' OR n.name STARTS WITH 'G' RETURN count(n)",
               {{"7"}});

    expectRows("MATCH (n) WHERE n.name CONTAINS 'oo' OR n.name ENDS WITH 'l' RETURN count(n)",
               {{"3"}});

    expectRows("MATCH (n) WHERE n.name STARTS WITH 'C' OR n.name STARTS WITH 'A' RETURN count(*)",
               {{"5"}});
}

TEST_F(StringPredicateTest, countMixingStringAndPropertyPredicates) {
    expectRows("MATCH (n:Person) WHERE n.name ENDS WITH 's' OR n.isFrench = true RETURN count(n)",
               {{"6"}});

    expectRows("MATCH (n:Person) WHERE n.name STARTS WITH 'M' OR n.hasPhD = false RETURN count(n)",
               {{"5"}});

    expectRows("MATCH (n) WHERE n.name STARTS WITH 'C' OR n.isReal = false RETURN count(n)",
               {{"4"}});

    expectRows("MATCH (n:Person) WHERE n.name STARTS WITH 'M' AND n.hasPhD = false RETURN count(n)",
               {{"1"}});

    expectRows("MATCH (n:Person) WHERE n.name ENDS WITH 's' AND n.isFrench = false RETURN count(n)",
               {{"2"}});

    expectRows("MATCH (n) WHERE n.name CONTAINS 'o' AND n.isReal = true RETURN count(n)",
               {{"2"}});
}

TEST_F(StringPredicateTest, sumOfAgeUnderStringPredicates) {
    expectRows("MATCH (n:Person) WHERE n.name STARTS WITH 'R' OR n.name STARTS WITH 'A' RETURN sum(n.age)",
               {{"64"}});

    expectRows("MATCH (n:Person) WHERE n.name ENDS WITH 'y' OR n.hasPhD = true RETURN sum(n.age)",
               {{"64"}});

    expectRows("MATCH (n:Person) WHERE n.name STARTS WITH 'S' OR n.name STARTS WITH 'D' RETURN sum(n.age)",
               {{"0"}});

    expectRows("MATCH (n:Person) WHERE n.name STARTS WITH 'Z' RETURN sum(n.age)",
               {{"0"}});
}

TEST_F(StringPredicateTest, countAndSumTogether) {
    expectRows("MATCH (n:Person) WHERE n.name STARTS WITH 'R' OR n.name ENDS WITH 'm' RETURN count(n), sum(n.age)",
               {{"2", "64"}});

    expectRows("MATCH (n:Person) WHERE n.name ENDS WITH 's' OR n.isFrench = true RETURN count(n), sum(n.age)",
               {{"6", "64"}});

    expectRows("MATCH (n:Person) WHERE n.name STARTS WITH 'R' OR n.name STARTS WITH 'S' RETURN count(n), count(n.age)",
               {{"2", "1"}});
}

TEST_F(StringPredicateTest, aggregatesOverEdgeStringPredicates) {
    expectRows("MATCH (a)-[e]->(b) WHERE e.name STARTS WITH 'Remy' RETURN count(e), sum(e.duration)",
               {{"4", "60"}});

    expectRows("MATCH (a)-[e]->(b) WHERE e.name STARTS WITH 'Remy' OR e.name STARTS WITH 'Ghosts' RETURN sum(e.duration)",
               {{"260"}});

    expectRows("MATCH (a)-[e]->(b) WHERE e.name ENDS WITH 'Gym' RETURN count(e), sum(e.duration)",
               {{"3", "0"}});
}

TEST_F(StringPredicateTest, parenthesizedAndOrPermutations) {
    expectRows("MATCH (n:Person) WHERE (n.name STARTS WITH 'M' OR n.name STARTS WITH 'R') AND n.hasPhD = true RETURN count(n)",
               {{"2"}});

    expectRows("MATCH (n:Person) WHERE (n.name ENDS WITH 's' OR n.name ENDS WITH 'y') AND n.isFrench = false RETURN count(n)",
               {{"2"}});

    expectRows("MATCH (n) WHERE n.name STARTS WITH 'C' AND (n.name ENDS WITH 's' OR n.name ENDS WITH 'g') RETURN count(n)",
               {{"3"}});

    expectRows("MATCH (n:Person) WHERE n.name STARTS WITH 'R' OR n.name STARTS WITH 'A' OR n.name STARTS WITH 'M' RETURN count(n), sum(n.age)",
               {{"4", "64"}});
}

TEST_F(StringPredicateTest, aggregatesOverDobStringPredicates) {
    expectRows("MATCH (n:Person) WHERE n.dob STARTS WITH '18' RETURN count(n)",
               {{"2"}});

    expectRows("MATCH (n:Person) WHERE n.dob ENDS WITH '05' OR n.dob ENDS WITH '07' RETURN count(n)",
               {{"2"}});

    expectRows("MATCH (n:Person) WHERE n.dob CONTAINS '/0' RETURN count(n), sum(n.age)",
               {{"4", "64"}});

    expectRows("MATCH (n:Person) WHERE n.name STARTS WITH 'A' OR n.dob STARTS WITH '24' RETURN count(n), sum(n.age)",
               {{"2", "32"}});
}

TEST_F(StringPredicateTest, taggedListElementOperands) {
    // A heterogeneous list hands out cells that carry their own type, so the predicate is
    // tested per row: a cell holding a string matches on its characters, and a cell holding
    // anything else has none to match.
    expectRows("UNWIND ['C', 5] AS v MATCH (n) WHERE n.name STARTS WITH v RETURN n.name",
               {{"Computers"}, {"Cooking"}, {"Cyrus"}});

    expectRows("UNWIND ['ties', 7] AS v MATCH (n) WHERE n.name ENDS WITH v RETURN n.name",
               {{"Eighties"}});

    expectRows("UNWIND ['oo', 7] AS v MATCH (n) WHERE n.name CONTAINS v RETURN n.name",
               {{"Cooking"}});

    expectRows("UNWIND [1, 2.5] AS v MATCH (n) WHERE n.name STARTS WITH v RETURN n.name", {});
}

TEST_F(StringPredicateTest, taggedListElementOnBothSides) {
    expectRows("UNWIND ['Remy', 1] AS a UNWIND ['Rem', 2] AS b "
               "MATCH (n) WHERE n.name = a AND n.name STARTS WITH b RETURN n.name",
               {{"Remy"}});
}

TEST_F(StringPredicateTest, taggedListElementAgainstALiteral) {
    expectRows("UNWIND ['Remy', 1] AS v MATCH (n) WHERE n.name = v AND v STARTS WITH 'Rem' "
               "RETURN n.name",
               {{"Remy"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
