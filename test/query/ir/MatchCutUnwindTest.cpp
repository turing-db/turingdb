#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>

#include "NLOutputSink.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// A cut a MATCH carries is charged to the rows that MATCH produced. An UNWIND written
// after it opens a dataflow of its own and crosses those rows, so the cut has to have
// been applied before the cross: charging it to the crossed rows spends the limit on
// repetitions of the first row and drops the rest of the matched entities outright.
class MatchCutUnwindTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    QueryStatus runQuery(std::string_view query, NLOutputSink* sink) {
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              sink);

        return status;
    }

    // An UNWIND does not order what it crosses, so the rows are compared as a set
    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Rows actual;
        sink.sortedRows(actual);

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\ngot:\n" << actualText;
    }

    void expectCounts(std::string_view query, const Counts& expected) {
        CountSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Counts actual;
        sink.sortedCounts(actual);

        EXPECT_EQ(actual, expected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

// The two people the limit keeps are Adam and Cyrus, and each is crossed with both
// elements of the list: four rows, and Cyrus is one of them.
TEST_F(MatchCutUnwindTest, crossesTheUnwindWithTheRowsTheLimitKept) {
    expectRows("MATCH (a:Person) ORDER BY a.name LIMIT 2 UNWIND [1, 2] AS x RETURN a.name, x",
               {{"Adam", "1"}, {"Adam", "2"}, {"Cyrus", "1"}, {"Cyrus", "2"}});
}

TEST_F(MatchCutUnwindTest, countsTheRowsTheLimitKeptCrossedWithTheUnwind) {
    expectCounts("MATCH (a:Person) ORDER BY a.name LIMIT 2 UNWIND [1, 2] AS x RETURN count(*)", {4});
}

// A SKIP is charged to the same rows: the two people left after six are dropped, each
// crossed with both elements.
TEST_F(MatchCutUnwindTest, crossesTheUnwindWithTheRowsTheSkipKept) {
    expectRows("MATCH (a:Person) ORDER BY a.name SKIP 6 UNWIND [1, 2] AS x RETURN a.name, x",
               {{"Remy", "1"}, {"Remy", "2"}, {"Suhas", "1"}, {"Suhas", "2"}});
}

// Every person the limit kept reaches the group of each unwound value, so both lists hold
// both of them: a cut charged to the crossed rows loses one of the two entirely.
TEST_F(MatchCutUnwindTest, collectsEveryRowTheLimitKept) {
    expectRows("MATCH (a:Person) ORDER BY a.name LIMIT 2 UNWIND [1, 2] AS x "
               "WITH x, collect(a.name) AS people RETURN x, people",
               {{"1", "[Adam, Cyrus]"}, {"2", "[Adam, Cyrus]"}});
}

TEST_F(MatchCutUnwindTest, crossesTwoUnwindsWithTheRowsTheLimitKept) {
    expectCounts("MATCH (a:Person) ORDER BY a.name LIMIT 2 "
                 "UNWIND [1, 2] AS x UNWIND [3, 4] AS y RETURN count(*)",
                 {8});
}

// The same cut spelled on a barrier, which already charges it to the matched rows: the
// MATCH-level cut above has to land on the same four rows as this.
TEST_F(MatchCutUnwindTest, cutsAtABarrierBeforeTheUnwind) {
    expectRows("MATCH (a:Person) WITH a ORDER BY a.name LIMIT 2 UNWIND [1, 2] AS x RETURN a.name, x",
               {{"Adam", "1"}, {"Adam", "2"}, {"Cyrus", "1"}, {"Cyrus", "2"}});
}

// A MATCH written after the UNWIND already closes the part on its cut, so the two people
// are crossed with both elements and then with the ten interests.
TEST_F(MatchCutUnwindTest, cutsWhenAMatchFollowsTheUnwind) {
    expectCounts("MATCH (a:Person) ORDER BY a.name LIMIT 2 UNWIND [1, 2] AS x "
                 "MATCH (i:Interest) RETURN count(*)",
                 {40});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
