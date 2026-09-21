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

// A keyless aggregate inside a per-row CALL subquery runs once per outer row, so each
// row gets its own list. An ORDER BY past the CALL reorders those rows and leaves the
// lists they carry alone.
class SubqueryCollectSortTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void runQuery(std::string_view query, StringRowSink& sink, QueryStatus& status) {
        _interpreter->execute(status, query, _graphName, CommitHash::head(), ChangeID::head(), &_env->getMem(), &sink);
    }

    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        QueryStatus status;
        runQuery(query, sink, status);
        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();

        std::vector<StringRowSink::Row> actual;
        sink.sortedRows(actual);

        std::vector<StringRowSink::Row> sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        EXPECT_EQ(actual, sortedExpected) << "query: " << query;
    }

    const std::vector<StringRowSink::Row> _interestsPerPerson {
        {"Remy", "Ghosts, Computers, Eighties"},
        {"Adam", "Bio, Cooking"},
        {"Maxime", "Bio, Padel"},
        {"Luc", "Animals, Computers"},
        {"Martina", "Cooking"},
        {"Suhas", "Gym, JiuJitsu"},
        {"Cyrus", "Gym, Travel"},
        {"Doruk", "Gym"},
    };

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(SubqueryCollectSortTest, collectsTheInterestsOfEachPerson) {
    expectRows("MATCH (p:Person) "
               "CALL { WITH p MATCH (p)-[:INTERESTED_IN]->(i) RETURN collect(i.name) AS names } "
               "RETURN p.name, names",
               _interestsPerPerson);
}

TEST_F(SubqueryCollectSortTest, ordersTheCollectedInterestsByPerson) {
    expectRows("MATCH (p:Person) "
               "CALL { WITH p MATCH (p)-[:INTERESTED_IN]->(i) RETURN collect(i.name) AS names } "
               "RETURN p.name, names ORDER BY p.name",
               _interestsPerPerson);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
