#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>

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

// `[(a)-[:KNOWS]->(b WHERE p(b)) | f(b)]` keeps the matches its inline WHERE accepts, as
// `[(a)-[:KNOWS]->(b) WHERE p(b) | f(b)]` does.
class PatternComprehensionWhereTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Rows actual;
        sink.sortedRows(actual);

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\ngot:\n" << actualText;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(PatternComprehensionWhereTest, filtersOnTheEndNode) {
    expectRows("MATCH (p:Person) RETURN p.name, [(p)-[:KNOWS_WELL]->(f:Person WHERE f.age > 30) | f.name]",
               {{"Remy", "[Adam]"},
                {"Adam", "[Remy]"},
                {"Maxime", "[]"},
                {"Luc", "[]"},
                {"Martina", "[]"},
                {"Suhas", "[]"},
                {"Cyrus", "[]"},
                {"Doruk", "[]"}});
}

TEST_F(PatternComprehensionWhereTest, filtersOnTheRootNode) {
    expectRows("MATCH (p:Person) RETURN p.name, [(p WHERE p.age > 30)-[:INTERESTED_IN]->(i) | i.name]",
               {{"Remy", "[Ghosts, Computers, Eighties]"},
                {"Adam", "[Bio, Cooking]"},
                {"Maxime", "[]"},
                {"Luc", "[]"},
                {"Martina", "[]"},
                {"Suhas", "[]"},
                {"Cyrus", "[]"},
                {"Doruk", "[]"}});
}

TEST_F(PatternComprehensionWhereTest, filtersOnTheEdge) {
    expectRows("MATCH (p:Person) RETURN p.name, [(p)-[e:INTERESTED_IN WHERE e.duration = 20]->(i) | i.name]",
               {{"Remy", "[Ghosts, Eighties]"},
                {"Adam", "[]"},
                {"Maxime", "[]"},
                {"Luc", "[Animals]"},
                {"Martina", "[]"},
                {"Suhas", "[]"},
                {"Cyrus", "[]"},
                {"Doruk", "[]"}});
}

TEST_F(PatternComprehensionWhereTest, combinesWithTheComprehensionWhere) {
    expectRows("MATCH (p:Person) RETURN p.name, "
               "[(p)-[:INTERESTED_IN]->(i WHERE i.name <> 'Ghosts') WHERE i.name <> 'Computers' | i.name]",
               {{"Remy", "[Eighties]"},
                {"Adam", "[Bio, Cooking]"},
                {"Maxime", "[Bio, Padel]"},
                {"Luc", "[Animals]"},
                {"Martina", "[Cooking]"},
                {"Suhas", "[Gym, JiuJitsu]"},
                {"Cyrus", "[Gym, Travel]"},
                {"Doruk", "[Gym]"}});
}

TEST_F(PatternComprehensionWhereTest, readsTheRowItIsReadOn) {
    expectRows("MATCH (p:Person) RETURN p.name, [(p)-[:KNOWS_WELL]->(f WHERE f.age = p.age) | f.name]",
               {{"Remy", "[Adam]"},
                {"Adam", "[Remy]"},
                {"Maxime", "[]"},
                {"Luc", "[]"},
                {"Martina", "[]"},
                {"Suhas", "[]"},
                {"Cyrus", "[]"},
                {"Doruk", "[]"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
