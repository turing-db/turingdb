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

// A path a WITH publishes under two names is that path under each, past the filter its WHERE
// cuts the rows with. Of the people, only Adam has an edge to Remy, and Remy walks back to
// himself through Adam and through Ghosts.
class PathUnderTwoNamesTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void runQuery(std::string_view query, StringRowSink& sink, QueryStatus& status) {
        _interpreter->execute(status, query, _graphName, CommitHash::head(), ChangeID::head(), &sink);
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

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(PathUnderTwoNamesTest, readsTheNodesOfThePathUnderEachName) {
    expectRows("MATCH p = (a:Person)-[e]->(b) WITH a, e, b, p AS q, p AS z WHERE b.name = 'Remy' "
               "RETURN a.name, [x IN nodes(q) | x.name], [x IN nodes(z) | x.name]",
               {{"Adam", "Adam, Remy", "Adam, Remy"}});
}

TEST_F(PathUnderTwoNamesTest, readsTheLengthOfThePathUnderEachName) {
    expectRows("MATCH p = (a:Person)-[e]->(b) WITH a, e, b, p AS q, p AS z WHERE b.name = 'Remy' "
               "RETURN a.name, length(q), length(z)",
               {{"Adam", "1", "1"}});
}

TEST_F(PathUnderTwoNamesTest, readsTheRelationshipsOfThePathPublishedFirst) {
    expectRows("MATCH p = (a:Person)-[e]->(b) WITH p AS q, p AS z, a, e, b WHERE b.name = 'Remy' "
               "RETURN [r IN relationships(q) | r.name], [r IN relationships(z) | r.name]",
               {{"Adam -> Remy", "Adam -> Remy"}});
}

TEST_F(PathUnderTwoNamesTest, readsThePathTheFilterAlsoTests) {
    expectRows("MATCH p = (a:Person)-[e]->(b) WITH a, b, p AS q, p AS z WHERE b.name = 'Remy' AND length(q) = 1 "
               "RETURN a.name, length(z), [x IN nodes(z) | x.name]",
               {{"Adam", "1", "Adam, Remy"}});
}

TEST_F(PathUnderTwoNamesTest, readsTheLengthOfAWalkUnderEachName) {
    expectRows("MATCH p = (a:Person)-[*1..2]->(b) WITH a, b, p AS q, p AS z WHERE b.name = 'Remy' "
               "RETURN a.name, length(q), length(z)",
               {{"Remy", "2", "2"}, {"Remy", "2", "2"}, {"Adam", "1", "1"}});
}
