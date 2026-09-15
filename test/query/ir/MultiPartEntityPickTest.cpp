#include <gtest/gtest.h>

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

// Seven parts over one dataflow: rank the interests by how many Persons hold them, keep
// the top three, pick the first fan, the last fan and the last edge out of the lists each
// ranking collected, join the two fans through an interest they share, tally what the last
// one knows, and unwind that tally back into a row per shared interest.
class MultiPartEntityPickTest : public TuringTest {
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

    // The rows in the order the query emits them, since the query ends on an ORDER BY
    void expectRowsInOrder(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        QueryStatus status;
        runQuery(query, sink, status);
        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();

        EXPECT_EQ(sink.getRows(), expected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(MultiPartEntityPickTest, joinsTheFirstAndLastFanOfEveryTopInterest) {
    expectRowsInOrder("MATCH (p:Person)-[e:INTERESTED_IN]->(i) WITH i, p, e ORDER BY p.name "
                      "WITH i, collect(p) AS fans, collect(e) AS edges, count(*) AS reach "
                      "WHERE reach > 1 "
                      "WITH i, fans[0] AS first, fans[reach - 1] AS last, "
                      "edges[reach - 1] AS lastEdge, reach "
                      "ORDER BY reach DESC, i.name LIMIT 3 "
                      "MATCH (first)-[:INTERESTED_IN]->(shared)<-[:INTERESTED_IN]-(last) "
                      "OPTIONAL MATCH (last)-[:KNOWS_WELL]->(friend) "
                      "WITH i.name AS interest, first.name AS firstFan, last.name AS lastFan, reach, "
                      "lastEdge.name AS lastEdgeName, "
                      "coalesce(lastEdge.proficiency, 'none') AS lastLevel, "
                      "collect(DISTINCT shared.name) AS sharedNames, count(friend) AS friends "
                      "UNWIND sharedNames AS sharedName "
                      "WITH interest, sharedName, firstFan, lastFan, lastEdgeName, lastLevel, friends, "
                      "CASE WHEN reach > 2 THEN 'crowded' ELSE 'pair' END AS band "
                      "RETURN DISTINCT band, interest, sharedName, firstFan, lastFan, "
                      "lastEdgeName, lastLevel, friends "
                      "ORDER BY band, interest",
                      {{"crowded", "Gym", "Gym", "Cyrus", "Suhas", "Suhas -> Gym", "none", "0"},
                       {"pair", "Bio", "Bio", "Adam", "Maxime", "Maxime -> Bio", "none", "0"},
                       {"pair", "Computers", "Computers", "Luc", "Remy", "Remy -> Computers", "expert", "1"}});
}
