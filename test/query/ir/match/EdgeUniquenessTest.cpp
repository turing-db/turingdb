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

// Relationship isomorphism over fixed patterns: no edge is bound twice in one MATCH clause,
// nodes may repeat, and the rule stops at the clause. Expected counts are an enumeration of
// simpledb's 18 edges under openCypher's rule.
class EdgeUniquenessTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());
    }

protected:
    void run(std::string_view query, StringRowSink& sink) {
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &sink);

        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();
    }

    void expectCount(std::string_view query, std::string_view count) {
        StringRowSink sink;
        run(query, sink);

        EXPECT_EQ(sink.getRows(), (std::vector<StringRowSink::Row> {{std::string {count}}})) << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(EdgeUniquenessTest, excludesTheBacktrackOfAnUndirectedHop) {
    expectCount("MATCH (a)-[e1]->(b)-[e2]-(c) RETURN count(*)", "26");
}

TEST_F(EdgeUniquenessTest, excludesTheSharedEdgeOfTwoHopsMeetingAtANode) {
    expectCount("MATCH (a)-[e1]->(b)<-[e2]-(c) RETURN count(*)", "14");
}

TEST_F(EdgeUniquenessTest, excludesRepeatsAcrossTwoUndirectedHops) {
    expectCount("MATCH (a)-[e1]-(b)-[e2]-(c) RETURN count(*)", "64");
}

TEST_F(EdgeUniquenessTest, excludesRepeatsAcrossThreeUndirectedHops) {
    expectCount("MATCH (a)-[e1]-(b)-[e2]-(c)-[e3]-(d) RETURN count(*)", "106");
}

TEST_F(EdgeUniquenessTest, excludesTheTwoCycleOfADirectedChain) {
    expectCount("MATCH (a)-[e1]->(b)-[e2]->(c)-[e3]->(d) RETURN count(*)", "12");
}

TEST_F(EdgeUniquenessTest, excludesOneEdgeBoundByBothCommaSeparatedPatterns) {
    expectCount("MATCH (a)-[e1]->(b), (c)-[e2]->(d) RETURN count(*)", "306");
}

TEST_F(EdgeUniquenessTest, appliesToAnonymousEdges) {
    expectCount("MATCH (a)-->(b)--(a) RETURN count(*)", "4");
    expectCount("MATCH (a)-->(b)<--(a) RETURN count(*)", "0");
}

TEST_F(EdgeUniquenessTest, keepsTwoDirectedHopsThatOnlyASelfLoopCouldShare) {
    expectCount("MATCH (a)-[e1]->(b)-[e2]->(c) RETURN count(*)", "12");
}

TEST_F(EdgeUniquenessTest, keepsHopsOfDisjointTypes) {
    expectCount("MATCH (a)-[e1:KNOWS_WELL]->(b)-[e2:INTERESTED_IN]->(c) RETURN count(*)", "8");
}

TEST_F(EdgeUniquenessTest, agreesWithAnExplicitInequality) {
    expectCount("MATCH (a)-[e1]->(b)-[e2]-(c) WHERE e1 <> e2 RETURN count(*)", "26");
}

TEST_F(EdgeUniquenessTest, doesNotReachASecondMatch) {
    expectCount("MATCH (a)-[e1]->(b) MATCH (b)-[e2]-(c) RETURN count(*)", "44");
}

TEST_F(EdgeUniquenessTest, doesNotReachAnOptionalMatch) {
    expectCount("MATCH (a)-[e1]->(b) OPTIONAL MATCH (b)-[e2]-(c) RETURN count(*)", "44");
}
