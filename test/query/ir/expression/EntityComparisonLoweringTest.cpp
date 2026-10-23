#include <gtest/gtest.h>

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

#include "StringRowSink.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// An entity comparison answers null for the invalid ID an OPTIONAL MATCH leaves either way,
// so the rows do not say whether the entity column was copied into a nullable one first:
// these read the lowered program.
class EntityComparisonLoweringTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());
    }

protected:
    void expectComparesTheIDColumnAsItStands(std::string_view query, std::string_view comparison) {
        StringRowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              std::string("EXPLAIN (nl) ") + std::string(query),
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &sink);

        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        std::string_view nlProgram;
        for (const StringRowSink::Row& row : sink.getRows()) {
            if (row.front() == "nl") {
                nlProgram = row.back();
            }
        }

        EXPECT_NE(nlProgram.find(comparison), std::string_view::npos) << nlProgram;
        EXPECT_EQ(nlProgram.find("nl.to_nullable"), std::string_view::npos) << nlProgram;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(EntityComparisonLoweringTest, comparesANodeAgainstAnIDWithoutACopy) {
    expectComparesTheIDColumnAsItStands("MATCH (n:Person) RETURN n = 1", "nl.eq");
}

TEST_F(EntityComparisonLoweringTest, comparesAnIDAgainstANodeWithoutACopy) {
    expectComparesTheIDColumnAsItStands("MATCH (n:Person) RETURN 1 <> n", "nl.neq");
}

TEST_F(EntityComparisonLoweringTest, comparesTwoNodesWithoutACopy) {
    expectComparesTheIDColumnAsItStands("MATCH (a:Person)-[:KNOWS_WELL]->(b) RETURN a <> b", "nl.neq");
}

TEST_F(EntityComparisonLoweringTest, comparesTwoEdgesWithoutACopy) {
    expectComparesTheIDColumnAsItStands("MATCH ()-[e]->()-[f]->() RETURN e = f", "nl.eq");
}

TEST_F(EntityComparisonLoweringTest, testsAMissedNodeForNullWithoutACopy) {
    expectComparesTheIDColumnAsItStands("MATCH (p:Person) OPTIONAL MATCH (p)-[:KNOWS_WELL]->(f) RETURN f IS NULL", "nl.eq");
}

TEST_F(EntityComparisonLoweringTest, testsAMissedEdgeForNullWithoutACopy) {
    expectComparesTheIDColumnAsItStands("MATCH (p:Person) OPTIONAL MATCH (p)-[r:KNOWS_WELL]->() RETURN r IS NOT NULL", "nl.neq");
}
