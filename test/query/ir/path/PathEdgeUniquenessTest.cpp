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

// Relationship isomorphism across the boundary of a variable-length path: an edge a fixed
// hop or another path of the same MATCH bound is not on the path. Expected counts are an
// enumeration of simpledb's 18 edges under openCypher's rule.
class PathEdgeUniquenessTest : public TuringTest {
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

    void optimisedDump(std::string_view query, std::string& dump) {
        StringRowSink sink;
        run(query, sink);

        for (const StringRowSink::Row& row : sink.getRows()) {
            if (row.front() == "db") {
                dump = row.back();
            }
        }
    }

    static bool contains(std::string_view text, std::string_view part) {
        return text.find(part) != std::string_view::npos;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(PathEdgeUniquenessTest, keepsTheTrailRuleInsideOnePath) {
    expectCount("MATCH (a)-[e*1..3]->(b) RETURN count(*)", "42");
}

TEST_F(PathEdgeUniquenessTest, excludesTheHopBeforeThePath) {
    expectCount("MATCH (a)-[e1]->(b)-[e*1..2]->(c) RETURN count(*)", "24");
}

TEST_F(PathEdgeUniquenessTest, excludesThePathFromTheHopAfterIt) {
    expectCount("MATCH (a)-[e*1..2]->(b)-[f]->(c) RETURN count(*)", "24");
}

TEST_F(PathEdgeUniquenessTest, excludesTheFirstPathFromTheSecond) {
    expectCount("MATCH (a)-[e*1..2]->(b)-[f*1..2]->(c) RETURN count(*)", "46");
}

TEST_F(PathEdgeUniquenessTest, distinctSearchExcludesTheHopBeforeIt) {
    std::string dump;
    optimisedDump("EXPLAIN (db) MATCH (a)-[e1]->(b)-[*1..2]->(c) WITH DISTINCT a, b, c RETURN count(*)", dump);
    ASSERT_TRUE(contains(dump, "distinct")) << dump;

    expectCount("MATCH (a)-[e1]->(b)-[*1..2]->(c) WITH DISTINCT a, b, c RETURN count(*)", "24");
}

TEST_F(PathEdgeUniquenessTest, distinctSearchExcludesThePathBeforeIt) {
    std::string dump;
    optimisedDump("EXPLAIN (db) MATCH (a)-[*1..2]->(b)-[*1..2]->(c) WITH DISTINCT a, b, c RETURN count(*)", dump);
    ASSERT_TRUE(contains(dump, "distinct")) << dump;

    expectCount("MATCH (a)-[*1..2]->(b)-[*1..2]->(c) WITH DISTINCT a, b, c RETURN count(*)", "43");
}

TEST_F(PathEdgeUniquenessTest, undirectedDistinctSearchClosesNoCycleThroughTheHopBeforeIt) {
    std::string dump;
    optimisedDump("EXPLAIN (db) MATCH (a)-[e1]-(b)-[*1..2]-(c) WITH DISTINCT a, b, c RETURN count(*)", dump);
    ASSERT_TRUE(contains(dump, "distinct")) << dump;

    expectCount("MATCH (a)-[e1]-(b)-[*1..2]-(c) WITH DISTINCT a, b, c RETURN count(*)", "94");

    StringRowSink sink;
    run("MATCH (a)-[e1]-(b)-[*1..2]-(c) WITH DISTINCT a, b, c WHERE b = c RETURN a.name, b.name ORDER BY a.name, b.name", sink);

    const std::vector<StringRowSink::Row> expected {{"Adam", "Remy"},
                                                    {"Bio", "Adam"},
                                                    {"Computers", "Remy"},
                                                    {"Cooking", "Adam"},
                                                    {"Eighties", "Remy"},
                                                    {"Ghosts", "Remy"}};
    EXPECT_EQ(sink.getRows(), expected);

    expectCount("MATCH (a)-[e1]-(b)-[*]-(c) WITH DISTINCT a, b, c RETURN count(*)", "160");

    StringRowSink unbounded;
    run("MATCH (a)-[e1]-(b)-[*]-(c) WITH DISTINCT a, b, c WHERE b = c RETURN a.name, b.name ORDER BY a.name, b.name", unbounded);
    EXPECT_EQ(unbounded.getRows(), expected);
}
