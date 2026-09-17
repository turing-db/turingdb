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

// count over a path tallies the rows its build ran over, and the build goes away
class CountPathRowsTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());
    }

protected:
    void run(std::string_view query, StringRowSink& sink) {
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();
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

TEST_F(CountPathRowsTest, doesNotBuildANamedPathToCountIt) {
    std::string dump;
    optimisedDump("EXPLAIN (db) MATCH p = (n:Person)-[e]->+(m:Person) RETURN count(p)", dump);

    EXPECT_FALSE(contains(dump, "db.make_path")) << dump;
    EXPECT_TRUE(contains(dump, "db.count")) << dump;
    EXPECT_TRUE(contains(dump, "rows")) << dump;
}

// A relationship variable is already the handle column, so its count builds nothing
TEST_F(CountPathRowsTest, countsAWalkOffItsHandles) {
    std::string dump;
    optimisedDump("EXPLAIN (db) MATCH (n:Person)-[e]->+(m:Person) RETURN count(e)", dump);

    EXPECT_FALSE(contains(dump, "db.expand_path")) << dump;
    EXPECT_TRUE(contains(dump, "db.count(%3) : (!db.column<!storage.path_ref>)")) << dump;
}

TEST_F(CountPathRowsTest, buildsThePathACountDeduplicates) {
    std::string dump;
    optimisedDump("EXPLAIN (db) MATCH p = (n:Person)-[e]->+(m:Person) RETURN count(DISTINCT p)", dump);

    EXPECT_TRUE(contains(dump, "db.make_path")) << dump;
}

TEST_F(CountPathRowsTest, buildsThePathAQueryReturnsBesideTheCount) {
    std::string dump;
    optimisedDump("EXPLAIN (db) MATCH p = (n:Person)-[e]->+(m:Person) RETURN p, count(p)", dump);

    EXPECT_TRUE(contains(dump, "db.make_path")) << dump;
}

TEST_F(CountPathRowsTest, countsEveryTrailTheWalkFound) {
    StringRowSink counted;
    run("MATCH p = (n:Person)-[e]->+(m:Person) RETURN count(p)", counted);
    EXPECT_EQ(counted.getRows(), (std::vector<StringRowSink::Row> {{"10"}}));

    StringRowSink walked;
    run("MATCH (n:Person)-[e]->+(m:Person) RETURN count(e)", walked);
    EXPECT_EQ(walked.getRows(), (std::vector<StringRowSink::Row> {{"10"}}));
}
