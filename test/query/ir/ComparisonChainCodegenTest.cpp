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

// A comparison chain hands its middle operand to both of its sides, so a walk of the
// expression meets that one node twice and everything it emits has to be emitted once
class ComparisonChainCodegenTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());
    }

protected:
    void explain(std::string_view query, StringRowSink& sink) {
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

    static std::string_view dumpOf(const StringRowSink& sink, std::string_view stage) {
        for (const StringRowSink::Row& row : sink.getRows()) {
            if (row.front() == stage) {
                return row.back();
            }
        }

        return {};
    }

    static bool contains(std::string_view text, std::string_view part) {
        return text.find(part) != std::string_view::npos;
    }

    static size_t occurrences(std::string_view text, std::string_view part) {
        size_t count = 0;

        for (size_t at = text.find(part); at != std::string_view::npos; at = text.find(part, at + part.size())) {
            count++;
        }

        return count;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(ComparisonChainCodegenTest, anAggregateStandingInAChainReducesOneColumn) {
    StringRowSink sink;
    explain("EXPLAIN (codegen) MATCH (n) RETURN n.name, 1 < count(n) < 5", sink);

    const std::string_view codegen = dumpOf(sink, "codegen");

    EXPECT_TRUE(contains(codegen, "aggregates [count] :")) << codegen;
    EXPECT_FALSE(contains(codegen, "count, count")) << codegen;
    EXPECT_TRUE(contains(codegen, ":2 = db.group_aggregate")) << codegen;
}

TEST_F(ComparisonChainCodegenTest, bothSidesOfAChainReadOneFilterAndOnePropertyColumn) {
    StringRowSink sink;
    explain("EXPLAIN (codegen) MATCH (n) WHERE 30 < n.age <= 32 RETURN n.name", sink);

    const std::string_view codegen = dumpOf(sink, "codegen");

    EXPECT_EQ(1U, occurrences(codegen, "db.filter")) << codegen;
    EXPECT_EQ(1U, occurrences(codegen, "\"age\"")) << codegen;
}
