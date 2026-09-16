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

// The shape a CALL subquery compiles to: the region op codegen emits for each kind of
// body, and the loops the lowering runs it through
class CallSubqueryCodegenTest : public TuringTest {
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

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

// Every column in flight enters the region, imported or not - the two nodes and the
// anonymous edge between them - and comes back beside the returned one: three inputs,
// four results
TEST_F(CallSubqueryCodegenTest, aCarryingBodyTakesEveryColumnInFlightAndHandsThemBack) {
    StringRowSink sink;
    explain("EXPLAIN (codegen, nl) "
            "MATCH (p:Person)-[:INTERESTED_IN]->(i) "
            "CALL (p) { MATCH (p)-->(x) RETURN x } "
            "RETURN i.name, x",
            sink);

    const std::string_view codegen = dumpOf(sink, "codegen");
    EXPECT_TRUE(contains(codegen, "db.call_subquery(")) << codegen;
    EXPECT_TRUE(contains(codegen, "carries_scope")) << codegen;
    EXPECT_TRUE(contains(codegen, "db.subquery_yield")) << codegen;
    EXPECT_TRUE(contains(codegen,
                         ": (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>) -> "
                         "(!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>, "
                         "!db.column<!storage.node_id>)"))
        << codegen;

    // Inlined: the body's hop nests in the scan's loop and no loop over single rows opens
    const std::string_view nlProgram = dumpOf(sink, "nl");
    EXPECT_FALSE(contains(nlProgram, "nl.each_row")) << nlProgram;
    EXPECT_FALSE(contains(nlProgram, "nl.cross_product")) << nlProgram;
}

TEST_F(CallSubqueryCodegenTest, anAggregatingBodyRunsOneRowAtATime) {
    StringRowSink sink;
    explain("EXPLAIN (codegen, nl) "
            "MATCH (p:Person) "
            "CALL (p) { MATCH (p)-->(x) RETURN count(x) AS c } "
            "RETURN p.name, c",
            sink);

    const std::string_view codegen = dumpOf(sink, "codegen");
    EXPECT_TRUE(contains(codegen, "db.call_subquery(")) << codegen;
    EXPECT_FALSE(contains(codegen, "carries_scope")) << codegen;

    // The row loop, the tally reset inside it, and the row crossed with the one count row
    const std::string_view nlProgram = dumpOf(sink, "nl");
    EXPECT_TRUE(contains(nlProgram, "nl.each_row")) << nlProgram;
    EXPECT_TRUE(contains(nlProgram, "nl.cross_product")) << nlProgram;
    EXPECT_TRUE(contains(nlProgram, "nl.count_result")) << nlProgram;
}

TEST_F(CallSubqueryCodegenTest, aUnitBodyHasNoResult) {
    StringRowSink sink;
    explain("EXPLAIN (codegen) "
            "MATCH (p:Person) "
            "CALL (p) { SET p.visited = true } "
            "RETURN count(p)",
            sink);

    const std::string_view codegen = dumpOf(sink, "codegen");
    EXPECT_TRUE(contains(codegen, "db.call_subquery(")) << codegen;
    EXPECT_TRUE(contains(codegen, ") unit ")) << codegen;
    EXPECT_TRUE(contains(codegen, "-> ()")) << codegen;
}

TEST_F(CallSubqueryCodegenTest, anOptionalBodyDrainsThroughTheOptionalAccumulator) {
    StringRowSink sink;
    explain("EXPLAIN (codegen, nl) "
            "MATCH (p:Person) "
            "OPTIONAL CALL (p) { MATCH (p)-[:KNOWS_WELL]->(k) RETURN k } "
            "RETURN p.name, k",
            sink);

    const std::string_view codegen = dumpOf(sink, "codegen");
    EXPECT_TRUE(contains(codegen, "carries_scope optional")) << codegen;

    const std::string_view nlProgram = dumpOf(sink, "nl");
    EXPECT_TRUE(contains(nlProgram, "nl.optional_buffer")) << nlProgram;
    EXPECT_TRUE(contains(nlProgram, "nl.optional_collect")) << nlProgram;
    EXPECT_TRUE(contains(nlProgram, "nl.optional_drain")) << nlProgram;
}
