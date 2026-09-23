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

// Which of the two paths each kind of EXISTS body compiles to. The rows both paths produce
// are the same when both are right, so the answers alone do not say which one ran: these
// read the op the codegen emitted and the loops the lowering opened.
class ExistsSubqueryCodegenTest : public TuringTest {
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

    // A body that keeps the rows it was given paired with what it made of them: the op
    // carries the scope, the tag rides through it, and no loop over single rows opens
    void expectCarriesTheRows(std::string_view body) {
        StringRowSink sink;
        explain(std::string("EXPLAIN (codegen, nl) MATCH (p:Person) WHERE EXISTS { ")
                    + std::string(body) + " } RETURN p.name",
                sink);

        const std::string_view codegen = dumpOf(sink, "codegen");
        EXPECT_TRUE(contains(codegen, "db.exists_subquery(")) << codegen;
        EXPECT_TRUE(contains(codegen, "carries_scope")) << codegen;
        EXPECT_TRUE(contains(codegen, "db.exists_yield")) << codegen;

        const std::string_view nlProgram = dumpOf(sink, "nl");
        EXPECT_TRUE(contains(nlProgram, "nl.exists_buffer")) << nlProgram;
        EXPECT_TRUE(contains(nlProgram, "nl.exists_mark")) << nlProgram;
        EXPECT_TRUE(contains(nlProgram, "nl.exists_result")) << nlProgram;
        EXPECT_FALSE(contains(nlProgram, "nl.each_row")) << nlProgram;
    }

    // A body that cannot: the op carries no scope, it takes no tag, and the lowering opens
    // the loop over the step's rows that runs it one at a time
    void expectRunsOneRowAtATime(std::string_view body) {
        StringRowSink sink;
        explain(std::string("EXPLAIN (codegen, nl) MATCH (p:Person) WHERE EXISTS { ")
                    + std::string(body) + " } RETURN p.name",
                sink);

        const std::string_view codegen = dumpOf(sink, "codegen");
        EXPECT_TRUE(contains(codegen, "db.exists_subquery(")) << codegen;
        EXPECT_FALSE(contains(codegen, "carries_scope")) << codegen;

        const std::string_view nlProgram = dumpOf(sink, "nl");
        EXPECT_TRUE(contains(nlProgram, "nl.each_row")) << nlProgram;
        EXPECT_TRUE(contains(nlProgram, "nl.exists_buffer")) << nlProgram;
        EXPECT_TRUE(contains(nlProgram, "nl.exists_mark")) << nlProgram;
        EXPECT_TRUE(contains(nlProgram, "nl.exists_result")) << nlProgram;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(ExistsSubqueryCodegenTest, aPatternBodyCarriesTheRows) {
    expectCarriesTheRows("(p)-[:KNOWS_WELL]->(k)");
}

// A plain WITH projects the rows on rather than reducing them, so the tag rides past it
TEST_F(ExistsSubqueryCodegenTest, aPlainBarrierBodyCarriesTheRows) {
    expectCarriesTheRows("MATCH (p)-[:INTERESTED_IN]->(i) WITH i WHERE i.isReal = true RETURN i");
}

TEST_F(ExistsSubqueryCodegenTest, anAggregatingBodyRunsOneRowAtATime) {
    expectRunsOneRowAtATime("MATCH (p)-[:INTERESTED_IN]->(i) WITH count(i) AS interests WHERE interests > 1 RETURN interests");
}

TEST_F(ExistsSubqueryCodegenTest, aDedupingBodyRunsOneRowAtATime) {
    expectRunsOneRowAtATime("MATCH (p)-[:INTERESTED_IN]->(i) RETURN DISTINCT i.name AS interest");
}

TEST_F(ExistsSubqueryCodegenTest, aSortingBodyRunsOneRowAtATime) {
    expectRunsOneRowAtATime("MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS interest ORDER BY interest");
}

TEST_F(ExistsSubqueryCodegenTest, aSkippingBodyRunsOneRowAtATime) {
    expectRunsOneRowAtATime("MATCH (p)-[:INTERESTED_IN]->(i) RETURN i SKIP 1");
}

TEST_F(ExistsSubqueryCodegenTest, aLimitingBodyRunsOneRowAtATime) {
    expectRunsOneRowAtATime("MATCH (p)-[:INTERESTED_IN]->(i) RETURN i LIMIT 1");
}

// The cut a MATCH inside the body carries counts as much as the RETURN's
TEST_F(ExistsSubqueryCodegenTest, aMatchCutInTheBodyRunsItOneRowAtATime) {
    expectRunsOneRowAtATime("MATCH (p)-[:INTERESTED_IN]->(i) SKIP 1 RETURN i");
}

// Nothing is in flight, so the body joins onto the single empty row and takes no tag
TEST_F(ExistsSubqueryCodegenTest, aBodyOverNoRowInFlightTakesNoTag) {
    StringRowSink sink;
    explain("EXPLAIN (codegen, nl) RETURN EXISTS { (p:Person) }", sink);

    const std::string_view codegen = dumpOf(sink, "codegen");
    EXPECT_TRUE(contains(codegen, "db.exists_subquery() carries_scope")) << codegen;
    EXPECT_TRUE(contains(codegen, "-> !db.column<!storage.bool>")) << codegen;

    const std::string_view nlProgram = dumpOf(sink, "nl");
    EXPECT_TRUE(contains(nlProgram, "nl.exists_buffer() : {}")) << nlProgram;
    EXPECT_FALSE(contains(nlProgram, "nl.each_row")) << nlProgram;
}
