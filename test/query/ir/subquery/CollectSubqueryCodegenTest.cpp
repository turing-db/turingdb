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

// Which path each kind of COLLECT body compiles to. Both paths collect the same lists when
// both are right, so these read the op the codegen emitted and the loops the lowering opened.
class CollectSubqueryCodegenTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());
    }

protected:
    void explain(std::string_view query, StringRowSink& sink) {
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
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

    // The line of @param text holding the first occurrence of @param part
    static std::string_view lineOf(std::string_view text, std::string_view part) {
        const size_t position = text.find(part);
        if (position == std::string_view::npos) {
            return {};
        }

        const size_t lineStart = text.rfind('\n', position) + 1;
        const size_t lineEnd = text.find('\n', position);

        return text.substr(lineStart, lineEnd - lineStart);
    }

    static size_t occurrences(std::string_view text, std::string_view part) {
        size_t count = 0;

        for (size_t position = text.find(part); position != std::string_view::npos; position = text.find(part, position + 1)) {
            count++;
        }

        return count;
    }

    void explainCollect(std::string_view body, StringRowSink& sink) {
        explain(std::string("EXPLAIN (codegen, nl) MATCH (p:Person) RETURN p.name, COLLECT { ")
                    + std::string(body) + " }",
                sink);
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(CollectSubqueryCodegenTest, aMatchBodyCarriesTheRows) {
    StringRowSink sink;
    explainCollect("MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name", sink);

    const std::string_view codegen = dumpOf(sink, "codegen");
    EXPECT_TRUE(contains(codegen, "db.collect_subquery(")) << codegen;
    EXPECT_TRUE(contains(codegen, "carries_scope")) << codegen;
    EXPECT_TRUE(lineOf(codegen, "db.collect_subquery_yield").find(", {}") != std::string_view::npos) << codegen;

    const std::string_view nlProgram = dumpOf(sink, "nl");
    EXPECT_TRUE(contains(nlProgram, "nl.row_lists_buffer")) << nlProgram;
    EXPECT_TRUE(contains(nlProgram, "nl.row_lists_collect %state, tag")) << nlProgram;
    EXPECT_TRUE(contains(nlProgram, "nl.row_lists_result")) << nlProgram;
    EXPECT_FALSE(contains(nlProgram, "nl.each_row")) << nlProgram;
}

TEST_F(CollectSubqueryCodegenTest, aSortingBodyRunsOneRowAtATime) {
    StringRowSink sink;
    explainCollect("MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS name ORDER BY name", sink);

    const std::string_view codegen = dumpOf(sink, "codegen");
    EXPECT_TRUE(contains(codegen, "db.collect_subquery(")) << codegen;
    EXPECT_FALSE(contains(codegen, "carries_scope")) << codegen;

    const std::string_view nlProgram = dumpOf(sink, "nl");
    EXPECT_TRUE(contains(nlProgram, "nl.each_row")) << nlProgram;
    EXPECT_TRUE(contains(nlProgram, "nl.row_lists_collect")) << nlProgram;
    EXPECT_FALSE(contains(nlProgram, "nl.row_lists_collect %state, tag")) << nlProgram;
}

TEST_F(CollectSubqueryCodegenTest, aUnionBodyIsOneCollectOverTheUnion) {
    StringRowSink sink;
    explainCollect("MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS name "
                   "UNION MATCH (p)-[:KNOWS_WELL]->(k) RETURN k.name AS name",
                   sink);

    const std::string_view codegen = dumpOf(sink, "codegen");
    EXPECT_EQ(occurrences(codegen, "db.collect_subquery("), 1) << codegen;
    EXPECT_FALSE(contains(codegen, "carries_scope")) << codegen;
    EXPECT_TRUE(contains(codegen, "db.union")) << codegen;

    const std::string_view nlProgram = dumpOf(sink, "nl");
    EXPECT_TRUE(contains(nlProgram, "nl.each_row")) << nlProgram;
}

TEST_F(CollectSubqueryCodegenTest, aUnionAllBodyConcatenatesACollectPerBranch) {
    StringRowSink sink;
    explainCollect("MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS name "
                   "UNION ALL MATCH (p)-[:KNOWS_WELL]->(k) RETURN k.name AS name",
                   sink);

    const std::string_view codegen = dumpOf(sink, "codegen");
    EXPECT_EQ(occurrences(codegen, "db.collect_subquery("), 2) << codegen;
    EXPECT_EQ(occurrences(codegen, "carries_scope"), 2) << codegen;
    EXPECT_TRUE(contains(codegen, "db.concat")) << codegen;
    EXPECT_FALSE(contains(codegen, "db.union")) << codegen;
}

TEST_F(CollectSubqueryCodegenTest, aBodyOverNoRowInFlightTakesNoTag) {
    StringRowSink sink;
    explain("EXPLAIN (codegen, nl) RETURN COLLECT { MATCH (p:Person) RETURN p.name }", sink);

    const std::string_view codegen = dumpOf(sink, "codegen");
    EXPECT_TRUE(contains(codegen, "db.collect_subquery() carries_scope")) << codegen;

    const std::string_view nlProgram = dumpOf(sink, "nl");
    EXPECT_TRUE(contains(nlProgram, "nl.row_lists_buffer()")) << nlProgram;
    EXPECT_FALSE(contains(nlProgram, "nl.row_lists_collect %state, tag")) << nlProgram;
}

TEST_F(CollectSubqueryCodegenTest, aPatternComprehensionSharesTheAccumulator) {
    StringRowSink sink;
    explain("EXPLAIN (codegen, nl) MATCH (p:Person) RETURN [(p)-[:INTERESTED_IN]->(i) | i.name]", sink);

    const std::string_view codegen = dumpOf(sink, "codegen");
    EXPECT_TRUE(contains(codegen, "db.pattern_comprehension(")) << codegen;

    const std::string_view nlProgram = dumpOf(sink, "nl");
    EXPECT_TRUE(contains(nlProgram, "nl.row_lists_buffer")) << nlProgram;
    EXPECT_TRUE(contains(nlProgram, "nl.row_lists_collect %state, tag")) << nlProgram;
    EXPECT_TRUE(contains(nlProgram, "nl.row_lists_result")) << nlProgram;
}
