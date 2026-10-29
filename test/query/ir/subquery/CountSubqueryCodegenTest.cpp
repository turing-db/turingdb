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

// Which path each kind of COUNT body compiles to. Both paths count the same rows when both
// are right, so these read the op the codegen emitted and the loops the lowering opened.
class CountSubqueryCodegenTest : public TuringTest {
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

    // The SSA name the line holding @param op binds
    static std::string_view resultOf(std::string_view text, std::string_view op) {
        const std::string_view line = lineOf(text, op);
        const size_t nameStart = line.find('%');
        if (nameStart == std::string_view::npos) {
            return {};
        }

        return line.substr(nameStart, line.find(' ', nameStart) - nameStart);
    }

    static size_t occurrences(std::string_view text, std::string_view part) {
        size_t count = 0;

        for (size_t position = text.find(part); position != std::string_view::npos; position = text.find(part, position + 1)) {
            count++;
        }

        return count;
    }

    void explainCount(std::string_view body, StringRowSink& sink) {
        explain(std::string("EXPLAIN (codegen, nl) MATCH (p:Person) RETURN p.name, COUNT { ")
                    + std::string(body) + " }",
                sink);
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(CountSubqueryCodegenTest, aPatternBodyCarriesTheRows) {
    StringRowSink sink;
    explainCount("(p)-[:INTERESTED_IN]->(i)", sink);

    const std::string_view codegen = dumpOf(sink, "codegen");
    EXPECT_TRUE(contains(codegen, "db.count_subquery(")) << codegen;
    EXPECT_TRUE(contains(codegen, "carries_scope")) << codegen;
    EXPECT_TRUE(contains(codegen, "-> !db.column<ui64>")) << codegen;

    const std::string_view nlProgram = dumpOf(sink, "nl");
    EXPECT_TRUE(contains(nlProgram, "nl.count_subquery_buffer")) << nlProgram;
    EXPECT_TRUE(contains(nlProgram, "nl.count_subquery_tally")) << nlProgram;
    EXPECT_TRUE(contains(nlProgram, "nl.count_subquery_result")) << nlProgram;
    EXPECT_FALSE(contains(nlProgram, "nl.each_row")) << nlProgram;
}

// The loop over single rows hands the body each row of the scan with its tag, the tally
// counts toward that row, and the counts are read over the scan's chunk once it closes
TEST_F(CountSubqueryCodegenTest, aLimitingBodyRunsOneRowAtATime) {
    StringRowSink sink;
    explainCount("MATCH (p)-[:INTERESTED_IN]->(i) RETURN i LIMIT 1", sink);

    const std::string_view codegen = dumpOf(sink, "codegen");
    EXPECT_TRUE(contains(codegen, "db.count_subquery(")) << codegen;
    EXPECT_FALSE(contains(codegen, "carries_scope")) << codegen;

    const std::string_view nlProgram = dumpOf(sink, "nl");
    EXPECT_TRUE(contains(nlProgram, "nl.each_row{%arg0, %tag} : {!nl.chunk<!storage.node_id>, !nl.chunk<ui64>}"))
        << nlProgram;
    EXPECT_TRUE(contains(nlProgram, "nl.count_subquery_tally %state row %arg2 rows(")) << nlProgram;
    EXPECT_TRUE(contains(nlProgram, "nl.count_subquery_result(%state, %arg0)")) << nlProgram;
}

TEST_F(CountSubqueryCodegenTest, aUnionBodyIsOneCountOverTheUnion) {
    StringRowSink sink;
    explainCount("MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS name "
                 "UNION MATCH (p)-[:KNOWS_WELL]->(k) RETURN k.name AS name",
                 sink);

    const std::string_view codegen = dumpOf(sink, "codegen");
    EXPECT_EQ(occurrences(codegen, "db.count_subquery("), 1) << codegen;
    EXPECT_FALSE(contains(codegen, "carries_scope")) << codegen;
    EXPECT_TRUE(contains(codegen, "db.union")) << codegen;

    const std::string_view nlProgram = dumpOf(sink, "nl");
    EXPECT_TRUE(contains(nlProgram, "nl.each_row")) << nlProgram;
}

TEST_F(CountSubqueryCodegenTest, aUnionAllBodyIsTheSumOfACountPerBranch) {
    StringRowSink sink;
    explainCount("MATCH (p)-[:INTERESTED_IN]->(i) UNION ALL MATCH (p)-[:KNOWS_WELL]->(k)", sink);

    const std::string_view codegen = dumpOf(sink, "codegen");
    EXPECT_EQ(occurrences(codegen, "db.count_subquery("), 2) << codegen;
    EXPECT_EQ(occurrences(codegen, "carries_scope"), 2) << codegen;
    EXPECT_TRUE(contains(codegen, "db.add")) << codegen;
    EXPECT_FALSE(contains(codegen, "db.union")) << codegen;
}

TEST_F(CountSubqueryCodegenTest, aBodyOverNoRowInFlightTakesNoTag) {
    StringRowSink sink;
    explain("EXPLAIN (codegen, nl) RETURN COUNT { (p:Person) }", sink);

    const std::string_view codegen = dumpOf(sink, "codegen");
    EXPECT_TRUE(contains(codegen, "db.count_subquery() carries_scope")) << codegen;
    EXPECT_TRUE(contains(codegen, "db.count_subquery_yield {")) << codegen;

    const std::string_view nlProgram = dumpOf(sink, "nl");
    EXPECT_TRUE(contains(nlProgram, "nl.count_subquery_buffer()")) << nlProgram;
    EXPECT_TRUE(contains(nlProgram, "rows(")) << nlProgram;
}

// Every row of the step is answered for before the rows are emitted, so the budget stops
// the scan and leaves the loop over single rows whole
TEST_F(CountSubqueryCodegenTest, anOuterLimitBudgetsTheScanAndNotTheLoopOverSingleRows) {
    StringRowSink sink;
    explain("EXPLAIN (codegen, nl) MATCH (p:Person) "
            "RETURN p.name, COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i LIMIT 1 } LIMIT 3",
            sink);

    const std::string_view nlProgram = dumpOf(sink, "nl");
    const std::string scanLoop = "in " + std::string(resultOf(nlProgram, "nl.scan_nodes_by_label")) + " limit ";
    const std::string rowLoop = "in " + std::string(resultOf(nlProgram, "nl.each_row")) + " : ";
    EXPECT_TRUE(contains(nlProgram, scanLoop)) << nlProgram;
    EXPECT_TRUE(contains(nlProgram, rowLoop)) << nlProgram;
}

// The tally reads the tag alone, so the hops of the body carry nothing else
TEST_F(CountSubqueryCodegenTest, aTaggedBodyYieldsItsTagAlone) {
    StringRowSink sink;
    explainCount("(p)-[:INTERESTED_IN]->(i)<-[:INTERESTED_IN]-(q)", sink);

    const std::string_view codegen = dumpOf(sink, "codegen");
    EXPECT_TRUE(lineOf(codegen, "db.count_subquery_yield").ends_with(", {}")) << codegen;

    const std::string_view nlProgram = dumpOf(sink, "nl");
    const std::string_view secondHop = lineOf(nlProgram, "nl.get_in_edges_by_type");
    EXPECT_TRUE(secondHop.ends_with("}) : !nl.chunk<ui64>")) << nlProgram;
}
