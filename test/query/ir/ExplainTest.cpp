#include <gtest/gtest.h>

#include <algorithm>
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

class ExplainTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());
    }

protected:
    void run(std::string_view query, StringRowSink& sink, QueryStatus& status) {
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);
    }

    void explain(std::string_view query, StringRowSink& sink) {
        QueryStatus status;
        run(query, sink, status);

        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();
        EXPECT_EQ(sink.getNames(), (std::vector<std::string> {"stage", "dump"}));
    }

    void expectRejected(std::string_view query, std::string_view message) {
        StringRowSink sink;
        QueryStatus status;
        run(query, sink, status);

        EXPECT_FALSE(status.isOk()) << "query: " << query;
        EXPECT_TRUE(contains(status.getError(), message)) << "query: " << query
                                                          << "\nerror: " << status.getError();
    }

    static void collectStages(const StringRowSink& sink, std::vector<std::string>& stages) {
        for (const StringRowSink::Row& row : sink.getRows()) {
            stages.push_back(row.front());
        }
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

TEST_F(ExplainTest, reportsTheDbAndTheNlProgramByDefault) {
    StringRowSink sink;
    explain("EXPLAIN MATCH (n) RETURN n", sink);

    std::vector<std::string> stages;
    collectStages(sink, stages);
    EXPECT_EQ(stages, (std::vector<std::string> {"db", "nl"}));

    const std::string_view dbProgram = dumpOf(sink, "db");
    EXPECT_TRUE(contains(dbProgram, "func.func @main")) << dbProgram;
    EXPECT_TRUE(contains(dbProgram, "db.scan_nodes")) << dbProgram;

    const std::string_view nlProgram = dumpOf(sink, "nl");
    EXPECT_TRUE(contains(nlProgram, "nl.scan_nodes")) << nlProgram;
    EXPECT_TRUE(contains(nlProgram, "nl.for")) << nlProgram;
}

TEST_F(ExplainTest, reportsOnlyTheStagesTheOptionsName) {
    StringRowSink sink;
    explain("EXPLAIN (nl) MATCH (n) RETURN n", sink);

    std::vector<std::string> stages;
    collectStages(sink, stages);
    EXPECT_EQ(stages, (std::vector<std::string> {"nl"}));
}

TEST_F(ExplainTest, readsTheOptionsWhateverTheirCase) {
    StringRowSink sink;
    explain("EXPLAIN (NL) MATCH (n) RETURN n", sink);

    std::vector<std::string> stages;
    collectStages(sink, stages);
    EXPECT_EQ(stages, (std::vector<std::string> {"nl"}));
}

TEST_F(ExplainTest, reportsEveryPassOfABracketedList) {
    StringRowSink sink;
    explain("EXPLAIN (after [fuse_scan_by_label, trim_unread_columns]) MATCH (n:Person) RETURN n.name", sink);

    std::vector<std::string> stages;
    collectStages(sink, stages);
    EXPECT_EQ(stages, (std::vector<std::string> {"after fuse_scan_by_label", "after trim_unread_columns"}));
}

TEST_F(ExplainTest, reportsTheModuleEnteringAndLeavingThePipeline) {
    StringRowSink sink;
    explain("EXPLAIN (codegen, db) MATCH (n:Person) RETURN n.name", sink);

    std::vector<std::string> stages;
    collectStages(sink, stages);
    EXPECT_EQ(stages, (std::vector<std::string> {"codegen", "db"}));

    const std::string_view codegen = dumpOf(sink, "codegen");
    EXPECT_TRUE(contains(codegen, "db.scan_nodes")) << codegen;
    EXPECT_FALSE(contains(codegen, "db.scan_nodes_by_label")) << codegen;

    const std::string_view optimised = dumpOf(sink, "db");
    EXPECT_TRUE(contains(optimised, "db.scan_nodes_by_label")) << optimised;
}

TEST_F(ExplainTest, reportsEveryPassThatChangedTheModule) {
    StringRowSink sink;
    explain("EXPLAIN (passes) MATCH (n:Person) RETURN n.name", sink);

    std::vector<std::string> stages;
    collectStages(sink, stages);

    ASSERT_FALSE(stages.empty());
    EXPECT_EQ(stages.front(), "codegen");
    EXPECT_NE(std::find(stages.begin(), stages.end(), "after fuse_scan_by_label"), stages.end());

    // The passes that rewrite nothing here report nothing
    EXPECT_EQ(std::find(stages.begin(), stages.end(), "after fuse_unwind_equality"), stages.end());
    EXPECT_EQ(std::find(stages.begin(), stages.end(), "after fuse_scan_edges"), stages.end());
}

TEST_F(ExplainTest, reportsBothSidesOfANamedPass) {
    StringRowSink sink;
    explain("EXPLAIN (around fuse_scan_by_label) MATCH (n:Person) RETURN n.name", sink);

    std::vector<std::string> stages;
    collectStages(sink, stages);
    EXPECT_EQ(stages, (std::vector<std::string> {"before fuse_scan_by_label", "after fuse_scan_by_label"}));

    EXPECT_FALSE(contains(dumpOf(sink, "before fuse_scan_by_label"), "db.scan_nodes_by_label"));
    EXPECT_TRUE(contains(dumpOf(sink, "after fuse_scan_by_label"), "db.scan_nodes_by_label"));
}

TEST_F(ExplainTest, reportsANamedPassThatChangedNothing) {
    StringRowSink sink;
    explain("EXPLAIN (after fuse_scan_edges) MATCH (n:Person) RETURN n.name", sink);

    std::vector<std::string> stages;
    collectStages(sink, stages);
    EXPECT_EQ(stages, (std::vector<std::string> {"after fuse_scan_edges"}));
}

TEST_F(ExplainTest, reportsTheDependencyGraphOfEveryQueryPart) {
    StringRowSink sink;
    explain("EXPLAIN (ast, vdg) MATCH (a)-->(b) WITH b MATCH (b)-->(c) RETURN c", sink);

    std::vector<std::string> stages;
    collectStages(sink, stages);
    EXPECT_EQ(stages, (std::vector<std::string> {"ast", "vdg 1", "vdg 2"}));

    const std::string_view firstPart = dumpOf(sink, "vdg 1");
    EXPECT_TRUE(contains(firstPart, "flowchart")) << firstPart;
    EXPECT_TRUE(contains(firstPart, "getout")) << firstPart;
}

TEST_F(ExplainTest, explainsASystemStatementWithoutRunningIt) {
    StringRowSink sink;
    explain("EXPLAIN LOAD GRAPH missingGraph", sink);

    std::vector<std::string> stages;
    collectStages(sink, stages);
    EXPECT_EQ(stages, (std::vector<std::string> {"db", "nl"}));

    EXPECT_TRUE(contains(dumpOf(sink, "db"), "db.load_graph"));
    EXPECT_TRUE(contains(dumpOf(sink, "nl"), "nl.load_graph"));
}

TEST_F(ExplainTest, rejectsAnUnknownOption) {
    expectRejected("EXPLAIN (bogus) MATCH (n) RETURN n", "Unknown EXPLAIN option 'bogus'");
}

TEST_F(ExplainTest, rejectsAPassThePipelineDoesNotRun) {
    expectRejected("EXPLAIN (after bogus_pass) MATCH (n) RETURN n", "Unknown EXPLAIN pass 'bogus_pass'");
    expectRejected("EXPLAIN (after bogus_pass) MATCH (n) RETURN n", "fuse_scan_by_label");
}
