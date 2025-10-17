#include "File.h"

#include <sstream>

#include "CompilerException.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

#include "SystemManager.h"
#include "TuringDB.h"
#include "PlanGraphDebug.h"
#include "Graph.h"
#include "SimpleGraph.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "PlanGraphGenerator.h"
#include "PlanGraph.h"
#include "CypherAnalyzer.h"
#include "CypherParser.h"
#include "CypherAST.h"

using namespace db;
using namespace db::v2;
using namespace turing::test;

class PlanGenTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _graph = _env->getSystemManager().createGraph("simpledb");
        SimpleGraph::createSimpleGraph(_graph);
    }

    void generatePlanGraph(std::string_view query, std::ostream& out) {
        const Transaction transaction = _graph->openTransaction();
        const GraphView view = transaction.viewGraph();

        CypherAST ast(query);
        CypherParser parser(&ast);
        CypherAnalyzer analyzer(&ast, view);
        PlanGraphGenerator planGen(ast, view);

        try {
            parser.parse(query);
        } catch (const CompilerException& e) {
            fmt::println(out, "PARSE ERROR");
            fmt::println(out, "{}", e.what());
            return;
        }

        try {
            analyzer.analyze();
        } catch (const CompilerException& e) {
            fmt::println(out, "ANALYZE ERROR");
            fmt::println(out, "{}", e.what());
            return;
        }

        try {
            planGen.generate(ast.queries().front());
        } catch (const CompilerException& e) {
            fmt::println(out, "PLAN ERROR");
            fmt::println(out, "{}", e.what());
            return;
        }

        const PlanGraph& planGraph = planGen.getPlanGraph();
        PlanGraphDebug::dumpMermaidContent(out, view, planGraph);
    }

    void runDirectory(const fs::Path& dir) {
        const auto queryFiles = dir.listDir();
        ASSERT_TRUE(queryFiles);

        std::string fileContent;
        std::string generatedPlan;
        std::stringstream out;

        for (const auto& queryFilePath : *queryFiles) {
            fmt::print("- Test file: {}\n", queryFilePath.get());
            out.str("");

            const auto queryFile = fs::File::open(queryFilePath);
            ASSERT_TRUE(queryFile);

            const size_t fileSize = queryFile->getInfo()._size;
            fileContent.resize(fileSize);

            ASSERT_TRUE(queryFile->read(fileContent.data(), fileSize));

            std::string_view content {fileContent.data(), fileSize};

            constexpr std::string_view queryDelimiter = "/// QUERY";
            constexpr std::string_view resDelimiter = "/// RESULT";

            // Find query delimiter
            const size_t queryDelimiterPos = content.find(queryDelimiter);
            ASSERT_NE(queryDelimiterPos, std::string_view::npos);

            // Remove the query delimiter
            std::string_view queryStr = content.substr(queryDelimiter.size() + 1);

            // Find result delimiter
            const size_t resDelimiterPos = queryStr.find(resDelimiter);
            ASSERT_NE(resDelimiterPos, std::string_view::npos);

            std::string_view resStr = queryStr.substr(resDelimiterPos + resDelimiter.size() + 1);
            queryStr = queryStr.substr(0, resDelimiterPos);

            // Checking if generated result is the same
            generatePlanGraph(queryStr, out);
            generatedPlan = out.str();

            if (resStr != generatedPlan) {
                fmt::println("== Unit test failed in file: {}", queryFilePath.get());
                fmt::println("== Generated result differ for query:\n{}", queryStr);
                fmt::println("== Expected:\n{}", resStr);
                fmt::println("== Actual:\n{}", generatedPlan);
                EXPECT_FALSE(true);
            }
        }
    }

protected:
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
};

TEST_F(PlanGenTest, SuccessReads) {
    runDirectory(fs::Path {SUCCESS_READ_QUERIES_DIR});
}

TEST_F(PlanGenTest, SuccessWrites) {
    runDirectory(fs::Path {SUCCESS_WRITE_QUERIES_DIR});
}

TEST_F(PlanGenTest, FailReads) {
    runDirectory(fs::Path {FAIL_READ_QUERIES_DIR});
}

TEST_F(PlanGenTest, FailWrites) {
    runDirectory(fs::Path {FAIL_WRITE_QUERIES_DIR});
}

int main(int argc, char** argv) {
    return turingTestMain(argc, argv, [] { testing::GTEST_FLAG(repeat) = 2; });
}
