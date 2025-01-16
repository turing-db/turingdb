#include <gtest/gtest.h>

#include "Graph.h"
#include "GraphView.h"
#include "GraphReader.h"
#include "GraphReport.h"
#include "DataPartBuilder.h"
#include "EdgeView.h"
#include "FileUtils.h"
#include "JobSystem.h"
#include "LogSetup.h"
#include "Neo4j/Neo4JParserConfig.h"
#include "Neo4jImporter.h"
#include "PerfStat.h"
#include "Time.h"

using namespace db;

class Neo4jImporterTest : public ::testing::Test {
protected:
    void SetUp() override {
        const testing::TestInfo* const testInfo =
            testing::UnitTest::GetInstance()->current_test_info();

        _outDir = testInfo->test_suite_name();
        _outDir += "_";
        _outDir += testInfo->name();
        _outDir += ".out";
        _logPath = FileUtils::Path(_outDir) / "log";
        _perfPath = FileUtils::Path(_outDir) / "perf";

        if (FileUtils::exists(_outDir)) {
            FileUtils::removeDirectory(_outDir);
        }
        FileUtils::createDirectory(_outDir);

        LogSetup::setupLogFileBacked(_logPath.string());
        PerfStat::init(_perfPath);

        _graph = std::make_unique<Graph>();
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->initialize();
    }

    void TearDown() override {
        _jobSystem->terminate();
        PerfStat::destroy();
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph {nullptr};
    std::string _outDir;
    FileUtils::Path _logPath;
    FileUtils::Path _perfPath;
};

TEST_F(Neo4jImporterTest, Simple) {
    {
        auto builder1 = _graph->newPartWriter();
        builder1->addNode(LabelSet::fromList({1})); // 0
        builder1->addNode(LabelSet::fromList({0})); // 1
        builder1->addNode(LabelSet::fromList({1})); // 2
        builder1->addNode(LabelSet::fromList({2})); // 3
        builder1->addEdge(0, 0, 1);
        builder1->commit(*_jobSystem);

        auto builder2 = _graph->newPartWriter();
        builder2->addEdge(2, 1, 2);
        EntityID id1 = builder2->addNode(LabelSet::fromList({0}));
        builder2->addNodeProperty<types::String>(id1, 0, "test1");

        builder2->addEdge(2, 0, 1);
        EntityID id2 = builder2->addNode(LabelSet::fromList({0}));
        builder2->addNodeProperty<types::String>(id2, 0, "test2");
        builder2->addNodeProperty<types::String>(2, 0, "test3");
        const EdgeRecord& edge= builder2->addEdge(4, 3, 4);
        builder2->addEdgeProperty<types::String>(edge, 0, "Edge property test");
        builder2->addNode(LabelSet::fromList({0}));
        builder2->addNode(LabelSet::fromList({0}));
        builder2->addNode(LabelSet::fromList({0}));
        builder2->addNode(LabelSet::fromList({0}));
        builder2->addNode(LabelSet::fromList({1}));
        builder2->addNode(LabelSet::fromList({1}));
        builder2->addNode(LabelSet::fromList({1}));
        builder2->addEdge(0, 3, 4);
        builder2->commit(*_jobSystem);
    }

    const auto view = _graph->view();
    const auto reader = view.read();
    std::cout << "All out edges: ";
    for (const auto& edge : reader.scanOutEdges()) {
        std::cout << edge._edgeID.getValue()
                  << edge._nodeID.getValue()
                  << edge._otherID.getValue()
                  << std::endl;
        auto view = reader.getEdgeView(edge._edgeID);
        std::cout << "  Has " << view.properties().getCount() << " properties" << std::endl;
    }
    std::cout << std::endl;

    auto it = reader.scanNodeProperties<types::String>(0).begin();
    for (; it.isValid(); it.next()) {
        std::cout << it.getCurrentNodeID().getValue() << ": " << it.get() << std::endl;
    }
}

TEST_F(Neo4jImporterTest, General) {
    JobSystem jobSystem;
    jobSystem.initialize();
    auto t0 = Clock::now();
    auto t1 = Clock::now();

    const std::string turingHome = std::getenv("TURING_HOME");
    const FileUtils::Path jsonDir = FileUtils::Path {turingHome} / "neo4j" / "cyber-security-db";
    t0 = Clock::now();
    const bool res = Neo4jImporter::importJsonDir(jobSystem,
                                                  _graph.get(),
                                                  db::json::neo4j::Neo4JParserConfig::nodeCountLimit,
                                                  db::json::neo4j::Neo4JParserConfig::edgeCountLimit,
                                                  {
                                                      ._jsonDir = jsonDir,
                                                      ._workDir = _outDir,
                                                  });
    t1 = Clock::now();
    ASSERT_TRUE(res);
    std::cout << "Parsing: " << duration<Microseconds>(t0, t1) << " us" << std::endl;

    const auto view = _graph->view();
    const auto reader = view.read();
    std::stringstream report;
    GraphReport::getReport(reader, report);
    std::cout << report.view() << std::endl;
}
