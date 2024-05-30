#include <gtest/gtest.h>
#include <numeric>
#include <regex>
#include <span>
#include <thread>

#include "DB.h"
#include "DBAccess.h"
#include "DBReport.h"
#include "DataBuffer.h"
#include "EdgeView.h"
#include "FileUtils.h"
#include "JobSystem.h"
#include "LogSetup.h"
#include "Neo4j/ParserConfig.h"
#include "Neo4jImporter.h"
#include "PerfStat.h"
#include "ScanEdgesIterator.h"
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

        _db = std::make_unique<DB>();
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->initialize();
    }

    void TearDown() override {
        _jobSystem->terminate();
        PerfStat::destroy();
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<DB> _db = nullptr;
    std::string _outDir;
    FileUtils::Path _logPath;
    FileUtils::Path _perfPath;
};

TEST_F(Neo4jImporterTest, Simple) {
    {
        auto buf1 = _db->access().newDataBuffer();
        buf1->addNode(LabelSet::fromList({1})); // 0
        buf1->addNode(LabelSet::fromList({0})); // 1
        buf1->addNode(LabelSet::fromList({1})); // 2
        buf1->addNode(LabelSet::fromList({2})); // 3
        buf1->addEdge(0, 0, 1);

        {
            auto datapart = _db->uniqueAccess().createDataPart(std::move(buf1));
            datapart->load(_db->access(), *_jobSystem);
            _db->uniqueAccess().pushDataPart(std::move(datapart));
        }

        auto buf2 = _db->access().newDataBuffer();
        buf2->addEdge(2, 1, 2);
        EntityID id1 = buf2->addNode(LabelSet::fromList({0}));
        buf2->addNodeProperty<types::String>(id1, 0, "test1");

        buf2->addEdge(2, 0, 1);
        EntityID id2 = buf2->addNode(LabelSet::fromList({0}));
        buf2->addNodeProperty<types::String>(id2, 0, "test2");
        buf2->addNodeProperty<types::String>(2, 0, "test3");
        const EdgeRecord& edge= buf2->addEdge(4, 3, 4);
        buf2->addEdgeProperty<types::String>(edge, 0, "Edge property test");
        buf2->addNode(LabelSet::fromList({0}));
        buf2->addNode(LabelSet::fromList({0}));
        buf2->addNode(LabelSet::fromList({0}));
        buf2->addNode(LabelSet::fromList({0}));
        buf2->addNode(LabelSet::fromList({1}));
        buf2->addNode(LabelSet::fromList({1}));
        buf2->addNode(LabelSet::fromList({1}));
        buf2->addEdge(0, 3, 4);

        {
            auto datapart = _db->uniqueAccess().createDataPart(std::move(buf2));
            datapart->load(_db->access(), *_jobSystem);
            _db->uniqueAccess().pushDataPart(std::move(datapart));
        }
    }

    auto access = _db->access();
    std::cout << "All out edges: ";
    for (const auto& edge : access.scanOutEdges()) {
        std::cout << edge._edgeID.getValue()
                  << edge._nodeID.getValue()
                  << edge._otherID.getValue()
                  << std::endl;
        auto view = access.getEdgeView(edge._edgeID);
        std::cout << "  Has " << view.properties().getCount() << " properties" << std::endl;
    }
    std::cout << std::endl;

    auto it = access.scanNodeProperties<types::String>(0).begin();
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
                                                  _db.get(),
                                                  nodeCountLimit,
                                                  edgeCountLimit,
                                                  {
                                                      ._jsonDir = jsonDir,
                                                      ._workDir = _outDir,
                                                  });
    t1 = Clock::now();
    ASSERT_TRUE(res);
    std::cout << "Parsing: " << duration<Microseconds>(t0, t1) << " us" << std::endl;

    std::stringstream report;
    DBReport::getReport(_db->access(), report);
    std::cout << report.view() << std::endl;
}
