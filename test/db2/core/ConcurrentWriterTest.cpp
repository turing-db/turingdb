#include <gtest/gtest.h>

#include <spdlog/spdlog.h>

#include "ConcurrentWriter.h"
#include "Graph.h"
#include "GraphView.h"
#include "GraphReader.h"
#include "GraphMetadata.h"
#include "DataPartBuilder.h"
#include "FileUtils.h"
#include "JobSystem.h"
#include "LogSetup.h"
#include "DataPart.h"

using namespace db;
using namespace js;

struct TestEdgeRecord {
    EntityID _edgeID;
    EntityID _nodeID;
    EntityID _otherID;
};

class ConcurrentWriterTest : public ::testing::Test {
protected:
    void SetUp() override {
        const testing::TestInfo* const testInfo =
            testing::UnitTest::GetInstance()->current_test_info();

        _outDir = testInfo->test_suite_name();
        _outDir += "_";
        _outDir += testInfo->name();
        _outDir += ".out";
        _logPath = FileUtils::Path(_outDir) / "log";

        if (FileUtils::exists(_outDir)) {
            FileUtils::removeDirectory(_outDir);
        }
        FileUtils::createDirectory(_outDir);
        LogSetup::setupLogFileBacked(_logPath.string());
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->initialize();

        _graph = new Graph();
        PropertyTypeID uint64ID = 0;

        auto writer = _graph->newConcurrentPartWriter();
        DataPartBuilder& builder1 = writer->newBuilder(3, 2);
        DataPartBuilder& builder2 = writer->newBuilder(2, 3);
        DataPartBuilder& builder3 = writer->newBuilder(4, 4);

        auto& labelsets = _graph->getMetadata()->labelsets();
        const LabelSet l0 = LabelSet::fromList({0});
        const LabelSet l1 = LabelSet::fromList({1});
        const LabelSet l01 = LabelSet::fromList({0, 1});
        const LabelSetID l0ID = labelsets.getOrCreate(l0);
        const LabelSetID l1ID = labelsets.getOrCreate(l1);
        const LabelSetID l01ID = labelsets.getOrCreate(l01);

        {
            // NODE 0 (temp ID: 0)
            const EntityID tmpID = builder1.addNode(l0ID);
            builder1.addNodeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
        }

        {
            // NODE 1 (temp ID: 1)
            const EntityID tmpID = builder1.addNode(l0ID);
            builder1.addNodeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
        }

        {
            // NODE 2 (temp ID: 2)
            const EntityID tmpID = builder1.addNode(l1ID);
            builder1.addNodeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
        }

        builder1.addEdge(0, 0, 1); // EDGE 0
        builder1.addEdge(0, 0, 2); // EDGE 1

        {
            // NODE 4 (temp ID: 3))
            const EntityID tmpID = builder2.addNode(l01ID);
            builder2.addNodeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
        }

        {
            // NODE 3 (temp ID: 4)
            const EntityID tmpID = builder2.addNode(l1ID);
            builder2.addNodeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
        }

        builder2.addEdge(0, 3, 4); // EDGE 2
        builder2.addEdge(0, 3, 4); // EDGE 3
        builder2.addEdge(0, 4, 3); // EDGE 4

        {
            // NODE 8 (temp ID: 5)
            const EntityID tmpID = builder3.addNode(l01ID);
            builder3.addNodeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
        }

        {
            // NODE 5 (temp ID: 6)
            const EntityID tmpID = builder3.addNode(l0ID);
            builder3.addNodeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
        }

        {
            // NODE 6 (temp ID: 7)
            const EntityID tmpID = builder3.addNode(l1ID);
            builder3.addNodeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
        }

        {
            // NODE 7 (temp ID: 8)
            const EntityID tmpID = builder3.addNode(l1ID);
            builder3.addNodeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
        }

        builder3.addEdge(0, 6, 4); // EDGE 5
        builder3.addEdge(0, 6, 8); // EDGE 6
        builder3.addEdge(0, 7, 8); // EDGE 7
        builder3.addEdge(0, 2, 5); // EDGE 8

        writer->commitAll(*_jobSystem);
    }

    void TearDown() override {
        _jobSystem->terminate();
        delete _graph;
    }

    Graph* _graph = nullptr;
    std::unique_ptr<JobSystem> _jobSystem;
    std::string _outDir;
    FileUtils::Path _logPath;
};

TEST_F(ConcurrentWriterTest, ScanEdgesIteratorTest) {
    const auto view = _graph->view();
    const auto reader = view.read();
    std::vector<TestEdgeRecord> compareSet {
        {0, 0, 1},
        {1, 0, 2},
        {2, 3, 4},
        {3, 4, 3},
        {4, 4, 3},
        {5, 2, 8},
        {6, 5, 4},
        {7, 5, 7},
        {8, 6, 7},
    };

    auto it = compareSet.begin();
    size_t count = 0;
    for (const EdgeRecord& v : reader.scanOutEdges()) {
        spdlog::info("Node: {} has labelset {}", it->_nodeID, reader.getNodeLabelSetID(it->_nodeID));
        ASSERT_EQ(it->_nodeID.getValue(), v._nodeID.getValue());
        ASSERT_EQ(it->_otherID.getValue(), v._otherID.getValue());
        count++;
        it++;
    }

    ASSERT_EQ(count, compareSet.size());
}

TEST_F(ConcurrentWriterTest, ScanNodesIteratorTest) {
    const auto view = _graph->view();
    const auto reader = view.read();
    std::vector<EntityID> compareSet {0, 1, 2, 3, 4, 5, 6, 7, 8};

    auto it = compareSet.begin();
    size_t count = 0;
    for (const EntityID id : reader.scanNodes()) {
        ASSERT_EQ(it->getValue(), id.getValue());
        count++;
        it++;
    }
    ASSERT_EQ(count, compareSet.size());
}

TEST_F(ConcurrentWriterTest, ScanNodesByLabelIteratorTest) {
    const auto view = _graph->view();
    const auto reader = view.read();
    std::vector<EntityID> compareSet {2, 4, 3, 8, 6, 7};

    auto it = compareSet.begin();
    size_t count = 0;
    const auto labelset = LabelSet::fromList({1});

    for (const EntityID id : reader.scanNodesByLabel(&labelset)) {
        ASSERT_EQ(it->getValue(), id.getValue());
        ASSERT_EQ(it->getValue(), id.getValue());
        count++;
        it++;
    }
    ASSERT_EQ(count, compareSet.size());
}
