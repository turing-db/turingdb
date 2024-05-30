#include <gtest/gtest.h>

#include "ConcurrentWriter.h"
#include "DB.h"
#include "DBAccess.h"
#include "DBMetadata.h"
#include "DataBuffer.h"
#include "FileUtils.h"
#include "JobSystem.h"
#include "iterators/ScanEdgesIterator.h"
#include "iterators/ScanNodesByLabelIterator.h"
#include "iterators/ScanNodesIterator.h"
#include "LogSetup.h"
#include "DataPart.h"

using namespace db;

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

        _db = new DB();
        PropertyTypeID uint64ID = 0;

        auto bufferManager = _db->access().newConcurrentWriter();
        DataBuffer& tempData1 = bufferManager.newBuffer(3, 2);
        DataBuffer& tempData2 = bufferManager.newBuffer(2, 3);
        DataBuffer& tempData3 = bufferManager.newBuffer(4, 4);

        auto& labelsets = _db->getMetadata()->labelsets();
        const LabelSet l0 = LabelSet::fromList({0});
        const LabelSet l1 = LabelSet::fromList({1});
        const LabelSet l01 = LabelSet::fromList({0, 1});
        const LabelSetID l0ID = labelsets.getOrCreate(l0);
        const LabelSetID l1ID = labelsets.getOrCreate(l1);
        const LabelSetID l01ID = labelsets.getOrCreate(l01);

        {
            // NODE 0 (temp ID: 0)
            const EntityID tmpID = tempData1.addNode(l0ID);
            tempData1.addNodeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
        }

        {
            // NODE 1 (temp ID: 1)
            const EntityID tmpID = tempData1.addNode(l0ID);
            tempData1.addNodeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
        }

        {
            // NODE 2 (temp ID: 2)
            const EntityID tmpID = tempData1.addNode(l1ID);
            tempData1.addNodeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
        }

        tempData1.addEdge(0, 0, 1); // EDGE 0
        tempData1.addEdge(0, 0, 2); // EDGE 1

        {
            // NODE 4 (temp ID: 3))
            const EntityID tmpID = tempData2.addNode(l01ID);
            tempData2.addNodeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
        }

        {
            // NODE 3 (temp ID: 4)
            const EntityID tmpID = tempData2.addNode(l1ID);
            tempData2.addNodeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
        }

        tempData2.addEdge(0, 3, 4); // EDGE 2
        tempData2.addEdge(0, 3, 4); // EDGE 3
        tempData2.addEdge(0, 4, 3); // EDGE 4

        {
            // NODE 8 (temp ID: 5)
            const EntityID tmpID = tempData3.addNode(l01ID);
            tempData3.addNodeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
        }

        {
            // NODE 5 (temp ID: 6)
            const EntityID tmpID = tempData3.addNode(l0ID);
            tempData3.addNodeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
        }

        {
            // NODE 6 (temp ID: 7)
            const EntityID tmpID = tempData3.addNode(l1ID);
            tempData3.addNodeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
        }

        {
            // NODE 7 (temp ID: 8)
            const EntityID tmpID = tempData3.addNode(l1ID);
            tempData3.addNodeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
        }

        tempData3.addEdge(0, 6, 4); // EDGE 5
        tempData3.addEdge(0, 6, 8); // EDGE 6
        tempData3.addEdge(0, 7, 8); // EDGE 7
        tempData3.addEdge(0, 2, 5); // EDGE 8

        std::unique_ptr<DataBuffer> tempData = bufferManager.build();
        {
            auto datapart = _db->uniqueAccess().createDataPart(std::move(tempData));
            datapart->load(_db->access(), *_jobSystem);
            _db->uniqueAccess().pushDataPart(std::move(datapart));
        }
    }

    void TearDown() override {
        _jobSystem->terminate();
        delete _db;
    }

    DB* _db = nullptr;
    std::unique_ptr<JobSystem> _jobSystem;
    std::string _outDir;
    FileUtils::Path _logPath;
};

TEST_F(ConcurrentWriterTest, ScanEdgesIteratorTest) {
    auto access = _db->access();
    std::vector<TestEdgeRecord> compareSet {
        {0, 0, 1},
        {1, 0, 3},
        {2, 2, 4},
        {3, 2, 6},
        {4, 3, 8},
        {5, 4, 7},
        {6, 5, 6},
        {7, 7, 4},
        {8, 7, 4},
    };

    auto it = compareSet.begin();
    size_t count = 0;
    for (const EdgeRecord& v : access.scanOutEdges()) {
        ASSERT_EQ(it->_nodeID.getValue(), v._nodeID.getValue());
        ASSERT_EQ(it->_otherID.getValue(), v._otherID.getValue());
        count++;
        it++;
    }
    ASSERT_EQ(count, compareSet.size());
}

TEST_F(ConcurrentWriterTest, ScanNodesIteratorTest) {
    auto access = _db->access();
    std::vector<EntityID> compareSet {0, 1, 2, 3, 4, 5, 6, 7, 8};

    auto it = compareSet.begin();
    size_t count = 0;
    for (const EntityID id : access.scanNodes()) {
        ASSERT_EQ(it->getValue(), id.getValue());
        count++;
        it++;
    }
    ASSERT_EQ(count, compareSet.size());
}

TEST_F(ConcurrentWriterTest, ScanNodesByLabelIteratorTest) {
    auto access = _db->access();
    std::vector<EntityID> compareSet {7, 8, 3, 4, 5, 6};

    auto it = compareSet.begin();
    size_t count = 0;
    const auto& labelsets = _db->getMetadata()->labelsets();
    const LabelSet labelset = LabelSet::fromList({1});
    const LabelSetID labelsetID = labelsets.get(labelset);

    for (const EntityID id : access.scanNodesByLabel(labelsetID)) {
        ASSERT_EQ(it->getValue(), id.getValue());
        count++;
        it++;
    }
    ASSERT_EQ(count, compareSet.size());
}
