#include <gtest/gtest.h>

#include "ConcurrentWriter.h"
#include "DB.h"
#include "DataBuffer.h"
#include "FileUtils.h"
#include "Reader.h"
#include "iterators/ScanEdgesIterator.h"
#include "iterators/ScanNodesByLabelIterator.h"
#include "iterators/ScanNodesIterator.h"
#include "LogSetup.h"

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

        _db = new DB();
        auto access = _db->uniqueAccess();
        PropertyTypeID uint64ID = 0;

        auto bufferManager = access.newConcurrentWriter();
        DataBuffer& tempData1 = bufferManager.newBuffer(3, 2);
        DataBuffer& tempData2 = bufferManager.newBuffer(2, 3);
        DataBuffer& tempData3 = bufferManager.newBuffer(4, 4);

        const Labelset l0 = Labelset::fromList({0});
        const Labelset l1 = Labelset::fromList({1});
        const Labelset l01 = Labelset::fromList({0, 1});
        const LabelsetID l0ID = access.getLabelsetID(l0);
        const LabelsetID l1ID = access.getLabelsetID(l1);
        const LabelsetID l01ID = access.getLabelsetID(l01);

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
        access.pushDataPart(*tempData);
    }

    void TearDown() override {
        delete _db;
    }

    DB* _db = nullptr;
    std::string _outDir;
    FileUtils::Path _logPath;
};

TEST_F(ConcurrentWriterTest, ScanEdgesIteratorTest) {
    auto access = _db->access();
    auto reader = access.getReader();
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
    for (const EdgeRecord& v : reader.scanOutEdges()) {
        ASSERT_EQ(it->_nodeID.getValue(), v._nodeID.getValue());
        ASSERT_EQ(it->_otherID.getValue(), v._otherID.getValue());
        count++;
        it++;
    }
    ASSERT_EQ(count, compareSet.size());
}

TEST_F(ConcurrentWriterTest, ScanNodesIteratorTest) {
    auto access = _db->access();
    auto reader = access.getReader();
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
    auto access = _db->access();
    auto reader = access.getReader();
    std::vector<EntityID> compareSet {7, 8, 3, 4, 5, 6};

    auto it = compareSet.begin();
    size_t count = 0;
    const Labelset labelset = Labelset::fromList({1});
    const LabelsetID labelsetID = access.getLabelsetID(labelset);

    for (const EntityID id : reader.scanNodesByLabel(labelsetID)) {
        ASSERT_EQ(it->getValue(), id.getValue());
        count++;
        it++;
    }
    ASSERT_EQ(count, compareSet.size());
}
