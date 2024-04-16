#include <gtest/gtest.h>

#include "BioLog.h"
#include "ConcurrentWriter.h"
#include "DB.h"
#include "FileUtils.h"
#include "Reader.h"
#include "DataBuffer.h"
#include "iterators/GetCoreInEdgesIterator.h"
#include "iterators/GetCoreOutEdgesIterator.h"
#include "iterators/GetPatchInEdgesIterator.h"
#include "iterators/GetPatchOutEdgesIterator.h"
#include "iterators/ScanCoreEdgesIterator.h"
#include "iterators/ScanNodesByLabelIterator.h"
#include "iterators/ScanNodesIterator.h"
#include "iterators/ScanPatchEdgesIterator.h"

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

        Log::BioLog::init();
        Log::BioLog::openFile(_logPath.string());

        _db = new DB();
        auto access = _db->uniqueAccess();
        PropertyTypeID uint64ID = 0;

        auto bufferManager = access.newConcurrentWriter();
        DataBuffer& tempData1 = bufferManager.newBuffer(3, 2);
        DataBuffer& tempData2 = bufferManager.newBuffer(2, 3);
        DataBuffer& tempData3 = bufferManager.newBuffer(4, 4);

        {
            // NODE 0 (temp ID: 0)
            const EntityID tmpID = tempData1.addNode(LabelSet {0});
            tempData1.addProperty<UInt64PropertyType>(
                tmpID, uint64ID, tmpID.getValue());
        }

        {
            // NODE 1 (temp ID: 1)
            const EntityID tmpID = tempData1.addNode(LabelSet {0});
            tempData1.addProperty<UInt64PropertyType>(
                tmpID, uint64ID, tmpID.getValue());
        }

        {
            // NODE 2 (temp ID: 2)
            const EntityID tmpID = tempData1.addNode(LabelSet {1});
            tempData1.addProperty<UInt64PropertyType>(
                tmpID, uint64ID, tmpID.getValue());
        }

        tempData1.addEdge(0, 0, 1); // EDGE 0
        tempData1.addEdge(0, 0, 2); // EDGE 1

        {
            // NODE 4 (temp ID: 3))
            const EntityID tmpID = tempData2.addNode(LabelSet {0, 1});
            tempData2.addProperty<UInt64PropertyType>(
                tmpID, uint64ID, tmpID.getValue());
        }

        {
            // NODE 3 (temp ID: 4)
            const EntityID tmpID = tempData2.addNode(LabelSet {1});
            tempData2.addProperty<UInt64PropertyType>(
                tmpID, uint64ID, tmpID.getValue());
        }

        tempData2.addEdge(0, 3, 4); // EDGE 2
        tempData2.addEdge(0, 3, 4); // EDGE 3
        tempData2.addEdge(0, 4, 3); // EDGE 4

        {
            // NODE 8 (temp ID: 5)
            const EntityID tmpID = tempData3.addNode(LabelSet {0, 1});
            tempData3.addProperty<UInt64PropertyType>(
                tmpID, uint64ID, tmpID.getValue());
        }

        {
            // NODE 5 (temp ID: 6)
            const EntityID tmpID = tempData3.addNode(LabelSet {0});
            tempData3.addProperty<UInt64PropertyType>(
                tmpID, uint64ID, tmpID.getValue());
        }

        {
            // NODE 6 (temp ID: 7)
            const EntityID tmpID = tempData3.addNode(LabelSet {1});
            tempData3.addProperty<UInt64PropertyType>(
                tmpID, uint64ID, tmpID.getValue());
        }

        {
            // NODE 7 (temp ID: 8)
            const EntityID tmpID = tempData3.addNode(LabelSet {1});
            tempData3.addProperty<UInt64PropertyType>(
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
        Log::BioLog::destroy();
    }

    DB* _db = nullptr;
    std::string _outDir;
    FileUtils::Path _logPath;
};

TEST_F(ConcurrentWriterTest, ScanCoreEdgesIteratorTest) {
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
    for (const EdgeRecord& v : reader.scanCoreEdges()) {
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
    std::vector<EntityID> compareSet {
        0, 1, 2, 3, 4, 5, 6, 7, 8
    };

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
    std::vector<EntityID> compareSet {
        3, 4, 5, 6, 7, 8
    };

    auto it = compareSet.begin();
    size_t count = 0;
    for (const EntityID id : reader.scanNodesByLabel({1})) {
        ASSERT_EQ(it->getValue(), id.getValue());
        count++;
        it++;
    }
    ASSERT_EQ(count, compareSet.size());
}
