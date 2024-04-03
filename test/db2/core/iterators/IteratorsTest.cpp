#include <gtest/gtest.h>
#include <set>

#include "BioLog.h"
#include "DB.h"
#include "FileUtils.h"
#include "Reader.h"
#include "TemporaryDataBuffer.h"
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

class IteratorsTest : public ::testing::Test {
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

        //
        /* FIRST BUFFER */
        //
        TemporaryDataBuffer tempData1 = access.createTempBuffer();
        PropertyTypeID uint64ID = 0;
        PropertyTypeID stringID = 1;

        {
            // NODE 0 (temp ID: 0)
            const EntityID tmpID = tempData1.addNode(LabelSet {0});
            tempData1.addProperty<UInt64PropertyType>(
                tmpID, uint64ID, tmpID.getValue());
            tempData1.addProperty<StringPropertyType>(
                tmpID, stringID, "TmpID" + std::to_string(tmpID));
        }

        {
            // NODE 1 (temp ID: 1)
            const EntityID tmpID = tempData1.addNode(LabelSet {0});
            tempData1.addProperty<UInt64PropertyType>(
                tmpID, uint64ID, tmpID.getValue());
            tempData1.addProperty<StringPropertyType>(
                tmpID, stringID, "TmpID" + std::to_string(tmpID));
        }

        {
            // NODE 2 (temp ID: 2)
            const EntityID tmpID = tempData1.addNode(LabelSet {1});
            tempData1.addProperty<UInt64PropertyType>(
                tmpID, uint64ID, tmpID.getValue());
            tempData1.addProperty<StringPropertyType>(
                tmpID, stringID, "TmpID" + std::to_string(tmpID));
        }

        // EDGE 0 [0->1] (temp ID: 0)
        tempData1.addEdge(_edgeTypeID, 0, 1);

        // EDGE 1 [0->2] (temp ID: 1)
        tempData1.addEdge(_edgeTypeID, 0, 2);

        //
        /* SECOND BUFFER (Concurrent to the first one) */
        //
        TemporaryDataBuffer tempData2 = access.createTempBuffer();

        {
            // NODE 4 (temp ID: 0))
            const EntityID tmpID = tempData2.addNode(LabelSet {0, 1});
            tempData2.addProperty<UInt64PropertyType>(
                tmpID, uint64ID, tmpID.getValue());
            tempData2.addProperty<StringPropertyType>(
                tmpID, stringID, "TmpID" + std::to_string(tmpID));
        }

        {
            // NODE 3 (temp ID: 1)
            const EntityID tmpID = tempData2.addNode(LabelSet {1});
            tempData2.addProperty<UInt64PropertyType>(
                tmpID, uint64ID, tmpID.getValue());
        }

        // EDGE 3 [4->3] (temp ID: 0 [0->1])
        tempData2.addEdge(_edgeTypeID, 0, 1);

        // EDGE 4 [4->3] (temp ID: 1 [0->1])
        tempData2.addEdge(_edgeTypeID, 0, 1);

        // EDGE 2 [3->4] (temp ID: 2 [1->0])
        tempData2.addEdge(_edgeTypeID, 1, 0);

        // PUSH DATAPARTS
        access.pushDataPart(tempData1);
        access.pushDataPart(tempData2);

        //
        /* THIRD BUFFER (Empty) */
        //
        TemporaryDataBuffer tempData3 = access.createTempBuffer();
        access.pushDataPart(tempData3);

        //
        /* FOURTH BUFFER (First node and edge ids: 5, 5) */
        //
        TemporaryDataBuffer tempData4 = access.createTempBuffer();

        {
            // NODE 8 (temp ID: 5)
            const EntityID tmpID = tempData4.addNode(LabelSet {0, 1});
            tempData4.addProperty<UInt64PropertyType>(
                tmpID, uint64ID, tmpID.getValue());
            tempData4.addProperty<StringPropertyType>(
                tmpID, stringID, "TmpID" + std::to_string(tmpID));
        }

        {
            // NODE 5 (temp ID: 6)
            const EntityID tmpID = tempData4.addNode(LabelSet {0});
            tempData4.addProperty<UInt64PropertyType>(
                tmpID, uint64ID, tmpID.getValue());
            tempData4.addProperty<StringPropertyType>(
                tmpID, stringID, "TmpID" + std::to_string(tmpID));
        }

        {
            // NODE 6 (temp ID: 7)
            const EntityID tmpID = tempData4.addNode(LabelSet {1});
            tempData4.addProperty<UInt64PropertyType>(
                tmpID, uint64ID, tmpID.getValue());
            tempData4.addProperty<StringPropertyType>(
                tmpID, stringID, "TmpID" + std::to_string(tmpID));
        }

        {
            // NODE 7 (temp ID: 8)
            const EntityID tmpID = tempData4.addNode(LabelSet {1});
            tempData4.addProperty<UInt64PropertyType>(
                tmpID, uint64ID, tmpID.getValue());
            tempData4.addProperty<StringPropertyType>(
                tmpID, stringID, "TmpID" + std::to_string(tmpID));
        }

        // Edge 5 [5->4] (temp ID: 5 [6->4])
        tempData4.addEdge(_edgeTypeID, 6, 4);

        // Edge 6 [5->7] (temp ID: 6 [6->8])
        tempData4.addEdge(_edgeTypeID, 6, 8);

        // Edge 7 [6->7] (temp ID: 7 [7->8])
        tempData4.addEdge(_edgeTypeID, 7, 8);

        // Edge 8 [2->8] (temp ID: 8 [2->5])
        tempData4.addEdge(_edgeTypeID, 2, 5);

        access.pushDataPart(tempData4);
    }

    void TearDown() override {
        delete _db;
        Log::BioLog::destroy();
    }

    DB* _db = nullptr;
    std::string _outDir;
    FileUtils::Path _logPath;
    static inline const EntityTypeID _edgeTypeID = 0;
};

TEST_F(IteratorsTest, ScanCoreEdgesIteratorTest) {
    auto access = _db->access();
    auto reader = access.getReader();
    std::vector<TestEdgeRecord> compareSet {
        {0, 0, 1},
        {1, 0, 2},
        {2, 3, 4},
        {3, 4, 3},
        {4, 4, 3},
        {5, 5, 4},
        {6, 5, 7},
        {7, 6, 7},
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

TEST_F(IteratorsTest, ScanPatchEdgesIteratorTest) {
    auto access = _db->access();
    auto reader = access.getReader();
    std::vector<TestEdgeRecord> compareSet {
        {8, 2, 8},
    };

    auto it = compareSet.begin();
    size_t count = 0;
    for (const EdgeRecord& v : reader.scanPatchEdges()) {
        ASSERT_EQ(it->_nodeID.getValue(), v._nodeID.getValue());
        ASSERT_EQ(it->_otherID.getValue(), v._otherID.getValue());
        count++;
        it++;
    }
    ASSERT_EQ(count, compareSet.size());
}

TEST_F(IteratorsTest, ScanNodesIteratorTest) {
    auto access = _db->access();
    auto reader = access.getReader();
    std::vector<EntityID> compareSet {0, 1, 2, 3, 4, 5, 6, 7, 8};

    auto it = compareSet.begin();
    size_t count = 0;
    for (const EntityID id : reader.scanNodes()) {
        ASSERT_EQ(it->getValue(), id.getValue());
        ASSERT_EQ(it->getValue(), id.getValue());
        count++;
        it++;
    }
    ASSERT_EQ(count, compareSet.size());
}

TEST_F(IteratorsTest, ScanNodesByLabelIteratorTest) {
    auto access = _db->access();
    auto reader = access.getReader();
    std::vector<EntityID> compareSet {2, 3, 4, 6, 7, 8};

    auto it = compareSet.begin();
    size_t count = 0;
    for (const EntityID id : reader.scanNodesByLabel({1})) {
        ASSERT_EQ(it->getValue(), id.getValue());
        ASSERT_EQ(it->getValue(), id.getValue());
        count++;
        it++;
    }
    ASSERT_EQ(count, compareSet.size());
}

TEST_F(IteratorsTest, GetCoreEdgesIteratorTest) {
    auto access = _db->access();
    auto reader = access.getReader();
    ColumnNodes inputNodeIDs = {1, 3, 8};
    std::vector<TestEdgeRecord> compareSet {
        {2, 3, 4},
        {0, 1, 0},
        {3, 3, 4},
        {4, 3, 4},
    };

    auto it = compareSet.begin();
    size_t count = 0;
    for (const EdgeRecord& v : reader.getCoreOutEdges(&inputNodeIDs)) {
        ASSERT_EQ(it->_nodeID.getValue(), v._nodeID.getValue());
        ASSERT_EQ(it->_otherID.getValue(), v._otherID.getValue());
        count++;
        it++;
    }
    for (const EdgeRecord& v : reader.getCoreInEdges(&inputNodeIDs)) {
        ASSERT_EQ(it->_nodeID.getValue(), v._nodeID.getValue());
        ASSERT_EQ(it->_otherID.getValue(), v._otherID.getValue());
        count++;
        it++;
    }
    ASSERT_EQ(count, compareSet.size());
}

TEST_F(IteratorsTest, GetPatchEdgesIteratorTest) {
    auto access = _db->access();
    auto reader = access.getReader();
    ColumnNodes inputNodeIDs = {1, 3, 8};
    std::vector<TestEdgeRecord> compareSet {
        {8, 8, 2},
    };

    auto it = compareSet.begin();
    size_t count = 0;
    for (const EdgeRecord& v : reader.getPatchOutEdges(&inputNodeIDs)) {
        ASSERT_EQ(it->_nodeID.getValue(), v._nodeID.getValue());
        ASSERT_EQ(it->_otherID.getValue(), v._otherID.getValue());
        count++;
        it++;
    }
    for (const EdgeRecord& v : reader.getPatchInEdges(&inputNodeIDs)) {
        ASSERT_EQ(it->_nodeID.getValue(), v._nodeID.getValue());
        ASSERT_EQ(it->_otherID.getValue(), v._otherID.getValue());
        count++;
        it++;
    }
    ASSERT_EQ(count, compareSet.size());
}

TEST_F(IteratorsTest, ScanNodePropertiesIteratorTest) {
    auto access = _db->access();
    auto reader = access.getReader();

    {
        std::vector<uint64_t> compareSet {0, 1, 2, 1, 0, 6, 7, 8, 5};
        auto it = compareSet.begin();
        size_t count = 0;
        for (const uint64_t v : reader.scanNodeProperties<UInt64PropertyType>(0)) {
            ASSERT_EQ(*it, v);
            count++;
            it++;
        }
        ASSERT_EQ(count, compareSet.size());
    }

    {
        std::vector<std::string_view> compareSet {
            "TmpID0",
            "TmpID1",
            "TmpID2",
            // "TmpID1", This property is not set for this node
            "TmpID0",
            "TmpID6",
            "TmpID7",
            "TmpID8",
            "TmpID5",
        };
        auto it = compareSet.begin();
        size_t count = 0;
        for (const std::string& v : reader.scanNodeProperties<StringPropertyType>(1)) {
            ASSERT_STREQ(it->data(), v.c_str());
            count++;
            it++;
        }
        ASSERT_EQ(count, compareSet.size());
    }
}

TEST_F(IteratorsTest, GetNodePropertiesIteratorTest) {
    auto access = _db->access();
    auto reader = access.getReader();
    ColumnNodes inputNodeIDs = {1, 3, 8};

    {
        std::vector<uint64_t> compareSet {1, 1, 5};
        auto it = compareSet.begin();
        size_t count = 0;
        for (const uint64_t v : reader.getNodeProperties<UInt64PropertyType>(0, &inputNodeIDs)) {
            ASSERT_EQ(*it, v);
            count++;
            it++;
        }
        ASSERT_EQ(count, compareSet.size());
    }

    {
        std::vector<std::string_view> compareSet {
            "TmpID1",
            "TmpID5",
        };
        auto it = compareSet.begin();
        size_t count = 0;
        for (const std::string& v : reader.getNodeProperties<StringPropertyType>(1, &inputNodeIDs)) {
            ASSERT_STREQ(it->data(), v.c_str());
            count++;
            it++;
        }
        ASSERT_EQ(count, compareSet.size());
    }
}
