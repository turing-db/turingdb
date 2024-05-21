#include <gtest/gtest.h>
#include <set>

#include "DB.h"
#include "DataBuffer.h"
#include "FileUtils.h"
#include "Reader.h"
#include "LogSetup.h"
#include "iterators/GetInEdgesIterator.h"
#include "iterators/GetOutEdgesIterator.h"
#include "iterators/ScanEdgesIterator.h"
#include "iterators/ScanNodesByLabelIterator.h"
#include "iterators/ScanNodesIterator.h"

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
        LogSetup::setupLogFileBacked(_logPath.string());

        _db = new DB();
        auto access = _db->uniqueAccess();

        /* FIRST BUFFER */
        DataBuffer tempData1 = access.newDataBuffer();
        PropertyTypeID uint64ID = 0;
        PropertyTypeID stringID = 1;

        {
            // Node 0
            const EntityID tmpID = tempData1.addNode(Labelset::fromList({0}));
            tempData1.addNodeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
            tempData1.addNodeProperty<types::String>(
                tmpID, stringID, "TmpID" + std::to_string(tmpID));
        }

        {
            // Node 1
            const EntityID tmpID = tempData1.addNode(Labelset::fromList({0}));
            tempData1.addNodeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
            tempData1.addNodeProperty<types::String>(
                tmpID, stringID, "TmpID" + std::to_string(tmpID));
        }

        {
            // Node 2
            const EntityID tmpID = tempData1.addNode(Labelset::fromList({1}));
            tempData1.addNodeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
        }

        {
            // Edge 001
            const EntityID tmpID = tempData1.addEdge(_edgeTypeID, 0, 1);
            tempData1.addEdgeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
            tempData1.addEdgeProperty<types::String>(
                tmpID, stringID, "TmpEdgeID" + std::to_string(tmpID));
        }

        {
            // Edge 102
            const EntityID tmpID = tempData1.addEdge(_edgeTypeID, 0, 2);
            tempData1.addEdgeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
            tempData1.addEdgeProperty<types::String>(
                tmpID, stringID, "TmpEdgeID" + std::to_string(tmpID));
        }

        /* SECOND BUFFER (Concurrent to the first one) */
        DataBuffer tempData2 = access.newDataBuffer();

        {
            // Node 4
            const EntityID tmpID = tempData2.addNode(Labelset::fromList({0, 1}));
            tempData2.addNodeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
            tempData2.addNodeProperty<types::String>(
                tmpID, stringID, "TmpID" + std::to_string(tmpID));
        }

        {
            // Node 3
            const EntityID tmpID = tempData2.addNode(Labelset::fromList({1}));
            tempData2.addNodeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
        }

        {
            // Edge 343
            const EntityID tmpID = tempData2.addEdge(_edgeTypeID, 0, 1);
            tempData2.addEdgeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
            tempData2.addEdgeProperty<types::String>(
                tmpID, stringID, "TmpEdgeID" + std::to_string(tmpID));
        }

        {
            // Edge 443
            const EntityID tmpID = tempData2.addEdge(_edgeTypeID, 0, 1);
            tempData2.addEdgeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
            tempData2.addEdgeProperty<types::String>(
                tmpID, stringID, "TmpEdgeID" + std::to_string(tmpID));
        }

        {
            // Edge 234
            const EntityID tmpID = tempData2.addEdge(_edgeTypeID, 1, 0);
            tempData2.addEdgeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
        }

        // PUSH DATAPARTS
        access.pushDataPart(tempData1);
        access.pushDataPart(tempData2);

        /* THIRD BUFFER (Empty) */
        DataBuffer tempData3 = access.newDataBuffer();
        access.pushDataPart(tempData3);

        /* FOURTH BUFFER (First node and edge ids: 5, 5) */
        DataBuffer tempData4 = access.newDataBuffer();

        {
            // Node 8
            const EntityID tmpID = tempData4.addNode(Labelset::fromList({0, 1}));
            tempData4.addNodeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
            tempData4.addNodeProperty<types::String>(
                tmpID, stringID, "TmpID" + std::to_string(tmpID));
        }

        {
            // Node 5
            const EntityID tmpID = tempData4.addNode(Labelset::fromList({0}));
            tempData4.addNodeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
            tempData4.addNodeProperty<types::String>(
                tmpID, stringID, "TmpID" + std::to_string(tmpID));
        }

        {
            // Node 6
            const EntityID tmpID = tempData4.addNode(Labelset::fromList({1}));
            tempData4.addNodeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
            tempData4.addNodeProperty<types::String>(
                tmpID, stringID, "TmpID" + std::to_string(tmpID));
        }

        {
            // Node 7
            const EntityID tmpID = tempData4.addNode(Labelset::fromList({1}));
            tempData4.addNodeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
            tempData4.addNodeProperty<types::String>(
                tmpID, stringID, "TmpID" + std::to_string(tmpID));
        }

        {
            // Edge 654
            const EntityID tmpID = tempData4.addEdge(_edgeTypeID, 6, 4);
            tempData4.addEdgeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
            tempData4.addEdgeProperty<types::String>(
                tmpID, stringID, "TmpEdgeID" + std::to_string(tmpID));
        }

        {
            // Edge 757
            const EntityID tmpID = tempData4.addEdge(_edgeTypeID, 6, 8);
            tempData4.addEdgeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
            tempData4.addEdgeProperty<types::String>(
                tmpID, stringID, "TmpEdgeID" + std::to_string(tmpID));
        }

        {
            // Edge 867
            const EntityID tmpID = tempData4.addEdge(_edgeTypeID, 7, 8);
            tempData4.addEdgeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
            tempData4.addEdgeProperty<types::String>(
                tmpID, stringID, "TmpEdgeID" + std::to_string(tmpID));
        }

        {
            // Edge 528
            const EntityID tmpID = tempData4.addEdge(_edgeTypeID, 2, 5);
            tempData4.addEdgeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
            tempData4.addEdgeProperty<types::String>(
                tmpID, stringID, "TmpEdgeID" + std::to_string(tmpID));
        }

        tempData4.addNodeProperty<types::String>(
            2, stringID, "TmpID2 patch");
        tempData4.addEdgeProperty<types::String>(
            2, stringID, "TmpEdgeID2 patch");

        access.pushDataPart(tempData4);
    }

    void
    TearDown() override {
        delete _db;
    }

    DB* _db = nullptr;
    std::string _outDir;
    FileUtils::Path _logPath;
    static inline const EdgeTypeID _edgeTypeID {0};
};

TEST_F(IteratorsTest, ScanEdgesIteratorTest) {
    auto access = _db->access();
    auto reader = access.getReader();
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
    std::vector<EntityID> compareSet {2, 4, 3, 8, 6, 7};

    auto it = compareSet.begin();
    size_t count = 0;
    const auto labelset = Labelset::fromList({1});
    const LabelsetID labelsetID = _db->getLabelsetID(labelset);

    for (const EntityID id : reader.scanNodesByLabel(labelsetID)) {
        ASSERT_EQ(it->getValue(), id.getValue());
        ASSERT_EQ(it->getValue(), id.getValue());
        count++;
        it++;
    }
    ASSERT_EQ(count, compareSet.size());
}

TEST_F(IteratorsTest, GetEdgesIteratorTest) {
    auto access = _db->access();
    auto reader = access.getReader();
    ColumnNodes inputNodeIDs = {1, 2, 3, 8};
    std::vector<TestEdgeRecord> compareSet {
        {2, 3, 4},
        {5, 2, 8},
    };

    auto it = compareSet.begin();
    size_t count = 0;
    for (const EdgeRecord& v : reader.getOutEdges(&inputNodeIDs)) {
        ASSERT_EQ(it->_nodeID.getValue(), v._nodeID.getValue());
        ASSERT_EQ(it->_otherID.getValue(), v._otherID.getValue());
        count++;
        it++;
    }

    ASSERT_EQ(compareSet.size(), count);

    count = 0;
    compareSet = {
        {0, 0, 1},
        {1, 0, 2},
        {3, 4, 3},
        {4, 4, 3},
        {5, 2, 8},
    };
    it = compareSet.begin();

    for (const EdgeRecord& v : reader.getInEdges(&inputNodeIDs)) {
        ASSERT_EQ(it->_nodeID.getValue(), v._otherID.getValue());
        ASSERT_EQ(it->_otherID.getValue(), v._nodeID.getValue());
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
        for (const uint64_t v : reader.scanNodeProperties<types::UInt64>(0)) {
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
            // "TmpID1", This property is not set for this node
            "TmpID0",
            "TmpID2 patch",
            "TmpID6",
            "TmpID7",
            "TmpID8",
            "TmpID5",
        };
        auto it = compareSet.begin();
        size_t count = 0;
        for (const std::string& v : reader.scanNodeProperties<types::String>(1)) {
            ASSERT_STREQ(it->data(), v.c_str());
            count++;
            it++;
        }
        ASSERT_EQ(count, compareSet.size());
    }
}

TEST_F(IteratorsTest, ScanEdgePropertiesIteratorTest) {
    auto access = _db->access();
    auto reader = access.getReader();

    {
        std::vector<uint64_t> compareSet {0, 1, 2, 0, 1, 8, 5, 6, 7};
        auto it = compareSet.begin();
        size_t count = 0;
        for (const uint64_t v : reader.scanEdgeProperties<types::UInt64>(0)) {
            ASSERT_EQ(*it, v);
            count++;
            it++;
        }
        ASSERT_EQ(count, compareSet.size());
    }

    {
        std::vector<std::string_view> compareSet {
            "TmpEdgeID0",
            "TmpEdgeID1",
            "TmpEdgeID0",
            "TmpEdgeID1",
            "TmpEdgeID2 patch",
            "TmpEdgeID8",
            "TmpEdgeID5",
            "TmpEdgeID6",
            "TmpEdgeID7",
        };
        auto it = compareSet.begin();
        size_t count = 0;
        for (const std::string& v : reader.scanEdgeProperties<types::String>(1)) {
            ASSERT_STREQ(it->data(), v.c_str());
            count++;
            it++;
        }
        ASSERT_EQ(count, compareSet.size());
    }
}

TEST_F(IteratorsTest, ScanNodePropertiesByLabelIteratorTest) {
    auto access = _db->access();
    auto reader = access.getReader();

    const auto labelset = Labelset::fromList({1});
    const LabelsetID labelsetID = _db->getLabelsetID(labelset);

    {
        std::vector<uint64_t> compareSet {2, 0, 1, 5, 7, 8};
        auto it = compareSet.begin();
        size_t count = 0;
        for (const uint64_t v : reader.scanNodePropertiesByLabel<types::UInt64>(0, labelsetID)) {
            ASSERT_EQ(*it, v);
            count++;
            it++;
        }
        ASSERT_EQ(count, compareSet.size());
    }

    {
        std::vector<std::string_view> compareSet {
            // "TmpID1", This property is not set for this node
            "TmpID0",
            "TmpID5",
            "TmpID2 patch",
            "TmpID7",
            "TmpID8",
        };
        auto it = compareSet.begin();
        size_t count = 0;
        for (const std::string& v : reader.scanNodePropertiesByLabel<types::String>(1, labelsetID)) {
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
        for (const uint64_t v : reader.getNodeProperties<types::UInt64>(0, &inputNodeIDs)) {
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
        for (const std::string& v : reader.getNodeProperties<types::String>(1, &inputNodeIDs)) {
            ASSERT_STREQ(it->data(), v.c_str());
            count++;
            it++;
        }
        ASSERT_EQ(count, compareSet.size());
    }
}
