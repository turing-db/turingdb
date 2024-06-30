#include <gtest/gtest.h>
#include <set>

#include "DB.h"
#include "DBAccess.h"
#include "DBMetadata.h"
#include "DataBuffer.h"
#include "FileUtils.h"
#include "JobSystem.h"
#include "LogSetup.h"
#include "iterators/GetInEdgesIterator.h"
#include "iterators/GetOutEdgesIterator.h"
#include "iterators/ScanEdgesIterator.h"
#include "iterators/ScanNodesByLabelIterator.h"
#include "iterators/ScanNodesIterator.h"
#include "spdlog/spdlog.h"

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

        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->initialize();
        _db = new DB();

        /* FIRST BUFFER */
        std::unique_ptr<DataBuffer> tempData1 = _db->access().newDataBuffer();
        PropertyTypeID uint64ID = 0;
        PropertyTypeID stringID = 1;

        {
            // Node 0
            const EntityID tmpID = tempData1->addNode(LabelSet::fromList({0}));
            tempData1->addNodeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
            tempData1->addNodeProperty<types::String>(
                tmpID, stringID, "TmpID" + std::to_string(tmpID));
        }

        {
            // Node 1
            const EntityID tmpID = tempData1->addNode(LabelSet::fromList({0}));
            tempData1->addNodeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
            tempData1->addNodeProperty<types::String>(
                tmpID, stringID, "TmpID" + std::to_string(tmpID));
        }

        {
            // Node 2
            const EntityID tmpID = tempData1->addNode(LabelSet::fromList({1}));
            tempData1->addNodeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
        }

        {
            // Edge 001
            const EdgeRecord& edge = tempData1->addEdge(_edgeTypeID, 0, 1);
            tempData1->addEdgeProperty<types::UInt64>(
                edge, uint64ID, edge._edgeID.getValue());
            tempData1->addEdgeProperty<types::String>(
                edge, stringID, "TmpEdgeID" + std::to_string(edge._edgeID));
        }

        {
            // Edge 102
            const EdgeRecord& edge = tempData1->addEdge(_edgeTypeID, 0, 2);
            tempData1->addEdgeProperty<types::UInt64>(
                edge, uint64ID, edge._edgeID.getValue());
            tempData1->addEdgeProperty<types::String>(
                edge, stringID, "TmpEdgeID" + std::to_string(edge._edgeID));
        }

        /* SECOND BUFFER (Concurrent to the first one) */
        std::unique_ptr<DataBuffer> tempData2 = _db->access().newDataBuffer();

        {
            // Node 4
            const EntityID tmpID = tempData2->addNode(LabelSet::fromList({0, 1}));
            tempData2->addNodeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
            tempData2->addNodeProperty<types::String>(
                tmpID, stringID, "TmpID" + std::to_string(tmpID));
        }

        {
            // Node 3
            const EntityID tmpID = tempData2->addNode(LabelSet::fromList({1}));
            tempData2->addNodeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
        }

        {
            // Edge 343
            const EdgeRecord& edge = tempData2->addEdge(_edgeTypeID, 0, 1);
            tempData2->addEdgeProperty<types::UInt64>(
                edge, uint64ID, edge._edgeID.getValue());
            tempData2->addEdgeProperty<types::String>(
                edge, stringID, "TmpEdgeID" + std::to_string(edge._edgeID));
        }

        {
            // Edge 443
            const EdgeRecord& edge = tempData2->addEdge(_edgeTypeID, 0, 1);
            tempData2->addEdgeProperty<types::UInt64>(
                edge, uint64ID, edge._edgeID.getValue());
            tempData2->addEdgeProperty<types::String>(
                edge, stringID, "TmpEdgeID" + std::to_string(edge._edgeID));
        }

        {
            // Edge 234
            const EdgeRecord& edge = tempData2->addEdge(_edgeTypeID, 1, 0);
            tempData2->addEdgeProperty<types::UInt64>(
                edge, uint64ID, edge._edgeID.getValue());
        }

        // PUSH DATAPARTS
        {
            spdlog::info("Pushing 1");
            auto datapart = _db->uniqueAccess().createDataPart(std::move(tempData1));
            datapart->load(_db->access(), *_jobSystem);
            _db->uniqueAccess().pushDataPart(std::move(datapart));
        }

        {
            spdlog::info("Pushing 2");
            auto datapart = _db->uniqueAccess().createDataPart(std::move(tempData2));
            datapart->load(_db->access(), *_jobSystem);
            _db->uniqueAccess().pushDataPart(std::move(datapart));
        }

        /* THIRD BUFFER (Empty) */
        std::unique_ptr<DataBuffer> tempData3 = _db->access().newDataBuffer();
        {
            spdlog::info("Pushing 3");
            auto datapart = _db->uniqueAccess().createDataPart(std::move(tempData3));
            datapart->load(_db->access(), *_jobSystem);
            _db->uniqueAccess().pushDataPart(std::move(datapart));
        }

        /* FOURTH BUFFER (First node and edge ids: 5, 5) */
        std::unique_ptr<DataBuffer> tempData4 = _db->access().newDataBuffer();

        {
            // Node 8
            const EntityID tmpID = tempData4->addNode(LabelSet::fromList({0, 1}));
            tempData4->addNodeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
            tempData4->addNodeProperty<types::String>(
                tmpID, stringID, "TmpID" + std::to_string(tmpID));
        }

        {
            // Node 5
            const EntityID tmpID = tempData4->addNode(LabelSet::fromList({0}));
            tempData4->addNodeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
            tempData4->addNodeProperty<types::String>(
                tmpID, stringID, "TmpID" + std::to_string(tmpID));
        }

        {
            // Node 6
            const EntityID tmpID = tempData4->addNode(LabelSet::fromList({1}));
            tempData4->addNodeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
            tempData4->addNodeProperty<types::String>(
                tmpID, stringID, "TmpID" + std::to_string(tmpID));
        }

        {
            // Node 7
            const EntityID tmpID = tempData4->addNode(LabelSet::fromList({1}));
            tempData4->addNodeProperty<types::UInt64>(
                tmpID, uint64ID, tmpID.getValue());
            tempData4->addNodeProperty<types::String>(
                tmpID, stringID, "TmpID" + std::to_string(tmpID));
        }

        {
            // Edge 654
            const EdgeRecord& edge = tempData4->addEdge(_edgeTypeID, 6, 4);
            tempData4->addEdgeProperty<types::UInt64>(
                edge, uint64ID, edge._edgeID.getValue());
            tempData4->addEdgeProperty<types::String>(
                edge, stringID, "TmpEdgeID" + std::to_string(edge._edgeID));
        }

        {
            // Edge 757
            const EdgeRecord& edge = tempData4->addEdge(_edgeTypeID, 6, 8);
            tempData4->addEdgeProperty<types::UInt64>(
                edge, uint64ID, edge._edgeID.getValue());
            tempData4->addEdgeProperty<types::String>(
                edge, stringID, "TmpEdgeID" + std::to_string(edge._edgeID));
        }

        {
            // Edge 867
            const EdgeRecord& edge = tempData4->addEdge(_edgeTypeID, 7, 8);
            tempData4->addEdgeProperty<types::UInt64>(
                edge, uint64ID, edge._edgeID.getValue());
            tempData4->addEdgeProperty<types::String>(
                edge, stringID, "TmpEdgeID" + std::to_string(edge._edgeID));
        }

        {
            // Edge 528
            const EdgeRecord& edge = tempData4->addEdge(_edgeTypeID, 2, 5);
            tempData4->addEdgeProperty<types::UInt64>(
                edge, uint64ID, edge._edgeID.getValue());
            tempData4->addEdgeProperty<types::String>(
                edge, stringID, "TmpEdgeID" + std::to_string(edge._edgeID));
        }

        tempData4->addNodeProperty<types::String>(
            2, stringID, "TmpID2 patch");

        const EdgeRecord* edgeToPatch = _db->access().getEdge(2);
        tempData4->addEdgeProperty<types::String>(
            *edgeToPatch, stringID, "TmpEdgeID2 patch");

        {
            spdlog::info("Pushing 4");
            auto datapart = _db->uniqueAccess().createDataPart(std::move(tempData4));
            datapart->load(_db->access(), *_jobSystem);
            _db->uniqueAccess().pushDataPart(std::move(datapart));
        }
    }

    void TearDown() override {
        _jobSystem->terminate();
        delete _db;
    }

    std::unique_ptr<JobSystem> _jobSystem;
    DB* _db = nullptr;
    std::string _outDir;
    FileUtils::Path _logPath;
    static inline const EdgeTypeID _edgeTypeID {0};
};

TEST_F(IteratorsTest, ScanEdgesIteratorTest) {
    auto access = _db->access();
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
    for (const EdgeRecord& v : access.scanOutEdges()) {
        ASSERT_EQ(it->_nodeID.getValue(), v._nodeID.getValue());
        ASSERT_EQ(it->_otherID.getValue(), v._otherID.getValue());
        spdlog::info("Node: {} has labelset {}", it->_nodeID, access.getNodeLabelSetID(it->_nodeID));
        count++;
        it++;
    }

    ASSERT_EQ(count, compareSet.size());
}

TEST_F(IteratorsTest, ScanNodesIteratorTest) {
    auto access = _db->access();
    std::vector<EntityID> compareSet {0, 1, 2, 3, 4, 5, 6, 7, 8};

    auto it = compareSet.begin();
    size_t count = 0;
    for (const EntityID id : access.scanNodes()) {
        ASSERT_EQ(it->getValue(), id.getValue());
        ASSERT_EQ(it->getValue(), id.getValue());
        count++;
        it++;
    }
    ASSERT_EQ(count, compareSet.size());
}

TEST_F(IteratorsTest, ScanNodesByLabelIteratorTest) {
    auto access = _db->access();
    std::vector<EntityID> compareSet {2, 4, 3, 8, 6, 7};

    auto it = compareSet.begin();
    size_t count = 0;
    const auto& labelsets = _db->getMetadata()->labelsets();
    const auto labelset = LabelSet::fromList({1});
    const LabelSetID labelsetID = labelsets.get(labelset);

    for (const EntityID id : access.scanNodesByLabel(labelsetID)) {
        ASSERT_EQ(it->getValue(), id.getValue());
        ASSERT_EQ(it->getValue(), id.getValue());
        count++;
        it++;
    }
    ASSERT_EQ(count, compareSet.size());
}

TEST_F(IteratorsTest, GetEdgesIteratorTest) {
    auto access = _db->access();
    ColumnIDs inputNodeIDs = {1, 2, 3, 8};
    std::vector<TestEdgeRecord> compareSet {
        {2, 3, 4},
        {5, 2, 8},
    };

    auto it = compareSet.begin();
    size_t count = 0;
    spdlog::info("-- Out edges");
    for (const EdgeRecord& v : access.getOutEdges(&inputNodeIDs)) {
        spdlog::info("[{}: {}->{}]", v._edgeID, v._nodeID, v._otherID);
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

    spdlog::info("-- In edges");
    for (const EdgeRecord& v : access.getInEdges(&inputNodeIDs)) {
        spdlog::info("[{}: {}->{}]", v._edgeID, v._otherID, v._nodeID);
        ASSERT_EQ(it->_nodeID.getValue(), v._otherID.getValue());
        ASSERT_EQ(it->_otherID.getValue(), v._nodeID.getValue());
        count++;
        it++;
    }

    ASSERT_EQ(count, compareSet.size());
}

TEST_F(IteratorsTest, ScanNodePropertiesIteratorTest) {
    auto access = _db->access();

    {
        std::vector<uint64_t> compareSet {0, 1, 2, 1, 0, 6, 7, 8, 5};
        auto it = compareSet.begin();
        size_t count = 0;
        for (const uint64_t v : access.scanNodeProperties<types::UInt64>(0)) {
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
        for (const std::string& v : access.scanNodeProperties<types::String>(1)) {
            ASSERT_STREQ(it->data(), v.c_str());
            count++;
            it++;
        }
        ASSERT_EQ(count, compareSet.size());
    }
}

TEST_F(IteratorsTest, ScanEdgePropertiesIteratorTest) {
    auto access = _db->access();

    {
        std::vector<uint64_t> compareSet {0, 1, 2, 0, 1, 8, 5, 6, 7};
        auto it = compareSet.begin();
        size_t count = 0;
        for (const uint64_t v : access.scanEdgeProperties<types::UInt64>(0)) {
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
        for (const std::string& v : access.scanEdgeProperties<types::String>(1)) {
            ASSERT_STREQ(it->data(), v.c_str());
            count++;
            it++;
        }
        ASSERT_EQ(count, compareSet.size());
    }
}

TEST_F(IteratorsTest, ScanNodePropertiesByLabelIteratorTest) {
    auto access = _db->access();

    const auto& labelsets = _db->getMetadata()->labelsets();
    const auto labelset = LabelSet::fromList({1});
    const LabelSetID labelsetID = labelsets.get(labelset);

    {
        std::vector<uint64_t> compareSet {2, 0, 1, 5, 7, 8};
        auto it = compareSet.begin();
        size_t count = 0;
        for (const uint64_t v : access.scanNodePropertiesByLabel<types::UInt64>(0, labelsetID)) {
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
        for (const std::string& v : access.scanNodePropertiesByLabel<types::String>(1, labelsetID)) {
            ASSERT_STREQ(it->data(), v.c_str());
            count++;
            it++;
        }
        ASSERT_EQ(count, compareSet.size());
    }
}

TEST_F(IteratorsTest, GetNodePropertiesIteratorTest) {
    auto access = _db->access();
    ColumnIDs inputNodeIDs = {1, 3, 8};

    {
        std::vector<uint64_t> compareSet {1, 1, 5};
        auto it = compareSet.begin();
        size_t count = 0;
        for (const uint64_t v : access.getNodeProperties<types::UInt64>(0, &inputNodeIDs)) {
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
        for (const std::string& v : access.getNodeProperties<types::String>(1, &inputNodeIDs)) {
            ASSERT_STREQ(it->data(), v.c_str());
            count++;
            it++;
        }
        ASSERT_EQ(count, compareSet.size());
    }
}
