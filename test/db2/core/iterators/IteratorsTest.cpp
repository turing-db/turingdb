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
        EntityTypeID edgeTypeID = 0;
        PropertyTypeID tmpNodeIDProperty = 0;       // Mandatory property in all DataParts
        PropertyTypeID tmpNodeIDStringProperty = 1; // Optional property in DataPart 2

        //
        /* FIRST BUFFER */
        //
        TemporaryDataBuffer tempData1 = access.createTempBuffer();

        {
            // NODE 0 (temp ID: 0)
            EntityID tmpNodeID = tempData1.addNode(LabelSet {0});
            tempData1.addProperty<UInt64PropertyType>(
                tmpNodeID, tmpNodeIDProperty, 0);
            tempData1.addProperty<StringPropertyType>(
                tmpNodeID, tmpNodeIDStringProperty, "TmpID0");
        }

        {
            // NODE 1 (temp ID: 1)
            EntityID tmpNodeID = tempData1.addNode(LabelSet {0});
            tempData1.addProperty<UInt64PropertyType>(
                tmpNodeID, tmpNodeIDProperty, 1);
            tempData1.addProperty<StringPropertyType>(
                tmpNodeID, tmpNodeIDStringProperty, "TmpID1");
        }

        {
            // NODE 2 (temp ID: 2)
            EntityID tmpNodeID = tempData1.addNode(LabelSet {1});
            tempData1.addProperty<UInt64PropertyType>(
                tmpNodeID, tmpNodeIDProperty, 2);
            tempData1.addProperty<StringPropertyType>(
                tmpNodeID, tmpNodeIDStringProperty, "TmpID2");
        }

        // EDGE 0 [0->1] (temp ID: 0)
        tempData1.addEdge(/*typeID=*/edgeTypeID, /*source=*/0, /*target=*/1);

        // EDGE 1 [0->2] (temp ID: 1)
        tempData1.addEdge(/*typeID=*/edgeTypeID, /*source=*/0, /*target=*/2);

        //
        /* SECOND BUFFER (Concurrent to the first one) */
        //
        TemporaryDataBuffer tempData2 = access.createTempBuffer();

        {
            // NODE 4 (temp ID: 0))
            EntityID tmpNodeID = tempData2.addNode(LabelSet {0, 1});
            tempData2.addProperty<UInt64PropertyType>(
                tmpNodeID, tmpNodeIDProperty, 0);
            tempData2.addProperty<StringPropertyType>(
                tmpNodeID, tmpNodeIDStringProperty, "TmpID0");
        }

        {
            // NODE 3 (temp ID: 1)
            EntityID tmpNodeID = tempData2.addNode(LabelSet {1});
            tempData2.addProperty<UInt64PropertyType>(
                tmpNodeID, tmpNodeIDProperty, 1);
            // This node does not have a tmpNodeIDStringProperty prop
        }

        // EDGE 3 [4->3] (temp ID: 0 [0->1])
        tempData2.addEdge(/*typeID=*/edgeTypeID, /*source=*/0, /*target=*/1);

        // EDGE 4 [4->3] (temp ID: 1 [0->1])
        tempData2.addEdge(/*typeID=*/edgeTypeID, /*source=*/0, /*target=*/1);

        // EDGE 2 [3->4] (temp ID: 2 [1->0])
        tempData2.addEdge(/*typeID=*/edgeTypeID, /*source=*/1, /*target=*/0);

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
            EntityID tmpNodeID = tempData4.addNode(LabelSet {0, 1});
            tempData4.addProperty<UInt64PropertyType>(
                tmpNodeID, tmpNodeIDProperty, 5);
            tempData4.addProperty<StringPropertyType>(
                tmpNodeID, tmpNodeIDStringProperty, "TmpID5");
        }

        {
            // NODE 5 (temp ID: 6)
            EntityID tmpNodeID = tempData4.addNode(LabelSet {0});
            tempData4.addProperty<UInt64PropertyType>(
                tmpNodeID, tmpNodeIDProperty, 6);
            tempData4.addProperty<StringPropertyType>(
                tmpNodeID, tmpNodeIDStringProperty, "TmpID6");
        }

        {
            // NODE 6 (temp ID: 7)
            EntityID tmpNodeID = tempData4.addNode(LabelSet {1});
            tempData4.addProperty<UInt64PropertyType>(
                tmpNodeID, tmpNodeIDProperty, 7);
            tempData4.addProperty<StringPropertyType>(
                tmpNodeID, tmpNodeIDStringProperty, "TmpID7");
        }

        {
            // NODE 7 (temp ID: 8)
            EntityID tmpNodeID = tempData4.addNode(LabelSet {1});
            tempData4.addProperty<UInt64PropertyType>(
                tmpNodeID, tmpNodeIDProperty, 8);
            tempData4.addProperty<StringPropertyType>(
                tmpNodeID, tmpNodeIDStringProperty, "TmpID8");
        }

        // Reference node in previous datapart
        tempData4.addEdge(edgeTypeID, 6, 4); // Edge 5 [5->4] (temp ID: 5 [6->4])
        tempData4.addEdge(edgeTypeID, 6, 8); // Edge 6 [5->7] (temp ID: 6 [6->8])
        tempData4.addEdge(edgeTypeID, 7, 8); // Edge 7 [6->7] (temp ID: 7 [7->8])
        // Reference node in previous datapart
        tempData4.addEdge(edgeTypeID, 2, 5); // Edge 8 [2->8] (temp ID: 8 [2->5])

        access.pushDataPart(tempData4);
    }

    void TearDown() override {
        delete _db;
        Log::BioLog::destroy();
    }

    DB* _db = nullptr;
    std::string _outDir;
    FileUtils::Path _logPath;
};

TEST_F(IteratorsTest, ScanCoreEdgesIteratorTest) {
    auto access = _db->access();
    auto reader = access.getReader();
    std::string output;

    for (const EdgeRecord& v : reader.scanCoreEdges()) {
        output += std::to_string(v._edgeID)
                + std::to_string(v._nodeID)
                + std::to_string(v._otherID)
                + " ";
    }
    Log::BioLog::echo(output);
    ASSERT_STREQ(output.c_str(), "001 102 234 343 443 554 657 767 ");
}

TEST_F(IteratorsTest, ScanPatchEdgesIteratorTest) {
    auto access = _db->access();
    auto reader = access.getReader();
    std::string output;

    for (const EdgeRecord& v : reader.scanCoreEdges()) {
        output += std::to_string(v._edgeID)
                + std::to_string(v._nodeID)
                + std::to_string(v._otherID)
                + " ";
    }
    ASSERT_STREQ(output.c_str(), "001 102 234 343 443 554 657 767 ");

    for (const EdgeRecord& v : reader.scanPatchEdges()) {
        output += std::to_string(v._edgeID)
                + std::to_string(v._nodeID)
                + std::to_string(v._otherID)
                + " ";
    }
    Log::BioLog::echo(output);
    ASSERT_STREQ(output.c_str(), "001 102 234 343 443 554 657 767 828 ");
}

TEST_F(IteratorsTest, ScanNodesIteratorTest) {
    auto access = _db->access();
    auto reader = access.getReader();
    std::string output;

    for (const EntityID id : reader.scanNodes()) {
        output += std::to_string(id);
    }
    Log::BioLog::echo(output);
    ASSERT_STREQ(output.c_str(), "012345678");
}

TEST_F(IteratorsTest, ScanNodesByLabelIteratorTest) {
    auto access = _db->access();
    auto reader = access.getReader();
    std::string output;

    for (const EntityID id : reader.scanNodesByLabel({1})) {
        output += std::to_string(id);
    }
    Log::BioLog::echo(output);
    ASSERT_STREQ(output.c_str(), "234678");
}

TEST_F(IteratorsTest, GetCoreEdgesIteratorTest) {
    auto access = _db->access();
    auto reader = access.getReader();
    ColumnNodes inputNodeIDs = {1, 3, 8};
    std::string output;

    for (const EdgeRecord& v : reader.getCoreOutEdges(&inputNodeIDs)) {
        output += std::to_string(v._edgeID)
                + std::to_string(v._nodeID)
                + std::to_string(v._otherID)
                + " ";
    }
    Log::BioLog::echo(output);
    ASSERT_STREQ(output.c_str(), "234 ");

    for (const EdgeRecord& v : reader.getCoreInEdges(&inputNodeIDs)) {
        output += std::to_string(v._edgeID)
                + std::to_string(v._nodeID)
                + std::to_string(v._otherID)
                + " ";
    }
    Log::BioLog::echo(output);
    ASSERT_STREQ(output.c_str(), "234 010 334 434 ");
}

TEST_F(IteratorsTest, GetPatchEdgesIteratorTest) {
    auto access = _db->access();
    auto reader = access.getReader();
    ColumnNodes inputNodeIDs = {1, 3, 8};
    std::string output;

    for (const EdgeRecord& v : reader.getCoreOutEdges(&inputNodeIDs)) {
        output += std::to_string(v._edgeID)
                + std::to_string(v._nodeID)
                + std::to_string(v._otherID)
                + " ";
    }

    for (const EdgeRecord& v : reader.getCoreInEdges(&inputNodeIDs)) {
        output += std::to_string(v._edgeID)
                + std::to_string(v._nodeID)
                + std::to_string(v._otherID)
                + " ";
    }

    for (const EdgeRecord& v : reader.getPatchOutEdges(&inputNodeIDs)) {
        output += std::to_string(v._edgeID)
                + std::to_string(v._nodeID)
                + std::to_string(v._otherID)
                + " ";
    }

    for (const EdgeRecord& v : reader.getPatchInEdges(&inputNodeIDs)) {
        output += std::to_string(v._edgeID)
                + std::to_string(v._nodeID)
                + std::to_string(v._otherID)
                + " ";
    }
    Log::BioLog::echo(output);
    ASSERT_STREQ(output.c_str(), "234 010 334 434 882 ");
}

TEST_F(IteratorsTest, GetCoreInEdgesIteratorTest) {
    auto access = _db->access();
    auto reader = access.getReader();
    ColumnNodes inputNodeIDs = {1, 3, 8};
    std::string output;

    for (const EdgeRecord& v : reader.getCoreInEdges(&inputNodeIDs)) {
        output += std::to_string(v._edgeID)
                + std::to_string(v._nodeID)
                + std::to_string(v._otherID)
                + " ";
    }
    Log::BioLog::echo(output);
    ASSERT_STREQ(output.c_str(), "010 334 434 ");
}

TEST_F(IteratorsTest, GetPatchInEdgesIteratorTest) {
    auto access = _db->access();
    auto reader = access.getReader();
    ColumnNodes inputNodeIDs = {1, 3, 8};
    std::string output;

    for (const EdgeRecord& v : reader.getCoreInEdges(&inputNodeIDs)) {
        output += std::to_string(v._edgeID)
                + std::to_string(v._nodeID)
                + std::to_string(v._otherID)
                + " ";
    }
    ASSERT_STREQ(output.c_str(), "010 334 434 ");

    for (const EdgeRecord& v : reader.getPatchInEdges(&inputNodeIDs)) {
        output += std::to_string(v._edgeID)
                + std::to_string(v._nodeID)
                + std::to_string(v._otherID)
                + " ";
    }
    Log::BioLog::echo(output);
    ASSERT_STREQ(output.c_str(), "010 334 434 882 ");
}

TEST_F(IteratorsTest, GetCoreOutEdgesIteratorTest) {
    auto access = _db->access();
    auto reader = access.getReader();
    ColumnNodes inputNodeIDs = {1, 3, 8};
    std::string output;

    for (const EdgeRecord& v : reader.getCoreOutEdges(&inputNodeIDs)) {
        output += std::to_string(v._edgeID)
                + std::to_string(v._nodeID)
                + std::to_string(v._otherID)
                + " ";
    }
    Log::BioLog::echo(output);
    ASSERT_STREQ(output.c_str(), "234 ");
}

TEST_F(IteratorsTest, GetPatchOutEdgesIteratorTest) {
    auto access = _db->access();
    auto reader = access.getReader();
    ColumnNodes inputNodeIDs = {1, 2, 3, 8};
    std::string output;

    for (const EdgeRecord& v : reader.getCoreOutEdges(&inputNodeIDs)) {
        output += std::to_string(v._edgeID)
                + std::to_string(v._nodeID)
                + std::to_string(v._otherID)
                + " ";
    }
    ASSERT_STREQ(output.c_str(), "234 ");

    for (const EdgeRecord& v : reader.getPatchOutEdges(&inputNodeIDs)) {
        output += std::to_string(v._edgeID)
                + std::to_string(v._nodeID)
                + std::to_string(v._otherID)
                + " ";
    }
    Log::BioLog::echo(output);
    ASSERT_STREQ(output.c_str(), "234 828 ");
}

TEST_F(IteratorsTest, ScanNodePropertiesIteratorTest) {
    auto access = _db->access();
    auto reader = access.getReader();
    std::string output;

    auto range = reader.scanNodeProperties<UInt64PropertyType>(0);
    auto it1 = range.begin();
    for (; it1.isValid(); it1.next()) {
        output += std::to_string(it1.get());
    }
    ASSERT_STREQ(output.c_str(), "012106785");

    output.clear();

    for (const std::string& v : reader.scanNodeProperties<StringPropertyType>(1)) {
        output += v + " ";
    }
    ASSERT_STREQ(output.c_str(), "TmpID0 TmpID1 TmpID2 TmpID0 TmpID6 TmpID7 TmpID8 TmpID5 ");
}

TEST_F(IteratorsTest, GetNodePropertiesIteratorTest) {
    auto access = _db->access();
    auto reader = access.getReader();

    std::string output;
    ColumnNodes inputNodeIDs = {1, 3, 8};

    for (const std::string& v : reader.getNodeProperties<StringPropertyType>(1, &inputNodeIDs)) {
        output += v + " ";
    }
    ASSERT_STREQ(output.c_str(), "TmpID1 TmpID5 ");

    output.clear();

    for (uint64_t v : reader.getNodeProperties<UInt64PropertyType>(0, &inputNodeIDs)) {
        output += std::to_string(v);
    }
    ASSERT_STREQ(output.c_str(), "115");
}
