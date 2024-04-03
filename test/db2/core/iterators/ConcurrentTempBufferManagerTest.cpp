#include <gtest/gtest.h>
#include <set>

#include "BioLog.h"
#include "ConcurrentTempBufferManager.h"
#include "DB.h"
#include "FileUtils.h"
#include "Reader.h"
#include "TempBuffer.h"
#include "iterators/GetCoreInEdgesIterator.h"
#include "iterators/GetCoreOutEdgesIterator.h"
#include "iterators/GetPatchInEdgesIterator.h"
#include "iterators/GetPatchOutEdgesIterator.h"
#include "iterators/ScanCoreEdgesIterator.h"
#include "iterators/ScanNodesByLabelIterator.h"
#include "iterators/ScanNodesIterator.h"
#include "iterators/ScanPatchEdgesIterator.h"

using namespace db;

class ConcurrentTempBufferManagerTest : public ::testing::Test {
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

        auto bufferManager = access.createConcurrentTempBufferManager();
        TempBuffer& tempData1 = bufferManager.newBuffer(3, 2);
        TempBuffer& tempData2 = bufferManager.newBuffer(2, 3);
        TempBuffer& tempData3 = bufferManager.newBuffer(4, 4);

        tempData1.addNode({0});     // NODE 0
        tempData1.addNode({0});     // NODE 1
        tempData1.addNode({1});     // NODE 2
        tempData1.addEdge(0, 0, 1); // EDGE 0
        tempData1.addEdge(0, 0, 2); // EDGE 1
        tempData1.addProperty<UInt64PropertyType>(0, 0, 0);
        tempData1.addProperty<UInt64PropertyType>(1, 0, 1);
        tempData1.addProperty<UInt64PropertyType>(2, 0, 2);

        tempData2.addNode({0, 1});  // NODE 3
        tempData2.addNode({1});     // NODE 4
        tempData2.addEdge(0, 3, 4); // EDGE 2
        tempData2.addEdge(0, 3, 4); // EDGE 3
        tempData2.addEdge(0, 4, 3); // EDGE 4
        tempData2.addProperty<UInt64PropertyType>(3, 0, 3);
        tempData2.addProperty<UInt64PropertyType>(4, 0, 4);

        tempData3.addNode({0, 1});  // NODE 5
        tempData3.addNode({0});     // NODE 6
        tempData3.addNode({1});     // NODE 7
        tempData3.addNode({1});     // NODE 8
        tempData3.addEdge(0, 6, 4); // EDGE 5
        tempData3.addEdge(0, 6, 8); // EDGE 6
        tempData3.addEdge(0, 7, 8); // EDGE 7
        tempData3.addEdge(0, 2, 5); // EDGE 8
        tempData3.addProperty<UInt64PropertyType>(5, 0, 5);
        tempData3.addProperty<UInt64PropertyType>(6, 0, 6);
        tempData3.addProperty<UInt64PropertyType>(7, 0, 7);
        tempData3.addProperty<UInt64PropertyType>(8, 0, 8);

        _finalToTmpNodeID[0] = 0;
        _finalToTmpNodeID[1] = 1;
        _finalToTmpNodeID[2] = 6;
        _finalToTmpNodeID[3] = 2;
        _finalToTmpNodeID[4] = 4;
        _finalToTmpNodeID[5] = 7;
        _finalToTmpNodeID[6] = 8;
        _finalToTmpNodeID[7] = 3;
        _finalToTmpNodeID[8] = 5;

        _finalToTmpEdgeID[0] = 0;
        _finalToTmpEdgeID[1] = 1;
        _finalToTmpEdgeID[2] = 5;
        _finalToTmpEdgeID[3] = 6;
        _finalToTmpEdgeID[4] = 8;
        _finalToTmpEdgeID[5] = 4;
        _finalToTmpEdgeID[6] = 7;
        _finalToTmpEdgeID[7] = 2;
        _finalToTmpEdgeID[8] = 3;

        std::unique_ptr<TempBuffer> tempData = bufferManager.build();
        access.pushDataPart(*tempData);
    }

    void TearDown() override {
        delete _db;
        Log::BioLog::destroy();
    }

    DB* _db = nullptr;
    std::string _outDir;
    FileUtils::Path _logPath;
    std::unordered_map<EntityID, EntityID> _finalToTmpNodeID;
    std::unordered_map<EntityID, EntityID> _finalToTmpEdgeID;
};

TEST_F(ConcurrentTempBufferManagerTest, ScanCoreEdgesIteratorTest) {
    auto access = _db->access();
    auto reader = access.getReader();
    auto it = reader.getScanCoreEdgesIterator();

    std::string output;
    while (it.isValid()) {
        const auto& v = it.get();
        const auto& [source, target] = v._edgeDir == EdgeDirection::Incoming
                                         ? std::make_pair(v._otherID, v._nodeID)
                                         : std::make_pair(v._nodeID, v._otherID);
        output += std::to_string(_finalToTmpEdgeID.at(v._edgeID).getID());
        output += std::to_string(_finalToTmpNodeID.at(source).getID());
        output += std::to_string(_finalToTmpNodeID.at(target).getID()) + " ";

        it.next();
    }
    std::cout << "Output:" << std::endl;

    Log::BioLog::echo(output);
    ASSERT_STREQ(output.c_str(), "001 102 564 668 825 443 778 234 334 ");
}

TEST_F(ConcurrentTempBufferManagerTest, ScanNodesIteratorTest) {
    auto access = _db->access();
    auto reader = access.getReader();
    auto it = reader.getScanNodesIterator();

    std::string outputFinalIDs;
    std::string outputTempIDs;
    while (it.isValid()) {
        const EntityID v = it.get();
        outputFinalIDs += std::to_string(v.getID());
        outputTempIDs += std::to_string(_finalToTmpNodeID.at(v).getID());

        it.next();
    }
    Log::BioLog::echo(outputFinalIDs);
    Log::BioLog::echo(outputTempIDs);

    ASSERT_STREQ(outputFinalIDs.c_str(), "012345678");
}

TEST_F(ConcurrentTempBufferManagerTest, ScanNodesByLabelIteratorTest) {
    auto access = _db->access();
    auto reader = access.getReader();
    auto it = reader.getScanNodesByLabelIterator({1});

    std::string outputFinalIDs;
    while (it.isValid()) {
        const EntityID v = it.get();
        outputFinalIDs += std::to_string(v.getID());

        it.next();
    }
    Log::BioLog::echo(outputFinalIDs);

    ASSERT_STREQ(outputFinalIDs.c_str(), "345678");
}
