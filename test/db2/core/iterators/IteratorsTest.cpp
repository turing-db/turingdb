#include "BioLog.h"
#include "DB.h"
#include "FileUtils.h"
#include "Reader.h"
#include "TemporaryDataBuffer.h"
#include "iterators/GetCoreEdgesIterator.h"
#include "iterators/GetPatchEdgesIterator.h"
#include "iterators/GetCoreInEdgesIterator.h"
#include "iterators/GetPatchInEdgesIterator.h"
#include "iterators/GetCoreOutEdgesIterator.h"
#include "iterators/GetPatchOutEdgesIterator.h"
#include "iterators/ScanCoreEdgesIterator.h"
#include "iterators/ScanPatchEdgesIterator.h"
#include "iterators/ScanNodesIterator.h"

#include <gtest/gtest.h>

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

        _db = new DB;
        std::set<EntityID> nodeIDs;
        std::set<EntityID> edgeIDs;

        // First node and edge IDs: 0, 0
        TemporaryDataBuffer tempData1 = _db->createTempBuffer();
        nodeIDs.insert(tempData1.addNode());        // Node 0
        nodeIDs.insert(tempData1.addNode());        // Node 1
        nodeIDs.insert(tempData1.addNode());        // Node 2
        edgeIDs.insert(tempData1.addEdge(0, 0, 1)); // Edge 0
        edgeIDs.insert(tempData1.addEdge(0, 0, 2)); // Edge 1

        // Concurrent writing
        // First node and edge IDs: 0, 0
        TemporaryDataBuffer tempData2 = _db->createTempBuffer();
        nodeIDs.insert(tempData2.addNode());        // Node 3
        nodeIDs.insert(tempData2.addNode());        // Node 4
        edgeIDs.insert(tempData2.addEdge(0, 0, 1)); // Edge 2 [3->4]
        edgeIDs.insert(tempData2.addEdge(0, 0, 1)); // Edge 3 [3->4]
        edgeIDs.insert(tempData2.addEdge(0, 1, 0)); // Edge 4 [4->3]

        _db->pushDataPart(tempData1);
        _db->pushDataPart(tempData2);

        // First node and edge IDs: 5, 5
        // Empty buffer
        TemporaryDataBuffer tempData3 = _db->createTempBuffer();

        _db->pushDataPart(tempData3);

        // First node and edge IDs: 5, 5
        TemporaryDataBuffer tempData4 = _db->createTempBuffer();
        nodeIDs.insert(tempData4.addNode()); // Node 5
        nodeIDs.insert(tempData4.addNode()); // Node 6
        nodeIDs.insert(tempData4.addNode()); // Node 7
        nodeIDs.insert(tempData4.addNode()); // Node 8
        // Reference node in previous datapart
        edgeIDs.insert(tempData4.addEdge(0, 6, 3)); // Edge 5
        edgeIDs.insert(tempData4.addEdge(0, 6, 8)); // Edge 6
        edgeIDs.insert(tempData4.addEdge(0, 7, 8)); // Edge 7
        // Reference node in previous datapart
        edgeIDs.insert(tempData4.addEdge(0, 2, 8)); // Edge 8

        _db->pushDataPart(tempData4);
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
    auto it = reader.getScanCoreEdgesIterator();

    std::string output;
    while (it.isValid()) {
        const auto& v = it.get();
        const auto& [source, target] = v._edgeDir == EdgeDirection::Incoming
                                         ? std::make_pair(v._otherID, v._nodeID)
                                         : std::make_pair(v._nodeID, v._otherID);
        output += std::to_string(v._edgeID.getID());
        output += std::to_string(source.getID());
        output += std::to_string(target.getID());

        it.next();
    }

    Log::BioLog::echo(output);

    ASSERT_STREQ(output.c_str(), "001102234334443563668778");
}

TEST_F(IteratorsTest, ScanPatchEdgesIteratorTest) {
    auto access = _db->access();
    auto reader = access.getReader();
    auto coreIt = reader.getScanCoreEdgesIterator();
    auto patchIt = reader.getScanPatchEdgesIterator();

    std::string output;
    while (coreIt.isValid()) {
        const auto& v = coreIt.get();
        const auto& [source, target] = v._edgeDir == EdgeDirection::Incoming
                                         ? std::make_pair(v._otherID, v._nodeID)
                                         : std::make_pair(v._nodeID, v._otherID);
        output += std::to_string(v._edgeID.getID());
        output += std::to_string(source.getID());
        output += std::to_string(target.getID());

        coreIt.next();
    }

    ASSERT_STREQ(output.c_str(), "001102234334443563668778");

    while (patchIt.isValid()) {
        const auto& v = patchIt.get();
        const auto& [source, target] = v._edgeDir == EdgeDirection::Incoming
                                         ? std::make_pair(v._otherID, v._nodeID)
                                         : std::make_pair(v._nodeID, v._otherID);
        output += std::to_string(v._edgeID.getID());
        output += std::to_string(source.getID());
        output += std::to_string(target.getID());

        patchIt.next();
    }

    Log::BioLog::echo(output);

    ASSERT_STREQ(output.c_str(), "001102234334443563668778828");
}

TEST_F(IteratorsTest, ScanNodesIteratorTest) {
    auto access = _db->access();
    auto reader = access.getReader();
    auto it = reader.getScanNodesIterator();

    std::string output;
    while (it.isValid()) {
        const EntityID v = it.get();
        output += std::to_string(v.getID());

        it.next();
    }

    Log::BioLog::echo(output);

    ASSERT_STREQ(output.c_str(), "012345678");
}

TEST_F(IteratorsTest, GetCoreEdgesIteratorTest) {
    auto access = _db->access();
    auto reader = access.getReader();
    ColumnNodes inputNodeIDs = {1, 3, 8};
    auto it = reader.getGetCoreEdgesIterator(&inputNodeIDs);

    std::string output;
    while (it.isValid()) {
        const auto& v = it.get();
        const auto& [source, target] = v._edgeDir == EdgeDirection::Incoming
                                         ? std::make_pair(v._otherID, v._nodeID)
                                         : std::make_pair(v._nodeID, v._otherID);
        output += std::to_string(v._edgeID.getID());
        output += std::to_string(source.getID());
        output += std::to_string(target.getID());

        it.next();
    }

    Log::BioLog::echo(output);

    ASSERT_STREQ(output.c_str(), "234334001443668778828");
}

TEST_F(IteratorsTest, GetPatchEdgesIteratorTest) {
    auto access = _db->access();
    auto reader = access.getReader();
    ColumnNodes inputNodeIDs = {1, 3, 8};
    auto coreIt = reader.getGetCoreEdgesIterator(&inputNodeIDs);
    auto patchIt = reader.getGetPatchEdgesIterator(&inputNodeIDs);

    std::string output;

    while (coreIt.isValid()) {
        const auto& v = coreIt.get();
        const auto& [source, target] = v._edgeDir == EdgeDirection::Incoming
                                         ? std::make_pair(v._otherID, v._nodeID)
                                         : std::make_pair(v._nodeID, v._otherID);
        output += std::to_string(v._edgeID.getID());
        output += std::to_string(source.getID());
        output += std::to_string(target.getID());

        coreIt.next();
    }

    ASSERT_STREQ(output.c_str(), "234334001443668778828");

    while (patchIt.isValid()) {
        const auto& v = patchIt.get();
        const auto& [source, target] = v._edgeDir == EdgeDirection::Incoming
                                         ? std::make_pair(v._otherID, v._nodeID)
                                         : std::make_pair(v._nodeID, v._otherID);
        output += std::to_string(v._edgeID.getID());
        output += std::to_string(source.getID());
        output += std::to_string(target.getID());

        patchIt.next();
    }

    Log::BioLog::echo(output);

    ASSERT_STREQ(output.c_str(), "234334001443668778828563");
}

TEST_F(IteratorsTest, GetCoreInEdgesIteratorTest) {
    auto access = _db->access();
    auto reader = access.getReader();
    ColumnNodes inputNodeIDs = {1, 3, 8};
    auto it = reader.getGetCoreInEdgesIterator(&inputNodeIDs);

    std::string output;
    while (it.isValid()) {
        const auto& v = it.get();
        const auto& [source, target] = v._edgeDir == EdgeDirection::Incoming
                                         ? std::make_pair(v._otherID, v._nodeID)
                                         : std::make_pair(v._nodeID, v._otherID);
        output += std::to_string(v._edgeID.getID());
        output += std::to_string(source.getID());
        output += std::to_string(target.getID());

        it.next();
    }

    Log::BioLog::echo(output);

    ASSERT_STREQ(output.c_str(), "001443668778828");
}

TEST_F(IteratorsTest, GetPatchInEdgesIteratorTest) {
    auto access = _db->access();
    auto reader = access.getReader();
    ColumnNodes inputNodeIDs = {1, 3, 8};
    auto coreIt = reader.getGetCoreInEdgesIterator(&inputNodeIDs);
    auto patchIt = reader.getGetPatchInEdgesIterator(&inputNodeIDs);

    std::string output;

    while (coreIt.isValid()) {
        const auto& v = coreIt.get();
        const auto& [source, target] = v._edgeDir == EdgeDirection::Incoming
                                         ? std::make_pair(v._otherID, v._nodeID)
                                         : std::make_pair(v._nodeID, v._otherID);
        output += std::to_string(v._edgeID.getID());
        output += std::to_string(source.getID());
        output += std::to_string(target.getID());

        coreIt.next();
    }

    ASSERT_STREQ(output.c_str(), "001443668778828");

    while (patchIt.isValid()) {
        const auto& v = patchIt.get();
        const auto& [source, target] = v._edgeDir == EdgeDirection::Incoming
                                         ? std::make_pair(v._otherID, v._nodeID)
                                         : std::make_pair(v._nodeID, v._otherID);
        output += std::to_string(v._edgeID.getID());
        output += std::to_string(source.getID());
        output += std::to_string(target.getID());

        patchIt.next();
    }

    Log::BioLog::echo(output);

    ASSERT_STREQ(output.c_str(), "001443668778828563");
}

TEST_F(IteratorsTest, GetCoreOutEdgesIteratorTest) {
    auto access = _db->access();
    auto reader = access.getReader();
    ColumnNodes inputNodeIDs = {1, 3, 8};
    auto it = reader.getGetCoreOutEdgesIterator(&inputNodeIDs);

    std::string output;
    while (it.isValid()) {
        const auto& v = it.get();
        const auto& [source, target] = v._edgeDir == EdgeDirection::Incoming
                                         ? std::make_pair(v._otherID, v._nodeID)
                                         : std::make_pair(v._nodeID, v._otherID);
        output += std::to_string(v._edgeID.getID());
        output += std::to_string(source.getID());
        output += std::to_string(target.getID());

        it.next();
    }

    Log::BioLog::echo(output);

    ASSERT_STREQ(output.c_str(), "234334");
}

TEST_F(IteratorsTest, GetPatchOutEdgesIteratorTest) {
    auto access = _db->access();
    auto reader = access.getReader();
    ColumnNodes inputNodeIDs = {1, 2, 3, 8};
    auto coreIt = reader.getGetCoreOutEdgesIterator(&inputNodeIDs);
    auto patchIt = reader.getGetPatchOutEdgesIterator(&inputNodeIDs);

    std::string output;

    while (coreIt.isValid()) {
        const auto& v = coreIt.get();
        const auto& [source, target] = v._edgeDir == EdgeDirection::Incoming
                                         ? std::make_pair(v._otherID, v._nodeID)
                                         : std::make_pair(v._nodeID, v._otherID);
        output += std::to_string(v._edgeID.getID());
        output += std::to_string(source.getID());
        output += std::to_string(target.getID());

        coreIt.next();
    }

    ASSERT_STREQ(output.c_str(), "234334");

    while (patchIt.isValid()) {
        const auto& v = patchIt.get();
        const auto& [source, target] = v._edgeDir == EdgeDirection::Incoming
                                         ? std::make_pair(v._otherID, v._nodeID)
                                         : std::make_pair(v._nodeID, v._otherID);
        output += std::to_string(v._edgeID.getID());
        output += std::to_string(source.getID());
        output += std::to_string(target.getID());

        patchIt.next();
    }

    Log::BioLog::echo(output);

    ASSERT_STREQ(output.c_str(), "234334828");
}
