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

        std::set<EntityID> nodeIDs;
        std::set<EntityID> edgeIDs;

        _db = new DB();
        auto access = _db->uniqueAccess();

        // First node and edge IDs: 0, 0
        TemporaryDataBuffer tempData1 = access.createTempBuffer();
        nodeIDs.insert(tempData1.addNode({0}));     // Node 0
        nodeIDs.insert(tempData1.addNode({0}));     // Node 1
        nodeIDs.insert(tempData1.addNode({1}));     // Node 2
        edgeIDs.insert(tempData1.addEdge(0, 0, 1)); // Edge 0
        edgeIDs.insert(tempData1.addEdge(0, 0, 2)); // Edge 1

        // Concurrent writing
        // First node and edge IDs: 0, 0
        TemporaryDataBuffer tempData2 = access.createTempBuffer();
        nodeIDs.insert(tempData2.addNode({0, 1}));  // Node 3
        nodeIDs.insert(tempData2.addNode({1}));     // Node 4
        edgeIDs.insert(tempData2.addEdge(0, 0, 1)); // Edge 2 [3->4]
        edgeIDs.insert(tempData2.addEdge(0, 0, 1)); // Edge 3 [3->4]
        edgeIDs.insert(tempData2.addEdge(0, 1, 0)); // Edge 4 [4->3]

        access.pushDataPart(tempData1);
        access.pushDataPart(tempData2);

        // First node and edge IDs: 5, 5
        // Empty buffer
        TemporaryDataBuffer tempData3 = access.createTempBuffer();

        access.pushDataPart(tempData3);

        // First node and edge IDs: 5, 5
        TemporaryDataBuffer tempData4 = access.createTempBuffer();
        nodeIDs.insert(tempData4.addNode({0, 1})); // Node 5
        nodeIDs.insert(tempData4.addNode({0}));    // Node 6
        nodeIDs.insert(tempData4.addNode({1}));    // Node 7
        nodeIDs.insert(tempData4.addNode({1}));    // Node 8
        // Reference node in previous datapart
        edgeIDs.insert(tempData4.addEdge(0, 6, 3)); // Edge 5
        edgeIDs.insert(tempData4.addEdge(0, 6, 8)); // Edge 6
        edgeIDs.insert(tempData4.addEdge(0, 7, 8)); // Edge 7
        // Reference node in previous datapart
        edgeIDs.insert(tempData4.addEdge(0, 2, 8)); // Edge 8

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
    auto it = reader.getScanCoreEdgesIterator();

    std::string output;
    while (it.isValid()) {
        const auto& v = it.get();
        const auto& [source, target] = v._edgeDir == EdgeDirection::Incoming
                                         ? std::make_pair(v._otherID, v._nodeID)
                                         : std::make_pair(v._nodeID, v._otherID);
        std::cout << "(" << v._edgeID.getID() << ", " << source.getID() << ", " << target.getID() << ")\n";
        output += std::to_string(v._edgeID.getID());
        output += std::to_string(source.getID());
        output += std::to_string(target.getID()) + " ";

        it.next();
    }

    Log::BioLog::echo(output);
    ASSERT_STREQ(output.c_str(), "001 102 443 234 334 778 563 668 ");
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
        output += std::to_string(target.getID()) + " ";

        coreIt.next();
    }

    ASSERT_STREQ(output.c_str(), "001 102 443 234 334 778 563 668 ");

    while (patchIt.isValid()) {
        const auto& v = patchIt.get();
        const auto& [source, target] = v._edgeDir == EdgeDirection::Incoming
                                         ? std::make_pair(v._otherID, v._nodeID)
                                         : std::make_pair(v._nodeID, v._otherID);
        output += std::to_string(v._edgeID.getID());
        output += std::to_string(source.getID());
        output += std::to_string(target.getID()) + " ";

        patchIt.next();
    }

    Log::BioLog::echo(output);

    ASSERT_STREQ(output.c_str(), "001 102 443 234 334 778 563 668 828 ");
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

TEST_F(IteratorsTest, ScanNodesByLabelIteratorTest) {
    auto access = _db->access();
    auto reader = access.getReader();
    auto it = reader.getScanNodesByLabelIterator({1});

    std::string output;
    while (it.isValid()) {
        const EntityID v = it.get();
        output += std::to_string(v.getID());

        it.next();
    }

    Log::BioLog::echo(output);

    ASSERT_STREQ(output.c_str(), "243785");
}

TEST_F(IteratorsTest, GetCoreEdgesIteratorTest) {
    auto access = _db->access();
    auto reader = access.getReader();
    ColumnNodes inputNodeIDs = {1, 3, 8};
    auto coreOutIt = reader.getGetCoreOutEdgesIterator(&inputNodeIDs);
    auto coreInIt = reader.getGetCoreInEdgesIterator(&inputNodeIDs);

    std::string output;

    while (coreOutIt.isValid()) {
        const auto& v = coreOutIt.get();
        const auto& [source, target] = v._edgeDir == EdgeDirection::Incoming
                                         ? std::make_pair(v._otherID, v._nodeID)
                                         : std::make_pair(v._nodeID, v._otherID);
        output += std::to_string(v._edgeID.getID());
        output += std::to_string(source.getID());
        output += std::to_string(target.getID()) + " ";

        coreOutIt.next();
    }

    while (coreInIt.isValid()) {
        const auto& v = coreInIt.get();
        const auto& [source, target] = v._edgeDir == EdgeDirection::Incoming
                                         ? std::make_pair(v._otherID, v._nodeID)
                                         : std::make_pair(v._nodeID, v._otherID);
        output += std::to_string(v._edgeID.getID());
        output += std::to_string(source.getID());
        output += std::to_string(target.getID()) + " ";

        coreInIt.next();
    }

    Log::BioLog::echo(output);

    ASSERT_STREQ(output.c_str(), "234 334 001 443 668 778 828 ");
}

TEST_F(IteratorsTest, GetPatchEdgesIteratorTest) {
    auto access = _db->access();
    auto reader = access.getReader();
    ColumnNodes inputNodeIDs = {1, 3, 8};
    auto coreOutIt = reader.getGetCoreOutEdgesIterator(&inputNodeIDs);
    auto patchOutIt = reader.getGetPatchOutEdgesIterator(&inputNodeIDs);
    auto coreInIt = reader.getGetCoreInEdgesIterator(&inputNodeIDs);
    auto patchInIt = reader.getGetPatchInEdgesIterator(&inputNodeIDs);

    std::string output;

    while (coreOutIt.isValid()) {
        const auto& v = coreOutIt.get();
        const auto& [source, target] = v._edgeDir == EdgeDirection::Incoming
                                         ? std::make_pair(v._otherID, v._nodeID)
                                         : std::make_pair(v._nodeID, v._otherID);
        output += std::to_string(v._edgeID.getID());
        output += std::to_string(source.getID());
        output += std::to_string(target.getID()) + " ";

        coreOutIt.next();
    }

    while (coreInIt.isValid()) {
        const auto& v = coreInIt.get();
        const auto& [source, target] = v._edgeDir == EdgeDirection::Incoming
                                         ? std::make_pair(v._otherID, v._nodeID)
                                         : std::make_pair(v._nodeID, v._otherID);
        output += std::to_string(v._edgeID.getID());
        output += std::to_string(source.getID());
        output += std::to_string(target.getID()) + " ";

        coreInIt.next();
    }

    while (patchOutIt.isValid()) {
        const auto& v = patchOutIt.get();
        const auto& [source, target] = v._edgeDir == EdgeDirection::Incoming
                                         ? std::make_pair(v._otherID, v._nodeID)
                                         : std::make_pair(v._nodeID, v._otherID);
        output += std::to_string(v._edgeID.getID());
        output += std::to_string(source.getID());
        output += std::to_string(target.getID()) + " ";

        patchOutIt.next();
    }

    while (patchInIt.isValid()) {
        const auto& v = patchInIt.get();
        const auto& [source, target] = v._edgeDir == EdgeDirection::Incoming
                                         ? std::make_pair(v._otherID, v._nodeID)
                                         : std::make_pair(v._nodeID, v._otherID);
        output += std::to_string(v._edgeID.getID());
        output += std::to_string(source.getID());
        output += std::to_string(target.getID()) + " ";

        patchInIt.next();
    }

    Log::BioLog::echo(output);

    ASSERT_STREQ(output.c_str(), "234 334 001 443 668 778 828 563 ");
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
        output += std::to_string(target.getID()) + " ";

        it.next();
    }

    Log::BioLog::echo(output);

    ASSERT_STREQ(output.c_str(), "001 443 668 778 828 ");
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
        output += std::to_string(target.getID()) + " ";

        coreIt.next();
    }

    ASSERT_STREQ(output.c_str(), "001 443 668 778 828 ");

    while (patchIt.isValid()) {
        const auto& v = patchIt.get();
        const auto& [source, target] = v._edgeDir == EdgeDirection::Incoming
                                         ? std::make_pair(v._otherID, v._nodeID)
                                         : std::make_pair(v._nodeID, v._otherID);
        output += std::to_string(v._edgeID.getID());
        output += std::to_string(source.getID());
        output += std::to_string(target.getID()) + " ";

        patchIt.next();
    }

    Log::BioLog::echo(output);

    ASSERT_STREQ(output.c_str(), "001 443 668 778 828 563 ");
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
        output += std::to_string(target.getID()) + " ";

        it.next();
    }

    Log::BioLog::echo(output);

    ASSERT_STREQ(output.c_str(), "234 334 ");
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
        output += std::to_string(target.getID()) + " ";

        coreIt.next();
    }

    ASSERT_STREQ(output.c_str(), "234 334 ");

    while (patchIt.isValid()) {
        const auto& v = patchIt.get();
        const auto& [source, target] = v._edgeDir == EdgeDirection::Incoming
                                         ? std::make_pair(v._otherID, v._nodeID)
                                         : std::make_pair(v._nodeID, v._otherID);
        output += std::to_string(v._edgeID.getID());
        output += std::to_string(source.getID());
        output += std::to_string(target.getID()) + " ";

        patchIt.next();
    }

    Log::BioLog::echo(output);

    ASSERT_STREQ(output.c_str(), "234 334 828 ");
}
