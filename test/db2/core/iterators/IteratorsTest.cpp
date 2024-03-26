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
        std::map<LabelSet, std::string> labelsetNames = {
            {{0},    "{0}"   },
            {{1},    "{1}"   },
            {{0, 1}, "{0, 1}"},
        };

        Log::BioLog::echo("Labelsets:");
        for (const auto& [labelset, labelsetName] : labelsetNames) {
            Log::BioLog::echo(labelsetName);
        }

        // First node and edge IDs: 0, 0
        TemporaryDataBuffer tempData1 = access.createTempBuffer();
        tempData1.addNode({0});     // Node 0        (temp ID: 0)
        tempData1.addNode({0});     // Node 1        (temp ID: 1)
        tempData1.addNode({1});     // Node 2        (temp ID: 2)
        tempData1.addEdge(0, 0, 1); // Edge 0 [0->1] (temp ID: 0)
        tempData1.addEdge(0, 0, 2); // Edge 1 [0->2] (temp ID: 1)
        _finalToTmpNodeID[0] = 0;
        _finalToTmpNodeID[1] = 1;
        _finalToTmpNodeID[2] = 2;
        _finalToTmpEdgeID[0] = 0;
        _finalToTmpEdgeID[1] = 1;

        // Concurrent writing
        // First node and edge IDs: 0, 0
        TemporaryDataBuffer tempData2 = access.createTempBuffer();
        tempData2.addNode({0, 1});  // Node 4        (temp ID: 3)
        tempData2.addNode({1});     // Node 3        (temp ID: 4)
        tempData2.addEdge(0, 0, 1); // Edge 3 [4->3] (temp ID: 2 [3->4])
        tempData2.addEdge(0, 0, 1); // Edge 4 [4->3] (temp ID: 3 [3->4])
        tempData2.addEdge(0, 1, 0); // Edge 2 [3->4] (temp ID: 4 [4->3])
        _finalToTmpNodeID[4] = 3;
        _finalToTmpNodeID[3] = 4;
        _finalToTmpNodeID[2] = 2;
        _finalToTmpEdgeID[2] = 4;
        _finalToTmpEdgeID[3] = 2;
        _finalToTmpEdgeID[4] = 3;

        access.pushDataPart(tempData1);
        access.pushDataPart(tempData2);

        // First node and edge IDs: 5, 5
        // Empty buffer
        TemporaryDataBuffer tempData3 = access.createTempBuffer();

        access.pushDataPart(tempData3);

        // First node and edge IDs: 5, 5
        TemporaryDataBuffer tempData4 = access.createTempBuffer();
        tempData4.addNode({0, 1}); // Node 8        (temp ID: 5)
        tempData4.addNode({0});    // Node 5        (temp ID: 6)
        tempData4.addNode({1});    // Node 6        (temp ID: 7)
        tempData4.addNode({1});    // Node 7        (temp ID: 8)
        // Reference node in previous datapart
        tempData4.addEdge(0, 6, 4); // Edge 5 [5->4] (temp ID: 5 [6->4])
        tempData4.addEdge(0, 6, 8); // Edge 6 [5->7] (temp ID: 6 [6->8])
        tempData4.addEdge(0, 7, 8); // Edge 7 [6->7] (temp ID: 7 [7->8])
        // Reference node in previous datapart
        tempData4.addEdge(0, 2, 5); // Edge 8 [2->8] (temp ID: 8 [2->5])

        _finalToTmpNodeID[5] = 6;
        _finalToTmpNodeID[6] = 7;
        _finalToTmpNodeID[7] = 8;
        _finalToTmpNodeID[8] = 5;
        _finalToTmpEdgeID[5] = 5;
        _finalToTmpEdgeID[6] = 6;
        _finalToTmpEdgeID[7] = 7;
        _finalToTmpEdgeID[8] = 8;
        access.pushDataPart(tempData4);
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

TEST_F(IteratorsTest, ScanCoreEdgesIteratorTest) {
    auto access = _db->access();
    auto reader = access.getReader();
    auto it = reader.getScanCoreEdgesIterator();

    std::string output;
    while (it.isValid()) {
        std::cout << "Edge iterator is valid" << std::endl;
        const auto& v = it.get();
        const auto& [source, target] = v._edgeDir == EdgeDirection::Incoming
                                         ? std::make_pair(v._otherID, v._nodeID)
                                         : std::make_pair(v._nodeID, v._otherID);
        std::cout << "(" << v._edgeID.getID() << ", " << source.getID() << ", " << target.getID() << ")\n";
        output += std::to_string(v._edgeID.getID());
        output += std::to_string(source.getID()) ;
        output += std::to_string(target.getID()) + " ";

        it.next();
    }
    std::cout << "Output:" << std::endl;

    Log::BioLog::echo(output);
    ASSERT_STREQ(output.c_str(), "001 102 234 343 443 554 657 767 ");
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

    ASSERT_STREQ(output.c_str(), "001 102 234 343 443 554 657 767 ");

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

    ASSERT_STREQ(output.c_str(), "001 102 234 343 443 554 657 767 828 ");
}

TEST_F(IteratorsTest, ScanNodesIteratorTest) {
    auto access = _db->access();
    auto reader = access.getReader();
    auto it = reader.getScanNodesIterator();

    std::string outputFinalIDs;
    std::string outputTempIDs;
    while (it.isValid()) {
        const EntityID v = it.get();
        outputFinalIDs += std::to_string(v.getID());
        outputTempIDs += std::to_string(_finalToTmpNodeID[v].getID());

        it.next();
    }

    Log::BioLog::echo(outputFinalIDs);

    ASSERT_STREQ(outputFinalIDs.c_str(), "012345678");
    ASSERT_STREQ(outputTempIDs.c_str(), "012436785");
}

TEST_F(IteratorsTest, ScanNodesByLabelIteratorTest) {
    auto access = _db->access();
    auto reader = access.getReader();
    auto it = reader.getScanNodesByLabelIterator({1});

    std::string outputFinalIDs;
    std::string outputTempIDs;
    while (it.isValid()) {
        const EntityID v = it.get();
        outputFinalIDs += std::to_string(v.getID());
        outputTempIDs += std::to_string(_finalToTmpNodeID[v].getID());

        it.next();
    }

    Log::BioLog::echo(outputFinalIDs);

    ASSERT_STREQ(outputFinalIDs.c_str(), "234678");
    ASSERT_STREQ(outputTempIDs.c_str(), "243785");
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

    Log::BioLog::echo(output);
    ASSERT_STREQ(output.c_str(), "234 ");

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

    ASSERT_STREQ(output.c_str(), "234 001 343 443 ");
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

    ASSERT_STREQ(output.c_str(), "234 001 343 443 828 ");
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

    ASSERT_STREQ(output.c_str(), "001 343 443 ");
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

    ASSERT_STREQ(output.c_str(), "001 343 443 ");

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

    ASSERT_STREQ(output.c_str(), "001 343 443 828 ");
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

    ASSERT_STREQ(output.c_str(), "234 ");
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

    ASSERT_STREQ(output.c_str(), "234 ");

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

    ASSERT_STREQ(output.c_str(), "234 828 ");
}
