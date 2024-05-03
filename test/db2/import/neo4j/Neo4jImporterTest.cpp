#include <gtest/gtest.h>
#include <numeric>
#include <regex>
#include <span>
#include <thread>

#include "BioAssert.h"
#include "BioLog.h"
#include "ConcurrentWriter.h"
#include "DB.h"
#include "DataBuffer.h"
#include "EdgeView.h"
#include "FileUtils.h"
#include "JobSystem.h"
#include "Neo4jImporter.h"
#include "NodeView.h"
#include "PerfStat.h"
#include "Reader.h"
#include "ScanCoreEdgesIterator.h"
#include "Time.h"

using namespace db;

class Neo4jImporterTest : public ::testing::Test {
protected:
    void SetUp() override {
        const testing::TestInfo* const testInfo =
            testing::UnitTest::GetInstance()->current_test_info();

        _outDir = testInfo->test_suite_name();
        _outDir += "_";
        _outDir += testInfo->name();
        _outDir += ".out";
        _logPath = FileUtils::Path(_outDir) / "log";
        _perfPath = FileUtils::Path(_outDir) / "perf";

        if (FileUtils::exists(_outDir)) {
            FileUtils::removeDirectory(_outDir);
        }
        FileUtils::createDirectory(_outDir);

        Log::BioLog::init();
        Log::BioLog::openFile(_logPath.string());
        PerfStat::init(_perfPath);

        _db = std::make_unique<DB>();
    }

    void TearDown() override {
        Log::BioLog::destroy();
        PerfStat::destroy();
    }

    void findApoe() {
        auto access = _db->access();
        auto reader = access.getReader();

        std::string_view apoe4ReactomeID = "R-HSA-9711070";
        PropertyType displayNameType = access.getPropertyType("displayName (String)");
        PropertyType stIDType = access.getPropertyType("stId (String)");
        // const Labelset apoeLabelset = access.getLabelset({
        //     "DatabaseObject",
        //     "PhysicalEntity",
        //     "GenomeEncodedEntity",
        //     "EntityWithAccessionedSequence",
        // });
        //{
        //     std::vector<LabelID> labelIDs;
        //     apoeLabelset.decompose(labelIDs);
        //     std::cout << "Requested labels: ";
        //     for (const auto& id : labelIDs) {
        //         std::cout << " " << id.getValue();
        //     }
        //     std::cout << std::endl;
        // }

        auto it = reader.scanNodeProperties<types::String>(stIDType._id).begin();
        const auto findReactomeID = [&]() {
            size_t i = 0;
            for (; it.isValid(); it.next()) {
                const types::String::Primitive& stID = it.get();
                if (stID == apoe4ReactomeID) {
                    EntityID apoeID = it.getCurrentNodeID();
                    std::cout << "Found APOE-4 at ID: " << apoeID.getValue()
                              << " in " << i << " string comparisons"
                              << std::endl;
                    return apoeID;
                }
                i++;
            }
            std::cout << "Could not find APOE-4" << std::endl;
            throw;
        };

        auto t0 = Clock::now();
        EntityID apoeID = findReactomeID();
        auto t1 = Clock::now();
        std::cout << "found APOE-4 in: " << duration<Milliseconds>(t0, t1) << " ms" << std::endl;

        t0 = Clock::now();
        NodeView apoe = reader.getNodeView(apoeID);
        t1 = Clock::now();
        std::cout << "Got APOE-4 view in: " << duration<Microseconds>(t0, t1) << " us" << std::endl;
        std::vector<LabelID> labelIDs;
        // apoe.labelset().decompose(labelIDs);
        // std::cout << "APOE-4 has labels: " << std::endl;
        // for (const LabelID id : labelIDs) {
        //     std::cout << "  - " << access.getLabelName(id) << std::endl;
        // }
        const auto& apoeEdges = apoe.edges();
        const auto& apoeProperties = apoe.properties();
        const auto printProperties = [&access](const EntityPropertyView& view) {
            // if (!view.uint64s().empty()) {
            std::cout << "  #  UInt64 properties" << std::endl;
            for (const auto& prop : view.uint64s()) {
                const std::string_view ptName = access.getPropertyTypeName(prop._id);
                std::cout << "    - " << ptName << ": " << prop.get<types::UInt64>() << std::endl;
            }
            //}

            // if (!view.int64s().empty()) {
            std::cout << "  #  Int64 properties" << std::endl;
            for (const auto& prop : view.int64s()) {
                const std::string_view ptName = access.getPropertyTypeName(prop._id);
                std::cout << "    - " << ptName << ": " << prop.get<types::Int64>() << std::endl;
            }
            //}

            // if (!view.doubles().empty()) {
            std::cout << "  #  Double properties" << std::endl;
            for (const auto& prop : view.doubles()) {
                const std::string_view ptName = access.getPropertyTypeName(prop._id);
                std::cout << "    - " << ptName << ": " << prop.get<types::Double>() << std::endl;
            }
            //}

            // if (!view.strings().empty()) {
            std::cout << "  #  String properties" << std::endl;
            for (const auto& prop : view.strings()) {
                const std::string_view ptName = access.getPropertyTypeName(prop._id);
                std::cout << "    - " << ptName << ": " << prop.get<types::String>() << std::endl;
            }
            //}

            // if (!view.bools().empty()) {
            std::cout << "  #  Bool properties" << std::endl;
            for (const auto& prop : view.bools()) {
                const std::string_view ptName = access.getPropertyTypeName(prop._id);
                std::cout << "    - " << ptName << ": " << prop.get<types::Bool>() << std::endl;
            }
            //}
        };

        printProperties(apoeProperties);

        std::cout
            << "APOE has " << apoeEdges.getOutEdgeCount() << " out edges" << std::endl;
        for (const auto& edge : apoe.edges().outEdges()) {
            const EdgeView edgeView = reader.getEdgeView(edge._edgeID);
            const NodeView target = reader.getNodeView(edge._otherID);
            const types::String::Primitive& displayName =
                target.properties().getProperty<types::String>(displayNameType._id);
            std::cout << "  -> " << edge._otherID.getValue()
                      << " with " << edgeView.properties().getCount() << " properties"
                      << std::endl;
            printProperties(edgeView.properties());
            std::cout << "       * " << displayName << std::endl;
        }
        std::cout << "APOE has " << apoeEdges.getInEdgeCount() << " in edges" << std::endl;
        for (const auto& edge : apoe.edges().inEdges()) {
            const EdgeView edgeView = reader.getEdgeView(edge._edgeID);
            const NodeView source = reader.getNodeView(edge._otherID);
            const types::String::Primitive& displayName =
                source.properties().getProperty<types::String>(displayNameType._id);
            std::cout << "  <- " << edge._otherID.getValue()
                      << " with " << edgeView.properties().getCount() << " properties"
                      << std::endl;
            printProperties(edgeView.properties());
            std::cout << "       * " << displayName << std::endl;
        }
    }

    void findLocation() {
        auto access = _db->access();
        auto reader = access.getReader();

        std::string_view address = "33 Plover Drive";
        PropertyType addressType = access.getPropertyType("address (String)");
        // Labelset locationLabels = access.getLabelset({"Location"});
        auto it = reader.scanNodeProperties<types::String>(addressType._id).begin();

        const auto findNodeID = [&]() {
            for (; it.isValid(); it.next()) {
                const types::String::Primitive& v = it.get();
                if (v == address) {
                    EntityID nodeID = it.getCurrentNodeID();
                    std::cout << "Found address at ID: " << nodeID.getValue() << std::endl;
                    return nodeID;
                }
            }
            std::cout << "Could not find location 33 Plover Drive" << std::endl;
            throw;
        };

        auto t0 = Clock::now();
        EntityID nodeID = findNodeID();
        auto t1 = Clock::now();
        std::cout << "found Location in: " << duration<Microseconds>(t0, t1) << " us" << std::endl;

        t0 = Clock::now();
        NodeView node = reader.getNodeView(nodeID);
        t1 = Clock::now();
        std::cout << "Got node view in: " << duration<Microseconds>(t0, t1) << " us" << std::endl;

        const auto& nodeEdges = node.edges();
        // const auto& nodeProperties = node.properties();
        // const types::String::Primitive& addressValue =
        //     nodeProperties.getProperty<types::String>(addressType._id);
        // std::cout << "Location has address: " << addressValue << std::endl;
        // std::cout << "And other props: " << std::endl;

        // const auto& strings = nodeProperties.strings();
        // for (const auto& prop : strings) {
        //     const auto& v = prop.get<types::String>();
        //     std::cout << "  - " << prop._id.getValue() << ": " << v << std::endl;
        // }

        // ASSERT_STREQ(addressValue.c_str(), "33 Plover Drive");
        // ASSERT_EQ(nodeEdges.getOutEdgeCount(), 2);
        // ASSERT_EQ(nodeEdges.getInEdgeCount(), 5);

        std::cout << "Location has " << nodeEdges.getOutEdgeCount() << " out edges" << std::endl;
        for (const auto& edge : nodeEdges.outEdges()) {
            NodeView target = reader.getNodeView(edge._otherID);
            const auto& targetProps = target.properties();

            std::cout << "  -> " << edge._otherID.getValue() << std::endl;

            for (const auto& propView : targetProps.strings()) {
                const auto& propName = access.getPropertyTypeName(propView._id);
                std::cout << "    - " << propName << ": " << propView.get<types::String>()
                          << std::endl;
            }
        }

        std::cout << "Location has " << nodeEdges.getInEdgeCount() << " in edges" << std::endl;
        for (const auto& edge : node.edges().inEdges()) {
            NodeView source = reader.getNodeView(edge._otherID);
            const auto& sourceProps = source.properties();

            std::cout << "  <- " << edge._otherID.getValue() << std::endl;

            for (const auto& propView : sourceProps.strings()) {
                const auto& propName = access.getPropertyTypeName(propView._id);
                std::cout << "    - " << propName << ": " << propView.get<types::String>()
                          << std::endl;
            }
        }
    }

    std::unique_ptr<DB> _db = nullptr;
    std::string _outDir;
    FileUtils::Path _logPath;
    FileUtils::Path _perfPath;
};

DataBuffer newDataBuffer(DB* db) {
    auto access = db->access();
    return access.newDataBuffer();
}

TEST_F(Neo4jImporterTest, Simple) {
    {
        auto buf1 = newDataBuffer(_db.get());
        buf1.addNode(Labelset {1}); // 0
        buf1.addNode(Labelset {0}); // 1
        buf1.addNode(Labelset {1}); // 2
        buf1.addNode(Labelset {2}); // 3
        buf1.addEdge(0, 0, 1);

        _db->uniqueAccess().pushDataPart(buf1);

        auto buf2 = newDataBuffer(_db.get());
        buf2.addEdge(2, 1, 2);
        EntityID id1 = buf2.addNode(Labelset {0});
        buf2.addNodeProperty<types::String>(id1, 0, "test1");

        buf2.addEdge(2, 0, 1);
        EntityID id2 = buf2.addNode(Labelset {0});
        buf2.addNodeProperty<types::String>(id2, 0, "test2");
        buf2.addNodeProperty<types::String>(2, 0, "test3");
        // buf2.addNodeProperty<types::UInt64>(5, 0, 17);
        EntityID edgeID = buf2.addEdge(4, 3, 4); // Core
        buf2.addEdgeProperty<types::String>(edgeID, 0, "Edge property test");
        buf2.addNode(Labelset {0});
        buf2.addNode(Labelset {0});
        buf2.addNode(Labelset {0});
        buf2.addNode(Labelset {0});
        buf2.addNode(Labelset {1});
        buf2.addNode(Labelset {1});
        buf2.addNode(Labelset {1});
        buf2.addEdge(0, 3, 4); // Core

        _db->uniqueAccess().pushDataPart(buf2);
    }

    auto access = _db->access();
    auto reader = access.getReader();

    std::cout << "All out edges: ";
    for (const auto& edge : reader.scanOutEdges()) {
        std::cout << edge._edgeID.getValue()
                  << edge._nodeID.getValue()
                  << edge._otherID.getValue()
                  << std::endl;
        auto view = reader.getEdgeView(edge._edgeID);
        std::cout << "  Has " << view.properties().getCount() << " properties" << std::endl;
    }
    std::cout << std::endl;
    // std::cout << std::endl;
    // std::cout << "node 3 has out edges: ";
    //  ColumnNodes nodeIDs = {3};
    //  for (const auto& edge : reader.getCoreOutEdges(&nodeIDs)) {
    //      std::cout << edge._edgeID.getValue()
    //                << edge._nodeID.getValue()
    //                << edge._otherID.getValue()
    //                << " ";
    //  }
    //  nodeIDs = {0};
    //  std::cout << std::endl;
    //  std::cout << "node 0 has out edges: ";
    //  for (const auto& edge : reader.getCoreOutEdges(&nodeIDs)) {
    //      std::cout << edge._edgeID.getValue()
    //                << edge._nodeID.getValue()
    //                << edge._otherID.getValue()
    //                << " ";
    //  }
    //  std::cout << std::endl;
    //  nodeIDs = {1};
    //  std::cout << "node 1 has in edges: ";
    //  for (const auto& edge : reader.getCoreInEdges(&nodeIDs)) {
    //      std::cout << edge._edgeID.getValue()
    //                << edge._nodeID.getValue()
    //                << edge._otherID.getValue()
    //                << " ";
    //  }
    //  std::cout << std::endl;

    auto it = reader.scanNodeProperties<types::String>(0).begin();
    for (; it.isValid(); it.next()) {
        std::cout << it.getCurrentNodeID().getValue() << ": " << it.get() << std::endl;
    }

    // auto view = reader.getNodeView(4);
    // std::cout << "Node data from view" << std::endl;
    // for (const auto& prop : view.properties().strings()) {
    //     std::cout << "  - " << prop.get<types::String>() << std::endl;
    // }
    // for (const auto& e : view.edges().inEdges()) {
    //     std::cout << "  - " << e._edgeID.getValue()
    //               << e._nodeID.getValue()
    //               << e._otherID.getValue() << std::endl;
    // }
}

TEST_F(Neo4jImporterTest, General) {
    JobSystem jobSystem;
    jobSystem.initialize();
    [[maybe_unused]] auto globalT0 = std::chrono::high_resolution_clock::now();
    [[maybe_unused]] auto t0 = Clock::now();
    [[maybe_unused]] auto t1 = Clock::now();
    [[maybe_unused]] static constexpr size_t nodeCountLimit = 400000;
    [[maybe_unused]] static constexpr size_t edgeCountLimit = 1000000;

    const std::string turingHome = std::getenv("TURING_HOME");
    //  const FileUtils::Path jsonDir = FileUtils::Path {turingHome} / "neo4j" / "cyber-security-db";
    const FileUtils::Path jsonDir = FileUtils::Path {turingHome} / "neo4j" / "pole-db";
    // const FileUtils::Path jsonDir = "/home/luclabarriere/jsonReactome";
    t0 = Clock::now();
    Neo4jImporter::importJsonDir(jobSystem,
                                 _db.get(),
                                 nodeCountLimit,
                                 edgeCountLimit,
                                 {
                                     ._jsonDir = jsonDir,
                                     ._workDir = _outDir,
                                 });
    t1 = Clock::now();
    std::cout << "Parsing: " << duration<Seconds>(t0, t1) << " s" << std::endl;

    // findApoe();
    findLocation();

    // auto access = _db->access();
    // std::stringstream report;
    // access.getReport(report);
    // std::cout << report.view() << std::endl;
}
