#include <gtest/gtest.h>
#include <numeric>
#include <regex>
#include <span>
#include <thread>

#include "BioAssert.h"
#include "BioLog.h"
#include "DB.h"
#include "FileUtils.h"
#include "JobSystem.h"
#include "Neo4jImporter.h"
#include "NodeView.h"
#include "PerfStat.h"
#include "Reader.h"
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

    void findReactome() {
        auto access = _db->access();
        auto reader = access.getReader();

        std::string_view apoe4ReactomeID = "R-HSA-9711070";
        PropertyType displayNameType = access.getPropertyType("displayName (String)");
        PropertyType stIDType = access.getPropertyType("stId (String)");

        auto it = reader.scanNodeProperties<types::String>(stIDType._id).begin();
        const auto findReactomeID = [&]() {
            for (; it.isValid(); it.next()) {
                const types::String::Primitive& stID = it.get();
                if (stID == apoe4ReactomeID) {
                    EntityID apoeID = it.getCurrentNodeID();
                    std::cout << "Found APOE-4 at ID: " << apoeID.getValue() << std::endl;
                    return apoeID;
                }
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

        const auto& apoeEdges = apoe.edges();
        const auto& apoeProperties = apoe.properties();
        const types::String::Primitive& apoeDisplayName =
            apoeProperties.getProperty<types::String>(displayNameType._id);
        std::cout << "APOE has display name: " << apoeDisplayName << std::endl;

        std::cout << "APOE has " << apoeEdges.getOutEdgeCount() << " out edges" << std::endl;
        for (const auto& edge : apoe.edges().outEdges()) {
            NodeView target = reader.getNodeView(edge._otherID);
            const types::String::Primitive& displayName =
                target.properties().getProperty<types::String>(displayNameType._id);
            std::cout << "  -> " << displayName << std::endl;
        }
        std::cout << "APOE has " << apoeEdges.getInEdgeCount() << " in edges" << std::endl;
        for (const auto& edge : apoe.edges().inEdges()) {
            NodeView source = reader.getNodeView(edge._otherID);
            const types::String::Primitive& displayName =
                source.properties().getProperty<types::String>(displayNameType._id);
            std::cout << "  <- " << displayName << std::endl;
        }
    }

    void findLocation() {
        auto access = _db->access();
        auto reader = access.getReader();

        std::string_view address = "33 Plover Drive";
        PropertyType addressType = access.getPropertyType("address (String)");
        // LabelSet locationLabels = access.getLabelSet({"Location"});

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
        const auto& nodeProperties = node.properties();
        const types::String::Primitive& addressValue =
            nodeProperties.getProperty<types::String>(addressType._id);
        std::cout << "Location has address: " << addressValue << std::endl;

        ASSERT_STREQ(addressValue.c_str(), "33 Plover Drive");
        ASSERT_EQ(nodeEdges.getOutEdgeCount(), 2);
        ASSERT_EQ(nodeEdges.getInEdgeCount(), 5);

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

TEST_F(Neo4jImporterTest, General) {
    JobSystem jobSystem;
    jobSystem.initialize();
    bioassert(false);
    [[maybe_unused]] auto globalT0 = std::chrono::high_resolution_clock::now();
    [[maybe_unused]] auto t0 = Clock::now();
    [[maybe_unused]] auto t1 = Clock::now();
    [[maybe_unused]] static constexpr size_t nodeCountLimit = 400000;
    [[maybe_unused]] static constexpr size_t edgeCountLimit = 1000000;

    const std::string turingHome = std::getenv("TURING_HOME");
    // const FileUtils::Path jsonDir = FileUtils::Path {turingHome} / "neo4j" / "cyber-security-db";
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

    findLocation();
}
