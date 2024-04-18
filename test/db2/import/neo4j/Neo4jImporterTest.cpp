#include <gtest/gtest.h>
#include <numeric>
#include <regex>
#include <span>
#include <thread>

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

    std::unique_ptr<DB> _db = nullptr;
    std::string _outDir;
    FileUtils::Path _logPath;
    FileUtils::Path _perfPath;
};

TEST_F(Neo4jImporterTest, General) {
    JobSystem jobSystem;
    jobSystem.initialize();
    [[maybe_unused]] auto globalT0 = std::chrono::high_resolution_clock::now();
    [[maybe_unused]] auto t0 = Clock::now();
    [[maybe_unused]] auto t1 = Clock::now();
    [[maybe_unused]] static constexpr size_t nodeCountLimit = 400000;
    [[maybe_unused]] static constexpr size_t edgeCountLimit = 1000000;

    //std::string testStr = "hello";
    //std::unordered_map<uint64_t, FixedString> strings;
    //strings.emplace(std::piecewise_construct,
    //                std::forward_as_tuple(0),
    //                std::forward_as_tuple(testStr.data(), testStr.size()));
    //FixedString testFixedStr(testStr.data(), testStr.size());
    //FixedString testFixedStr2(testFixedStr);
    //FixedString testFixedStr3 = testFixedStr;
    //strings.emplace(std::piecewise_construct,
    //                std::forward_as_tuple(1),
    //                std::forward_as_tuple(std::move(testFixedStr)));
    //strings[2] = FixedString();
    //std::unordered_map<uint64_t, FixedString> strings2;
    //strings2.merge(strings);
    //auto strings3 = strings2;
    //auto strings4 = std::move(strings2);
    //std::cout << strings4.at(1) << std::endl;

    //const std::string turingHome = std::getenv("TURING_HOME");
    //const FileUtils::Path jsonDir = FileUtils::Path {turingHome} / "neo4j" / "pole-db";
    const FileUtils::Path jsonDir = "/home/luclabarriere/jsonReactome";
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

    // t0 = Clock::now();
    // auto buf = parser.build();
    // t1 = Clock::now();
    // std::cout << "Build: " << duration<Milliseconds>(t0, t1) << " ms";

    // t0 = Clock::now();
    //_db->uniqueAccess().pushDataPart(*buf);
    // t1 = Clock::now();
    // std::cout << "Push: " << duration<Seconds>(t0, t1) << " s";

    // t1 = std::chrono::high_resolution_clock::now();

    auto access = _db->access();
    auto reader = access.getReader();

    // std::string_view nodeCriteria = "OL11 3HA";
    // PropertyType criteriaType = access.getPropertyType("postcode (String)");
    ////PropertyType stIDType = access.getPropertyType("stId");
    ////const LabelSet apoeLabelset = access.getLabelSet({"DatabaseObject",
    ////                                                  "PhysicalEntity",
    ////                                                  "GenomeEncodedEntity",
    ////                                                  "EntityWithAccessionedSequence"});

    // auto it = reader.scanNodeProperties<StringPropertyType>(criteriaType._id).begin();
    // const auto findReactome = [&]() {
    //     for (; it.isValid(); it.next()) {
    //         const std::string& stID = it.get();
    //         if (stID == nodeCriteria) {
    //             EntityID nodeID = it.getCurrentNodeID();
    //             std::cout << "Found node: " << nodeID.getValue() << std::endl;
    //             return nodeID;
    //         }
    //     }
    //     std::cout << "Could not find node" << std::endl;
    //     throw;
    // };

    // MEASURE_TIME("Find node", EntityID nodeID = findReactome());

    // NodeView node = reader.getNodeView(nodeID);
    // const auto& nodeEdges = node.edges();
    ////const std::string& nodeDisplayName = apoe.properties().getProperty<StringPropertyType>(displayNameType._id);
    ////std::cout << "APOE has display name: " << apoeDisplayName << std::endl;;

    // std::cout << "Node has " << nodeEdges.getOutEdgeCount() << " out edges" << std::endl;;
    // for (const auto& edge : node.edges().outEdges()) {
    //     std::cout << "  -> " << edge._otherID.getValue() << std::endl;
    // }
    // std::cout << "Node has " << nodeEdges.getOutEdgeCount() << " in edges" << std::endl;;
    // for (const auto& edge : node.edges().inEdges()) {
    //     std::cout << "  <- " << edge._otherID.getValue() << std::endl;
    // }

    std::string_view apoe4ReactomeID = "R-HSA-9711070";
    PropertyType displayNameType = access.getPropertyType("displayName (String)");
    PropertyType stIDType = access.getPropertyType("stId (String)");
    // const LabelSet apoeLabelset = access.getLabelSet({"DatabaseObject",
    //                                                   "PhysicalEntity",
    //                                                   "GenomeEncodedEntity",
    //                                                   "EntityWithAccessionedSequence"});

    auto it = reader.scanNodeProperties<StringPropertyType>(stIDType._id).begin();
    const auto findReactome = [&]() {
        for (; it.isValid(); it.next()) {
            const StringPropertyType::Primitive& stID = it.get();
            if (stID == apoe4ReactomeID) {
                EntityID apoeID = it.getCurrentNodeID();
                std::cout << "Found APOE-4 at ID: " << apoeID.getValue() << std::endl;
                return apoeID;
            }
        }
        std::cout << "Could not find APOE-4" << std::endl;
        throw;
    };

    t0 = Clock::now();
    EntityID apoeID = findReactome();
    t1 = Clock::now();
    std::cout << "found APOE-4 in: " << duration<Milliseconds>(t0, t1) << " ms" << std::endl;

    NodeView apoe = reader.getNodeView(apoeID);
    const auto& apoeEdges = apoe.edges();
    const StringPropertyType::Primitive& apoeDisplayName = apoe.properties().getProperty<StringPropertyType>(displayNameType._id);
    std::cout << "APOE has display name: " << apoeDisplayName << std::endl;

    std::cout << "APOE has " << apoeEdges.getOutEdgeCount() << " out edges" << std::endl;
    for (const auto& edge : apoe.edges().outEdges()) {
        NodeView target = reader.getNodeView(edge._otherID);
        const StringPropertyType::Primitive& displayName = target.properties().getProperty<StringPropertyType>(displayNameType._id);
        std::cout << "  -> " << displayName << std::endl;
    }
    std::cout << "APOE has " << apoeEdges.getInEdgeCount() << " in edges" << std::endl;
    for (const auto& edge : apoe.edges().inEdges()) {
        NodeView source = reader.getNodeView(edge._otherID);
        const StringPropertyType::Primitive& displayName = source.properties().getProperty<StringPropertyType>(displayNameType._id);
        std::cout << "  <- " << displayName << std::endl;
    }
}
