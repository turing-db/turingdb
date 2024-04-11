#include <gtest/gtest.h>
#include <numeric>
#include <regex>
#include <span>
#include <thread>

#include "BioLog.h"
#include "DB.h"
#include "FileUtils.h"
#include "JobSystem.h"
#include "JsonParser.h"
#include "NodeView.h"
#include "PerfStat.h"
#include "Reader.h"

using namespace db;
using namespace std::chrono;

#define MEASURE_TIME(msg, op)                       \
    t0 = std::chrono::high_resolution_clock::now(); \
    op;                                             \
    t1 = std::chrono::high_resolution_clock::now(); \
    printDuration<milliseconds>(msg, t0, t1);

class Neo4jJsonParserTest : public ::testing::Test {
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

        _db = std::make_unique<DB>();
    }

    void TearDown() override {
        Log::BioLog::destroy();
    }

    std::unique_ptr<DB> _db = nullptr;
    std::string _outDir;
    FileUtils::Path _logPath;
};

template <typename T>
void printDuration(std::string_view msg, const auto& t0, const auto& t1) {
    if constexpr (std::is_same_v<T, microseconds>) {
        float dur = duration_cast<nanoseconds>(t1 - t0).count();
        std::cout << msg << ": " << dur / 1'000.0f << " us" << std::endl;
    } else if constexpr (std::is_same_v<T, milliseconds>) {
        float dur = duration_cast<nanoseconds>(t1 - t0).count();
        std::cout << msg << ": " << dur / 1'000'000.0f << " ms" << std::endl;
    } else if constexpr (std::is_same_v<T, seconds>) {
        float dur = duration_cast<nanoseconds>(t1 - t0).count();
        std::cout << msg << ": " << dur / 1'000'000'000.0f << " s" << std::endl;
    }
}

// TEST_F(Neo4jJsonParserTest, General) {
//     JobSystem::Initialize();
//     [[maybe_unused]] auto globalT0 = std::chrono::high_resolution_clock::now();
//     [[maybe_unused]] auto t0 = std::chrono::high_resolution_clock::now();
//     [[maybe_unused]] auto t1 = std::chrono::high_resolution_clock::now();
//     [[maybe_unused]] static constexpr size_t nodeCountLimit = 1000000;
//     [[maybe_unused]] static constexpr size_t edgeCountLimit = 3000000;
// 
//     JsonParser parser(_db.get());
//     const std::string turingHome = std::getenv("TURING_HOME");
//     // const FileUtils::Path jsonDir = FileUtils::Path {turingHome} / "neo4j" / "pole-db";
//     const FileUtils::Path jsonDir = "/home/luclabarriere/reactome.out/json";
// 
//     auto statsPath = jsonDir / "stats.json";
//     std::string statsData;
//     FileUtils::readContent(statsPath, statsData);
//     const auto [nodeCount, edgeCount] = parser.parseStats(statsData);
//     // ASSERT_EQ(61521, nodeCount);
//     // ASSERT_EQ(105840, edgeCount);
// 
//     auto nodePropertiesPath = jsonDir / "nodeProperties.json";
//     std::string nodePropertiesData;
//     FileUtils::readContent(nodePropertiesPath, nodePropertiesData);
//     ASSERT_TRUE(parser.parseNodeProperties(nodePropertiesData));
//     // ASSERT_EQ(27, _db->propTypeMap().getCount());
// 
//     auto edgePropertiesPath = jsonDir / "edgeProperties.json";
//     std::string edgePropertiesData;
//     FileUtils::readContent(edgePropertiesPath, edgePropertiesData);
//     ASSERT_TRUE(parser.parseEdgeProperties(edgePropertiesData));
//     // ASSERT_EQ(18, _db->edgeTypeMap().getCount());
// 
//     // Nodes
//     std::vector<FileUtils::Path> nodeFiles;
//     FileUtils::listFiles(jsonDir, nodeFiles);
//     std::regex nodeRegex {"nodes_([0-9]*).json"};
//     std::map<size_t, FileUtils::Path> nodeFilesOrdered;
// 
//     for (const FileUtils::Path& path : nodeFiles) {
//         if (std::regex_search(path.string(), nodeRegex)) {
//             size_t index = std::stoul(std::regex_replace(
//                 path.filename().string(),
//                 nodeRegex, "$1"));
//             nodeFilesOrdered[index] = path;
//         }
//     }
// 
//     t0 = std::chrono::high_resolution_clock::now();
//     std::atomic<size_t> nodeCountLeft = nodeCount;
//     for (const auto& [index, path] : nodeFilesOrdered) {
//         size_t _nodeCountLeft = nodeCountLeft.load(std::memory_order_consume);
//         const size_t whole = (size_t)((_nodeCountLeft / nodeCountLimit) != 0);
//         const size_t modulo = _nodeCountLeft % nodeCountLimit;
//         const size_t fileNodeCount = whole * nodeCountLimit + !whole * modulo;
//         std::cout << "Nodes: " << nodeCount - nodeCountLeft << "/" << nodeCount << std::endl;
//         nodeCountLeft -= fileNodeCount;
// 
//         std::string data;
//         FileUtils::readContent(path, data);
//         DataBuffer& buf = parser.newDataBuffer(fileNodeCount, 0);
//         MEASURE_TIME("Nodes " + std::to_string(index), parser.parseNodes(data, buf););
//     }
//     t1 = std::chrono::high_resolution_clock::now();
//     printDuration<milliseconds>("All nodes", globalT0, t1);
// 
//     // Edges
//     std::vector<FileUtils::Path> edgeFiles;
//     FileUtils::listFiles(jsonDir, edgeFiles);
//     std::regex edgeRegex {"edges_([0-9]*).json"};
//     std::map<size_t, FileUtils::Path> edgeFilesOrdered;
// 
//     for (const FileUtils::Path& path : edgeFiles) {
//         if (std::regex_search(path.string(), edgeRegex)) {
//             size_t index = std::stoul(std::regex_replace(
//                 path.filename().string(),
//                 edgeRegex, "$1"));
//             edgeFilesOrdered[index] = path;
//         }
//     }
// 
//     size_t edgeCountLeft = edgeCount;
//     for (const auto& [index, path] : edgeFilesOrdered) {
//         const size_t whole = (size_t)((edgeCountLeft / edgeCountLimit) != 0);
//         const size_t modulo = edgeCountLeft % edgeCountLimit;
//         const size_t fileEdgeCount = whole * edgeCountLimit + !whole * modulo;
//         std::cout << "Edges: " << edgeCount - edgeCountLeft << "/" << edgeCount << std::endl;
//         edgeCountLeft -= fileEdgeCount;
// 
//         std::string data;
//         FileUtils::readContent(path, data);
//         DataBuffer& buf = parser.newDataBuffer(0, fileEdgeCount);
//         MEASURE_TIME("Edges " + std::to_string(index), parser.parseEdges(data, buf););
//     }
// 
//     MEASURE_TIME("Build", auto buf = parser.build());
//     MEASURE_TIME("Push", _db->uniqueAccess().pushDataPart(*buf));
// 
//     t1 = std::chrono::high_resolution_clock::now();
//     printDuration<seconds>("Total time", globalT0, t1);
// 
//     auto access = _db->access();
//     auto reader = access.getReader();
// 
//     std::string_view apoe4ReactomeID = "R-HSA-9711070";
//     PropertyType displayNameType = access.getPropertyType("displayName (String)");
//     PropertyType stIDType = access.getPropertyType("stId (String)");
//     // const LabelSet apoeLabelset = access.getLabelSet({"DatabaseObject",
//     //                                                   "PhysicalEntity",
//     //                                                   "GenomeEncodedEntity",
//     //                                                   "EntityWithAccessionedSequence"});
// 
//     auto it = reader.scanNodeProperties<StringPropertyType>(stIDType._id).begin();
//     const auto findReactome = [&]() {
//         for (; it.isValid(); it.next()) {
//             const std::string& stID = it.get();
//             if (stID == apoe4ReactomeID) {
//                 EntityID apoeID = it.getCurrentNodeID();
//                 std::cout << "Found reactome at id: " << apoeID.getValue() << std::endl;
//                 return apoeID;
//             }
//         }
//         std::cout << "Could not find reactome" << std::endl;
//         throw;
//     };
// 
//     MEASURE_TIME("Find reactome", EntityID apoeID = findReactome());
// 
//     NodeView apoe = reader.getNodeView(apoeID);
//     const auto& apoeEdges = apoe.edges();
//     const std::string& apoeDisplayName = apoe.properties().getProperty<StringPropertyType>(displayNameType._id);
//     std::cout << "APOE has display name: " << apoeDisplayName << std::endl;
//     ;
// 
//     std::cout << "APOE has " << apoeEdges.getOutEdgeCount() << " out edges" << std::endl;
//     ;
//     for (const auto& edge : apoe.edges().outEdges()) {
//         NodeView target = reader.getNodeView(edge._otherID);
//         const std::string& displayName = target.properties().getProperty<StringPropertyType>(displayNameType._id);
//         std::cout << "  -> " << displayName << std::endl;
//     }
//     std::cout << "APOE has " << apoeEdges.getInEdgeCount() << " in edges" << std::endl;
//     ;
//     for (const auto& edge : apoe.edges().inEdges()) {
//         NodeView source = reader.getNodeView(edge._otherID);
//         const std::string& displayName = source.properties().getProperty<StringPropertyType>(displayNameType._id);
//         std::cout << "  <- " << displayName << std::endl;
//     }
// }
// 
// TEST_F(Neo4jJsonParserTest, Multithreaded) {
//     JobSystem::Initialize();
//     [[maybe_unused]] auto globalT0 = std::chrono::high_resolution_clock::now();
//     [[maybe_unused]] auto t0 = std::chrono::high_resolution_clock::now();
//     [[maybe_unused]] auto t1 = std::chrono::high_resolution_clock::now();
//     [[maybe_unused]] static constexpr size_t nodeCountLimit = 1000000;
//     [[maybe_unused]] static constexpr size_t edgeCountLimit = 3000000;
// 
//     JsonParser parser(_db.get());
//     const std::string turingHome = std::getenv("TURING_HOME");
//     // const FileUtils::Path jsonDir = FileUtils::Path {turingHome} / "neo4j" / "pole-db";
//     const FileUtils::Path jsonDir = "/home/luclabarriere/reactome.out/json";
// 
//     struct FileData {
//         FileUtils::Path path;
//         std::string content;
//     };
//     std::vector<FileData> files = {
//         {jsonDir / "stats.json"},
//         {jsonDir / "nodeProperties.json"},
//         {jsonDir / "edgeProperties.json"},
//     };
// 
//     // Nodes
//     std::vector<FileUtils::Path> nodeFiles;
//     FileUtils::listFiles(jsonDir, nodeFiles);
//     std::regex nodeRegex {"nodes_([0-9]*).json"};
//     std::map<size_t, FileUtils::Path> nodeFilesOrdered;
// 
//     for (const FileUtils::Path& path : nodeFiles) {
//         if (std::regex_search(path.string(), nodeRegex)) {
//             size_t index = std::stoul(std::regex_replace(
//                 path.filename().string(),
//                 nodeRegex, "$1"));
//             nodeFilesOrdered[index] = path;
//         }
//     }
// 
//     for (const auto& [index, path] : nodeFilesOrdered) {
//         files.emplace_back(path);
//     }
// 
//     // Edges
//     std::vector<FileUtils::Path> edgeFiles;
//     FileUtils::listFiles(jsonDir, edgeFiles);
//     std::regex edgeRegex {"edges_([0-9]*).json"};
//     std::map<size_t, FileUtils::Path> edgeFilesOrdered;
// 
//     for (const FileUtils::Path& path : edgeFiles) {
//         if (std::regex_search(path.string(), edgeRegex)) {
//             size_t index = std::stoul(std::regex_replace(
//                 path.filename().string(),
//                 edgeRegex, "$1"));
//             edgeFilesOrdered[index] = path;
//         }
//     }
// 
//     for (const auto& [index, path] : edgeFilesOrdered) {
//         files.emplace_back(path);
//     }
// 
//     t0 = std::chrono::high_resolution_clock::now();
//     JobSystem::Initialize();
// 
//     for (auto& file : files) {
//         JobSystem::Submit([&] {
//             FileUtils::readContent(file.path, file.content);
//         });
//     }
// 
//     JobSystem::Wait();
// 
//     std::atomic<size_t> nodeCount = 0;
//     std::atomic<size_t> edgeCount = 0;
// 
//     // Stats
//     JobSystem::Submit([&] {
//         const auto stats = parser.parseStats(files[0].content);
//         nodeCount.store(stats.nodeCount);
//         edgeCount.store(stats.edgeCount);
//     });
// 
//     JobSystem::Wait();
// 
//     // Node properties
//     JobSystem::Submit([&] {
//         parser.parseNodeProperties(files[1].content);
//     });
// 
//     JobSystem::Wait();
// 
//     // Edge properties
//     JobSystem::Submit([&] {
//         parser.parseEdgeProperties(files[2].content);
//     });
// 
//     JobSystem::Wait();
// 
//     // Nodes
//     std::atomic<size_t> nodeCountLeft = nodeCount.load();
//     auto nodeFilesSpan = std::span {files}.subspan(3, nodeFilesOrdered.size());
//     for (const auto& file : nodeFilesSpan) {
//         size_t _nodeCountLeft = nodeCountLeft;
//         const size_t whole = (size_t)((_nodeCountLeft / nodeCountLimit) != 0);
//         const size_t modulo = _nodeCountLeft % nodeCountLimit;
//         const size_t fileNodeCount = whole * nodeCountLimit + !whole * modulo;
//         std::cout << "Nodes: " << nodeCount - nodeCountLeft << "/" << nodeCount << std::endl;
//         nodeCountLeft -= fileNodeCount;
//         DataBuffer& buf = parser.newDataBuffer(fileNodeCount, 0);
// 
//         JobSystem::Submit([&] {
//             parser.parseNodes(file.content, buf);
//         });
//     }
//     JobSystem::Wait();
// 
//     // Edges
//     std::atomic<size_t> edgeCountLeft = edgeCount.load();
//     auto edgeFilesSpan = std::span {files}.subspan(3 + nodeFilesOrdered.size(), edgeFilesOrdered.size());
//     for (const auto& file : edgeFilesSpan) {
//         size_t _edgeCountLeft = edgeCountLeft;
//         const size_t whole = (size_t)((_edgeCountLeft / edgeCountLimit) != 0);
//         const size_t modulo = _edgeCountLeft % edgeCountLimit;
//         const size_t fileEdgeCount = whole * edgeCountLimit + !whole * modulo;
//         std::cout << "Edges: " << edgeCount - edgeCountLeft << "/" << edgeCount << std::endl;
//         edgeCountLeft -= fileEdgeCount;
//         DataBuffer& buf = parser.newDataBuffer(0, fileEdgeCount);
// 
//         JobSystem::Submit([&] {
//             parser.parseEdges(file.content, buf);
//         });
//     }
// 
//     JobSystem::Wait();
//     JobSystem::Terminate();
// 
//     MEASURE_TIME("Build", auto buf = parser.build());
//     MEASURE_TIME("Push", _db->uniqueAccess().pushDataPart(*buf));
//     t1 = std::chrono::high_resolution_clock::now();
//     printDuration<milliseconds>("Total parsing time", globalT0, t1);
// 
//     auto access = _db->access();
//     auto reader = access.getReader();
// 
//     // std::string_view nodeCriteria = "OL11 3HA";
//     // PropertyType criteriaType = access.getPropertyType("postcode (String)");
//     ////PropertyType stIDType = access.getPropertyType("stId");
//     ////const LabelSet apoeLabelset = access.getLabelSet({"DatabaseObject",
//     ////                                                  "PhysicalEntity",
//     ////                                                  "GenomeEncodedEntity",
//     ////                                                  "EntityWithAccessionedSequence"});
// 
//     // auto it = reader.scanNodeProperties<StringPropertyType>(criteriaType._id).begin();
//     // const auto findReactome = [&]() {
//     //     for (; it.isValid(); it.next()) {
//     //         const std::string& stID = it.get();
//     //         if (stID == nodeCriteria) {
//     //             EntityID nodeID = it.getCurrentNodeID();
//     //             std::cout << "Found node: " << nodeID.getValue() << std::endl;
//     //             return nodeID;
//     //         }
//     //     }
//     //     std::cout << "Could not find node" << std::endl;
//     //     throw;
//     // };
// 
//     // MEASURE_TIME("Find node", EntityID nodeID = findReactome());
// 
//     // NodeView node = reader.getNodeView(nodeID);
//     // const auto& nodeEdges = node.edges();
//     ////const std::string& nodeDisplayName = apoe.properties().getProperty<StringPropertyType>(displayNameType._id);
//     ////std::cout << "APOE has display name: " << apoeDisplayName << std::endl;;
// 
//     // std::cout << "Node has " << nodeEdges.getOutEdgeCount() << " out edges" << std::endl;;
//     // for (const auto& edge : node.edges().outEdges()) {
//     //     std::cout << "  -> " << edge._otherID.getValue() << std::endl;
//     // }
//     // std::cout << "Node has " << nodeEdges.getOutEdgeCount() << " in edges" << std::endl;;
//     // for (const auto& edge : node.edges().inEdges()) {
//     //     std::cout << "  <- " << edge._otherID.getValue() << std::endl;
//     // }
// 
//     std::string_view apoe4ReactomeID = "R-HSA-9711070";
//     PropertyType displayNameType = access.getPropertyType("displayName (String)");
//     PropertyType stIDType = access.getPropertyType("stId (String)");
//     // const LabelSet apoeLabelset = access.getLabelSet({"DatabaseObject",
//     //                                                   "PhysicalEntity",
//     //                                                   "GenomeEncodedEntity",
//     //                                                   "EntityWithAccessionedSequence"});
// 
//     auto it = reader.scanNodeProperties<StringPropertyType>(stIDType._id).begin();
//     const auto findReactome = [&]() {
//         for (; it.isValid(); it.next()) {
//             const std::string& stID = it.get();
//             if (stID == apoe4ReactomeID) {
//                 EntityID apoeID = it.getCurrentNodeID();
//                 std::cout << "Found reactome at ID: " << apoeID.getValue() << std::endl;
//                 return apoeID;
//             }
//         }
//         std::cout << "Could not find reactome" << std::endl;
//         throw;
//     };
// 
//     MEASURE_TIME("Find reactome", EntityID apoeID = findReactome());
// 
//     NodeView apoe = reader.getNodeView(apoeID);
//     const auto& apoeEdges = apoe.edges();
//     const std::string& apoeDisplayName = apoe.properties().getProperty<StringPropertyType>(displayNameType._id);
//     std::cout << "APOE has display name: " << apoeDisplayName << std::endl;
// 
//     std::cout << "APOE has " << apoeEdges.getOutEdgeCount() << " out edges" << std::endl;
//     for (const auto& edge : apoe.edges().outEdges()) {
//         NodeView target = reader.getNodeView(edge._otherID);
//         const std::string& displayName = target.properties().getProperty<StringPropertyType>(displayNameType._id);
//         std::cout << "  -> " << displayName << std::endl;
//     }
//     std::cout << "APOE has " << apoeEdges.getInEdgeCount() << " in edges" << std::endl;
//     for (const auto& edge : apoe.edges().inEdges()) {
//         NodeView source = reader.getNodeView(edge._otherID);
//         const std::string& displayName = source.properties().getProperty<StringPropertyType>(displayNameType._id);
//         std::cout << "  <- " << displayName << std::endl;
//     }
// }
