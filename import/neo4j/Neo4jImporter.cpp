#include "Neo4jImporter.h"

#include <spdlog/spdlog.h>

#include "Graph.h"
#include "Neo4JHttpRequest.h"
#include "JobSystem.h"
#include "JobGroup.h"
#include "JsonParser.h"
#include "LogUtils.h"
#include "Neo4jInstance.h"
#include "Neo4JQueryManager.h"
#include "GraphFileType.h"
#include "versioning/Change.h"

using namespace db;

namespace {

std::string formatSize(size_t size) {
    std::array units = {" B", "kB", "MB", "GB"};
    double convertedSize = static_cast<double>(size);
    int unitIndex = 0;

    while (convertedSize >= 1024.0 && unitIndex < 3) {
        convertedSize /= 1024.0;
        ++unitIndex;
    }

    return fmt::format("{:>3.0f}{}", convertedSize, units[unitIndex]);
}

}

bool Neo4jImporter::fromUrlToJsonDir(JobSystem& jobSystem,
                                     Graph* graph,
                                     size_t nodeCountPerQuery,
                                     size_t edgeCountPerQuery,
                                     const UrlToJsonDirArgs& args) {
    std::unique_lock guard(_mutex);
    return fromUrlToJsonDirImpl(jobSystem, graph, nodeCountPerQuery, edgeCountPerQuery, args);
}

bool Neo4jImporter::importJsonDir(JobSystem& jobSystem,
                                  Graph* graph,
                                  std::size_t nodeCountPerFile,
                                  std::size_t edgeCountPerFile,
                                  const ImportJsonDirArgs& args) {
    std::unique_lock guard(_mutex);
    return importJsonDirImpl(jobSystem, graph, nodeCountPerFile, edgeCountPerFile, args);
}

bool Neo4jImporter::fromDumpFileToJsonDir(JobSystem& jobSystem,
                                          Graph* graph,
                                          std::size_t nodeCountPerQuery,
                                          std::size_t edgeCountPerQuery,
                                          const DumpFileToJsonDirArgs& args) {
    std::unique_lock guard(_mutex);
    return fromDumpFileToJsonDirImpl(jobSystem, graph, nodeCountPerQuery, edgeCountPerQuery, args);
}

bool Neo4jImporter::fromUrlToJsonDirImpl(JobSystem& jobSystem,
                                         Graph* graph,
                                         size_t nodeCountPerQuery,
                                         size_t edgeCountPerQuery,
                                         const UrlToJsonDirArgs& args) {
    JsonParser parser;
    QueryManager manager;
    const FileUtils::Path jsonDir = args._workDir / "json";
    JobGroup jobs = jobSystem.newGroup();

    if (!FileUtils::createDirectory(jsonDir)) {
        spdlog::error("Could not create json directory {}", jsonDir.string());
        return false;
    }

    const FileUtils::Path graphTypePath = jsonDir / "type";
    const auto typeTag = GraphFileTypeDescription::value(GraphFileType::NEO4J_JSON);

    if (!FileUtils::writeFile(graphTypePath, typeTag.data())) {
        spdlog::error("Could not write graph type file {}", graphTypePath.string());
        return false;
    }

    manager.setUrl(args._url);
    manager.setUrlSuffix(args._urlSuffix);
    manager.setUsername(args._username);
    manager.setPassword(args._password);
    manager.setPort(args._port);

    if (!manager.isServerRunning()) {
        spdlog::info("No anwser from: {}:{}{}", args._url, args._port, args._urlSuffix);
        return false;
    }

    std::atomic<bool> success = true;

    const auto writeJsonFile = [&](FileUtils::Path jsonPath, QueryManager::Query& query) {
        jobs.submit<void>(
            [jsonPath = std::move(jsonPath), &query, &success](Promise*) {
                if (!query.waitAnswer()) {
                    success.store(false);
                    return;
                }

                auto& content = query._response;
                spdlog::info("Writing json file: {:>20} {} ",
                             jsonPath.filename().c_str(), formatSize(content.size()));
                FileUtils::writeFile(jsonPath, content);
                content.clear();
                content.shrink_to_fit();
            });
    };

    const auto submitQuery = [&](QueryManager::Query& q) {
        q._future = jobs.submit<QueryManager::Response>(
            [&](Promise* p) {
                auto* promise = p->cast<QueryManager::Response>();

                if (!q._request->exec()) {
                    success.store(false);
                    promise->set_value(std::nullopt);
                    return;
                }
                promise->set_value(q._request->consumeResponse());
            });
    };

    QueryManager::Query statsQuery = manager.dbStatsQuery();
    QueryManager::Query nodeLabelsQuery = manager.nodeLabelsQuery();
    QueryManager::Query nodeLabelSetsQuery = manager.nodeLabelSetsQuery();
    QueryManager::Query edgeTypesQuery = manager.edgeTypesQuery();
    QueryManager::Query nodePropertiesQuery = manager.nodePropertiesQuery();
    QueryManager::Query edgePropertiesQuery = manager.edgePropertiesQuery();

    // Submitting meta data HTTP queries
    submitQuery(statsQuery);
    submitQuery(nodeLabelsQuery);
    submitQuery(nodeLabelSetsQuery);
    submitQuery(edgeTypesQuery);
    submitQuery(nodePropertiesQuery);
    submitQuery(edgePropertiesQuery);

    if (!statsQuery.waitAnswer()) {
        spdlog::error("Could not retrieve database info");
        return false;
    }

    const auto stats = parser.parseStats(statsQuery._response);
    spdlog::info("Database has {} nodes and {} edges", stats.nodeCount, stats.edgeCount);

    // Write node metadata json files
    writeJsonFile(jsonDir / "stats.json", statsQuery);
    writeJsonFile(jsonDir / "nodeLabels.json", nodeLabelsQuery);
    writeJsonFile(jsonDir / "labelSets.json", nodeLabelSetsQuery);
    writeJsonFile(jsonDir / "edgeTypes.json", edgeTypesQuery);
    writeJsonFile(jsonDir / "nodeProperties.json", nodePropertiesQuery);
    writeJsonFile(jsonDir / "edgeProperties.json", edgePropertiesQuery);

    jobs.wait();

    if (!success.load()) {
        spdlog::error("Could not retrieve metadata");
        return false;
    }

    // Query nodes and write json files
    std::vector<QueryManager::Query> nodeQueries;
    manager.nodesQueries(stats.nodeCount, nodeCountPerQuery, nodeQueries);

    for (auto& nodeQuery : nodeQueries) {
        submitQuery(nodeQuery);
    }

    for (size_t i = 0; i < nodeQueries.size(); i++) {
        auto& nodeQuery = nodeQueries[i];
        std::string fileName = "nodes_" + std::to_string(i + 1) + ".json";
        writeJsonFile(jsonDir / fileName, nodeQuery);
    }

    // Query edges and write json files
    std::vector<QueryManager::Query> edgeQueries;
    manager.edgesQueries(stats.edgeCount, edgeCountPerQuery, edgeQueries);

    for (auto& edgeQuery : edgeQueries) {
        submitQuery(edgeQuery);
    }

    jobs.wait();

    if (!success.load()) {
        spdlog::error("Could not retrieve nodes");
        return false;
    }

    for (size_t i = 0; i < edgeQueries.size(); i++) {
        auto& edgeQuery = edgeQueries[i];
        std::string fileName = "edges_" + std::to_string(i + 1) + ".json";
        writeJsonFile(jsonDir / fileName, edgeQuery);
    }

    jobs.wait();

    if (!success.load()) {
        spdlog::error("Could not retrieve edges");
        return false;
    }

    return true;
}

bool Neo4jImporter::importJsonDirImpl(JobSystem& jobSystem,
                                      Graph* graph,
                                      std::size_t nodeCountPerFile,
                                      std::size_t edgeCountPerFile,
                                      const ImportJsonDirArgs& args) {
    JsonParser parser;

    const FileUtils::Path statsPath = args._jsonDir / "stats.json";
    const FileUtils::Path nodeLabelsPath = args._jsonDir / "nodeLabels.json";
    const FileUtils::Path nodeLabelSetsPath = args._jsonDir / "labelSets.json";
    const FileUtils::Path edgeTypesPath = args._jsonDir / "edgeTypes.json";
    const FileUtils::Path nodePropertiesPath = args._jsonDir / "nodeProperties.json";
    const FileUtils::Path edgePropertiesPath = args._jsonDir / "edgeProperties.json";

    JobGroup jobs = jobSystem.newGroup();
    GraphStats stats;

    std::unique_ptr<Change> change = graph->newChange();

    {
        // Stats
        std::string statsData;
        if (!FileUtils::readContent(statsPath, statsData)) {
            spdlog::error("Could not read stats file");
            return false;
        }
        spdlog::info("Parsing database info");
        stats = parser.parseStats(statsData);

        spdlog::info("Database has {} nodes and {} edges", stats.nodeCount, stats.edgeCount);

        ChangeAccessor changeAccessor = change->access();

        // Node Labels
        std::string nodeLabelsData;
        if (!FileUtils::readContent(nodeLabelsPath, nodeLabelsData)) {
            spdlog::error("Could not read node labels file");
            return false;
        }

        spdlog::info("Parsing node labels");
        if (!parser.parseNodeLabels(changeAccessor, nodeLabelsData)) {
            spdlog::error("Could not parse node labels");
            return false;
        }

        // Node Labelsets
        std::string nodeLabelSetsData;
        if (!FileUtils::readContent(nodeLabelSetsPath, nodeLabelSetsData)) {
            spdlog::error("Could not read node label sets file");
            return false;
        }
        spdlog::info("Parsing node label sets");
        if (!parser.parseNodeLabelSets(changeAccessor, nodeLabelSetsData)) {
            spdlog::error("Could not parse node label sets");
            return false;
        }

        // Node Properties
        std::string nodePropertiesData;
        if (!FileUtils::readContent(nodePropertiesPath, nodePropertiesData)) {
            spdlog::error("Could not read node properties file");
            return false;
        }
        spdlog::info("Parsing node properties");
        if (!parser.parseNodeProperties(changeAccessor, nodePropertiesData)) {
            spdlog::error("Could not parse node properties");
            return false;
        }

        // Edge types
        std::string edgeTypesData;
        if (!FileUtils::readContent(edgeTypesPath, edgeTypesData)) {
            spdlog::error("Could not read edge types file");
            return false;
        }
        spdlog::info("Parsing edge types");
        if (!parser.parseEdgeTypes(changeAccessor, edgeTypesData)) {
            spdlog::error("Could not parse edge types");
            return false;
        }

        // Edge properties
        std::string edgePropertiesData;
        if (!FileUtils::readContent(edgePropertiesPath, edgePropertiesData)) {
            spdlog::error("Could not read edge properties file");
            return false;
        }
        if (!parser.parseEdgeProperties(changeAccessor, edgePropertiesData)) {
            spdlog::error("Could not parse edge properties");
            return false;
        }
    }

    // Query and parse nodes
    const size_t nodeSteps = stats.nodeCount / nodeCountPerFile
                           + (size_t)((stats.nodeCount % nodeCountPerFile) != 0);

    std::vector<SharedFuture<bool>> nodeResults(nodeSteps);

    for (size_t i = 0; i < nodeSteps; i++) {
        nodeResults[i] = jobs.submit<bool>(
            [&, i](Promise* p) {
                auto* promise = p->cast<bool>();
                std::string fileName = "nodes_" + std::to_string(i + 1) + ".json";

                std::string data;
                if (!FileUtils::readContent(args._jsonDir / fileName, data)) {
                    promise->set_value(false);
                    return;
                }

                const size_t remainder = (stats.nodeCount % nodeCountPerFile);
                const bool isLast = (i == nodeSteps - 1);
                const size_t currentCount = isLast
                                              ? (remainder == 0 ? nodeCountPerFile : remainder)
                                              : nodeCountPerFile;
                const size_t upperRange = i * nodeCountPerFile + currentCount;

                spdlog::info("Retrieving node range [{:>9}-{:<9}]",
                             i * nodeCountPerFile, upperRange);

                ChangeAccessor accessor = change->access();

                if (!parser.parseNodes(accessor, data)) {
                    spdlog::error("Could not parse nodes");
                    promise->set_value(false);
                    return;
                }

                promise->set_value(true);
            });
    }

    for (auto& nodeResult : nodeResults) {
        nodeResult.wait();
        if (!nodeResult.get()) {
            spdlog::error("Could not retrieve nodes");
            jobs.wait();
            return false;
        }
    }

    // Query and parse edges
    const size_t edgeSteps = stats.edgeCount / edgeCountPerFile
                           + (size_t)((stats.edgeCount % edgeCountPerFile) != 0);

    std::vector<SharedFuture<bool>> edgeResults(edgeSteps);

    for (size_t i = 0; i < edgeSteps; i++) {
        edgeResults[i] = jobs.submit<bool>(
            [&, i](Promise* p) {
                auto* promise = p->cast<bool>();
                std::string fileName = "edges_" + std::to_string(i + 1) + ".json";

                std::string data;
                if (!FileUtils::readContent(args._jsonDir / fileName, data)) {
                    promise->set_value(false);
                    return;
                }

                const size_t remainder = (stats.edgeCount % edgeCountPerFile);
                const bool isLast = (i == edgeSteps - 1);
                const size_t currentCount = isLast
                                              ? (remainder == 0 ? edgeCountPerFile : remainder)
                                              : edgeCountPerFile;
                const size_t upperRange = i * edgeCountPerFile + currentCount;
                spdlog::info("Retrieving edge range [{:>9}-{:<9}]",
                             i * edgeCountPerFile, upperRange);

                ChangeAccessor changeAccessor = change->access();

                if (!parser.parseEdges(changeAccessor, data)) {
                    spdlog::error("Could not parse edges");
                    promise->set_value(false);
                    return;
                }

                promise->set_value(true);
            });
    }

    for (auto& edgeResult : edgeResults) {
        edgeResult.wait();
        if (!edgeResult.get()) {
            spdlog::error("Could not retrieve edges");
            jobs.wait();
            return false;
        }
    }

    {
        ChangeAccessor accessor = change->access();

        if (auto res = accessor.submit(jobSystem); !res) {
            spdlog::error("Could not submit change: {}", res.error().fmtMessage());
            return false;
        }
    }

    return true;
}

bool Neo4jImporter::fromDumpFileToJsonDirImpl(JobSystem& jobSystem,
                                              Graph* graph,
                                              std::size_t nodeCountPerQuery,
                                              std::size_t edgeCountPerQuery,
                                              const DumpFileToJsonDirArgs& args) {
    Neo4jInstance instance(args._workDir);
    if (Neo4jInstance::isRunning()) {
        instance.stop();
    }

    if (!instance.setup()) {
        return false;
    }

    if (!instance.importDumpedDB(args._dumpFilePath)) {
        return false;
    }

    if (!instance.start()) {
        return false;
    }

    FileUtils::Path jsonDir = args._workDir / "json";

    if (!FileUtils::exists(jsonDir)) {
        if (!FileUtils::createDirectory(jsonDir)) {
            logt::CanNotCreateDir(jsonDir);
            return false;
        }
    }

    UrlToJsonDirArgs urlArgs {
        ._url = "localhost",
        ._urlSuffix = "/db/data/transaction/commit",
        ._username = "neo4j",
        ._password = "turing",
        ._port = 7474,
        ._workDir = args._workDir,
    };

    return Neo4jImporter::fromUrlToJsonDirImpl(jobSystem, graph, nodeCountPerQuery, edgeCountPerQuery, urlArgs);
}
