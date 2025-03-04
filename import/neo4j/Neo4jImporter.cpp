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
#include "TimerStat.h"

using namespace db;

bool Neo4jImporter::importUrl(JobSystem& jobSystem,
                              Graph* graph,
                              size_t nodeCountPerQuery,
                              size_t edgeCountPerQuery,
                              const ImportUrlArgs& args) {
    JsonParser parser(graph);
    QueryManager manager;
    const FileUtils::Path jsonDir = args._workDir / "json";
    JobGroup jobs = jobSystem.newGroup();

    if (args._writeFiles) {
        if (!FileUtils::createDirectory(jsonDir)) {
            spdlog::error("Could not create json directory {}", jsonDir.string());
            return false;
        }

        const FileUtils::Path graphTypePath = args._workDir / "type";
        const auto typeTag = GraphFileTypeDescription::value(GraphFileType::NEO4J_JSON);

        if (!FileUtils::writeFile(graphTypePath, typeTag.data())) {
            spdlog::error("Could not write graph type file {}", graphTypePath.string());
            return false;
        }
    }

    manager.setUrl(args._url);
    manager.setUrlSuffix(args._urlSuffix);
    manager.setUsername(args._username);
    manager.setPassword(args._password);
    manager.setPort(args._port);

    if (!manager.isServerRunning()) {
        std::string completeUrl = args._url + args._urlSuffix;
        spdlog::info("No anwser from: {}:{}{}", args._url, args._port, args._urlSuffix);
        return false;
    }

    const auto writeJsonFile = [&](auto& future,
                                   FileUtils::Path jsonPath,
                                   std::string& content) {
        jobs.submit<void>(
            [&args, &future, jsonPath = std::move(jsonPath), &content](Promise*) {
                spdlog::info("Writing json file: {}", jsonPath.filename().c_str());
                FileUtils::writeFile(jsonPath, content);
                if (!args._writeFilesOnly) {
                    future.wait();
                }
                content.clear();
                content.shrink_to_fit();
            });
    };

    const auto submitQuery = [&](QueryManager::Query& q) {
        q._future = jobs.submit<QueryManager::Response>(
            [&](Promise* p) {
                auto* promise = p->cast<QueryManager::Response>();

                if (!q._request->exec()) {
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

    SharedFuture<std::optional<GraphStats>> statsResult;
    SharedFuture<bool> nodeLabelsResult;
    SharedFuture<bool> nodeLabelSetsResult;
    SharedFuture<bool> edgeTypeResult;
    SharedFuture<bool> nodePropertiesResult;
    SharedFuture<bool> edgePropertiesResult;

    // Submitting meta data HTTP queries
    submitQuery(statsQuery);
    submitQuery(nodeLabelsQuery);
    submitQuery(nodeLabelSetsQuery);
    submitQuery(edgeTypesQuery);
    submitQuery(nodePropertiesQuery);
    submitQuery(edgePropertiesQuery);

    // Parsing stats to get the node and edge count
    if (!statsQuery.waitAnswer()) {
        spdlog::error("Could not retrieve database info");
        jobs.wait();
        return false;
    }

    statsResult = jobs.submit<std::optional<GraphStats>>(
        [&](Promise* p) {
            auto* promise = p->cast<std::optional<GraphStats>>();
            auto stats = parser.parseStats(statsQuery._response);
            spdlog::info("Retrieving database info");
            promise->set_value(stats);
            spdlog::info("Database has {} nodes and {} edges", stats.nodeCount, stats.edgeCount);
        });

    // Parsing node labels
    if (!nodeLabelsQuery.waitAnswer()) {
        spdlog::error("Could not retrieve node labels");
        jobs.wait();
        return false;
    }

    if (!args._writeFilesOnly) {
        nodeLabelsResult = jobs.submit<bool>(
            [&](Promise* p) {
                auto* promise = p->cast<bool>();
                spdlog::info("Retrieving node labels");
                promise->set_value(parser.parseNodeLabels(nodeLabelsQuery._response));
            });
    }

    // Parsing node label sets
    if (!nodeLabelSetsQuery.waitAnswer()) {
        spdlog::error("Could not retrieve node label sets");
        jobs.wait();
        return false;
    }

    if (!args._writeFilesOnly) {
        nodeLabelSetsResult = jobs.submit<bool>(
            [&](Promise* p) {
                auto* promise = p->cast<bool>();
                spdlog::info("Retrieving node label set");
                promise->set_value(parser.parseNodeLabelSets(nodeLabelsQuery._response));
            });
    }

    // Parsing node properties
    if (!nodePropertiesQuery.waitAnswer()) {
        spdlog::error("Could not retrieve node properties");
        jobs.wait();
        return false;
    }

    if (!args._writeFilesOnly) {
        nodePropertiesResult = jobs.submit<bool>(
            [&](Promise* p) {
                auto* promise = p->cast<bool>();
                spdlog::info("Retrieving node properties");
                promise->set_value(parser.parseNodeProperties(nodePropertiesQuery._response));
            });
    }

    // While the node meta data are being parsed, write the json files
    if (args._writeFiles) {
        writeJsonFile(statsResult, jsonDir / "stats.json", statsQuery._response);
        writeJsonFile(nodeLabelsResult, jsonDir / "nodeLabels.json", nodeLabelsQuery._response);
        writeJsonFile(nodeLabelSetsResult, jsonDir / "labelSets.json", nodeLabelSetsQuery._response);
        writeJsonFile(nodePropertiesResult, jsonDir / "nodeProperties.json", nodePropertiesQuery._response);
    }

    // Retrieve node and edge count
    statsResult.wait();
    auto statsOptional = statsResult.get();
    if (!statsOptional.has_value()) {
        spdlog::error("Could not retrieve stats");
        jobs.wait();
        return false;
    }
    const auto& stats = statsOptional.value();

    // Wait for node properties and labels to be known before parsing nodes
    if (!args._writeFilesOnly) {
        nodeLabelsResult.wait();
        if (!nodeLabelsResult.get()) {
            spdlog::error("Could not retrieve node labels");
            jobs.wait();
            return false;
        }

        nodePropertiesResult.wait();
        if (!nodePropertiesResult.get()) {
            spdlog::error("Could not retrieve node properties");
            jobs.wait();
            return false;
        }
    }

    // Query and parse nodes (and write json files)
    auto nodeQueries = manager.nodesQueries(stats.nodeCount, nodeCountPerQuery);
    std::vector<SharedFuture<bool>> nodeResults(nodeQueries.size());
    size_t i = 0;
    for (auto& q : nodeQueries) {
        // Submit the query
        submitQuery(q);

        // Create the buffer that will hold the node info
        DataPartBuilder* buf = nullptr;
        if (!args._writeFilesOnly) {
            buf = &parser.newDataBuffer(nodeCountPerQuery, 0);
        }

        // This job waits for the query to end before starting the parsing
        nodeResults[i] = jobs.submit<bool>(
            [&, buf, i](Promise* p) {
                auto* promise = p->cast<bool>();

                if (!q.waitAnswer()) {
                    promise->set_value(false);
                    return;
                }

                const size_t rawUpperRange = nodeCountPerQuery * (i + 1);
                const size_t overflow = (rawUpperRange % stats.nodeCount)
                                      * (rawUpperRange / stats.nodeCount);
                const size_t upperRange = rawUpperRange - overflow;
                spdlog::info("Retrieving node range [{}-{}] / {}",
                             i * nodeCountPerQuery,
                             upperRange,
                             stats.nodeCount);
                if (!args._writeFilesOnly) {
                    promise->set_value(parser.parseNodes(q._response, *buf));
                }
            });

        if (args._writeFiles) {
            // This job wait for the query to end before writing the json file
            jobs.submit<void>(
                [&, i](Promise*) {
                    if (!q.waitAnswer()) {
                        return;
                    }

                    std::string fileName = "nodes_" + std::to_string(i + 1) + ".json";
                    FileUtils::Path jsonPath = jsonDir / fileName;

                    spdlog::info("Writing json file: {}", jsonPath.filename().string());
                    FileUtils::writeFile(jsonPath, q._response);
                    if (!args._writeFilesOnly) {
                        nodeResults[i].wait();
                    }
                    q._response.clear();
                    q._response.shrink_to_fit();
                });
        }

        i++;
    }

    // Parsing edge types
    if (!edgeTypesQuery.waitAnswer()) {
        spdlog::error("Could not retrieve edge types");
        jobs.wait();
        return false;
    }

    if (!args._writeFilesOnly) {
        edgeTypeResult = jobs.submit<bool>(
            [&](Promise* p) {
                auto* promise = p->cast<bool>();
                spdlog::info("Retrieving edge types");
                promise->set_value(parser.parseEdgeTypes(edgeTypesQuery._response));
            });
    }

    // Parsing edge properties
    if (!edgePropertiesQuery.waitAnswer()) {
        spdlog::error("Could not retrieve edge properties");
        jobs.wait();
        return false;
    }

    if (!args._writeFilesOnly) {
        edgePropertiesResult = jobs.submit<bool>(
            [&](Promise* p) {
                auto* promise = p->cast<bool>();
                spdlog::info("Retrieving edge properties");
                promise->set_value(parser.parseEdgeProperties(edgePropertiesQuery._response));
            });
    }

    // While the edge meta data are being parsed, write the json files
    if (args._writeFiles) {
        writeJsonFile(edgeTypeResult, jsonDir / "edgeTypes.json", edgeTypesQuery._response);
        writeJsonFile(edgePropertiesResult, jsonDir / "edgeProperties.json", edgePropertiesQuery._response);
    }

    // Wait for the end of the node results before parsing the edges
    if (!args._writeFilesOnly) {
        for (auto& nodeResult : nodeResults) {
            nodeResult.wait();
            if (!nodeResult.get()) {
                spdlog::error("Could not retrieve nodes");
                jobs.wait();
                return false;
            }
        }
    }

    jobs.wait();

    if (!args._writeFilesOnly) {
        {
            TimerStat t {"Build and push DataPart for nodes"};
            parser.pushDataParts(*graph, jobSystem);
        }
    }

    parser.resetGraphView();

    // Query and parse edges (and write json files)
    auto edgeQueries = manager.edgesQueries(stats.edgeCount, edgeCountPerQuery);
    std::vector<SharedFuture<bool>> edgeResults(edgeQueries.size());
    i = 0;
    for (auto& q : edgeQueries) {
        // Submit the query
        submitQuery(q);

        // Create the buffer that will hold the edge info
        DataPartBuilder* buf = nullptr;
        if (!args._writeFilesOnly) {
            buf = &parser.newDataBuffer(0, edgeCountPerQuery);
        }

        // This job waits for the query to end before starting the parsing
        edgeResults[i] = jobs.submit<bool>(
            [&, buf, i](Promise* p) {
                auto* promise = p->cast<bool>();
                if (!q.waitAnswer()) {
                    promise->set_value(false);
                    return;
                }
                const size_t rawUpperRange = edgeCountPerQuery * (i + 1);
                const size_t overflow = (rawUpperRange % stats.edgeCount)
                                      * (rawUpperRange / stats.edgeCount);
                const size_t upperRange = rawUpperRange - overflow;
                spdlog::info("Retrieving edge range [{}-{}] / {}",
                             i * edgeCountPerQuery,
                             upperRange,
                             stats.edgeCount);
                if (!args._writeFilesOnly) {
                    promise->set_value(parser.parseEdges(q._response, *buf));
                }
            });

        if (args._writeFiles) {
            // This job wait for the query to end before writing the json file
            jobs.submit<void>(
                [&, i](Promise*) {
                    if (!q.waitAnswer()) {
                        return;
                    }

                    std::string fileName = "edges_" + std::to_string(i + 1) + ".json";
                    FileUtils::Path jsonPath = jsonDir / fileName;

                    spdlog::info("Writing json file: {}", jsonPath.filename().c_str());
                    FileUtils::writeFile(jsonPath, q._response);
                    if (!args._writeFilesOnly) {
                        edgeResults[i].wait();
                    }
                    q._response.clear();
                    q._response.shrink_to_fit();
                });
        }

        i++;
    }

    // Wait for the end before pushing the datapart
    // (json files are still being written in the bg)

    if (!args._writeFilesOnly) {
        for (auto& edgeResult : edgeResults) {
            edgeResult.wait();
            if (!edgeResult.get()) {
                spdlog::error("Could not retrieve edges");
                jobs.wait();
                return false;
            }
        }
    }

    jobs.wait();

    if (!args._writeFilesOnly) {
        TimerStat t {"Build and push DataPart for edges"};
        parser.pushDataParts(*graph, jobSystem);
    }

    return true;
}

bool Neo4jImporter::importJsonDir(JobSystem& jobSystem,
                                  Graph* graph,
                                  std::size_t nodeCountPerFile,
                                  std::size_t edgeCountPerFile,
                                  const ImportJsonDirArgs& args) {
    JsonParser parser(graph);

    const FileUtils::Path statsPath = args._jsonDir / "stats.json";
    const FileUtils::Path nodeLabelsPath = args._jsonDir / "nodeLabels.json";
    const FileUtils::Path nodeLabelSetsPath = args._jsonDir / "labelSets.json";
    const FileUtils::Path edgeTypesPath = args._jsonDir / "edgeTypes.json";
    const FileUtils::Path nodePropertiesPath = args._jsonDir / "nodeProperties.json";
    const FileUtils::Path edgePropertiesPath = args._jsonDir / "edgeProperties.json";
    JobGroup jobs = jobSystem.newGroup();

    auto statsRes = jobs.submit<std::optional<GraphStats>>(
        [&](Promise* p) {
            auto* promise = p->cast<std::optional<GraphStats>>();
            std::string data;

            if (!FileUtils::readContent(statsPath, data)) {
                promise->set_value(std::nullopt);
                return;
            }
            spdlog::info("Retrieving database info");
            auto stats = parser.parseStats(data);
            promise->set_value(stats);
        });

    auto nodeLabelsRes = jobs.submit<bool>(
        [&](Promise* p) {
            auto* promise = p->cast<bool>();
            std::string data;

            if (!FileUtils::readContent(nodeLabelsPath, data)) {
                promise->set_value(false);
                return;
            }

            spdlog::info("Retrieving node labels");
            promise->set_value(parser.parseNodeLabels(data));
        });

    auto edgeTypesRes = jobs.submit<bool>(
        [&](Promise* p) {
            auto* promise = p->cast<bool>();
            std::string data;

            if (!FileUtils::readContent(edgeTypesPath, data)) {
                promise->set_value(false);
                return;
            }

            spdlog::info("Retrieving edge types");
            promise->set_value(parser.parseEdgeTypes(data));
        });

    auto rawStats = statsRes.get();
    if (!rawStats.has_value()) {
        spdlog::error("Could not retrieve database info");
        jobs.wait();
        return false;
    }
    auto stats = rawStats.value();
    spdlog::info("Database has {} nodes and {} edges", stats.nodeCount, stats.edgeCount);

    if (!nodeLabelsRes.get()) {
        spdlog::error("Could not retrieve node labels");
        jobs.wait();
        return false;
    }

    if (!edgeTypesRes.get()) {
        spdlog::error("Could not retrieve edge types");
        jobs.wait();
        return false;
    }

    auto nodeLabelSetsRes = jobs.submit<bool>(
        [&](Promise* p) {
            auto* promise = p->cast<bool>();
            std::string data;

            if (!FileUtils::readContent(nodeLabelSetsPath, data)) {
                promise->set_value(false);
                return;
            }

            spdlog::info("Retrieving node label sets");
            promise->set_value(parser.parseNodeLabelSets(data));
        });


    auto nodePropertiesRes = jobs.submit<bool>(
        [&](Promise* p) {
            auto* promise = p->cast<bool>();
            std::string data;

            if (!FileUtils::readContent(nodePropertiesPath, data)) {
                promise->set_value(false);
                return;
            }

            spdlog::info("Retrieving node properties");
            promise->set_value(parser.parseNodeProperties(data));
        });

    nodePropertiesRes.wait();

    auto edgePropertiesRes = jobs.submit<bool>(
        [&](Promise* p) {
            auto* promise = p->cast<bool>();
            std::string data;

            if (!FileUtils::readContent(edgePropertiesPath, data)) {
                promise->set_value(false);
                return;
            }

            spdlog::info("Retrieving edge properties");
            promise->set_value(parser.parseEdgeProperties(data));
        });

    if (!nodePropertiesRes.get()) {
        spdlog::error("Could not retrieve node properties");
        jobs.wait();
        return false;
    }

    // We need to parse the label sets before we parse every node
    if (!nodeLabelSetsRes.get()) {
        spdlog::error("Could not retrieve node label sets");
        jobs.wait();
        return false;
    }

    // Query and parse nodes
    const size_t nodeSteps = stats.nodeCount / nodeCountPerFile
                           + (size_t)((stats.nodeCount % nodeCountPerFile) != 0);
    std::vector<SharedFuture<bool>> nodeResults(nodeSteps);

    const size_t nodeCountRemainder = stats.nodeCount % nodeCountPerFile;
    for (size_t i = 0; i < nodeSteps; i++) {
        // Create the buffer that will hold the node info
        DataPartBuilder& buf = parser.newDataBuffer(nodeCountPerFile, 0);

        nodeResults[i] = jobs.submit<bool>(
            [&, i](Promise* p) {
                auto* promise = p->cast<bool>();
                std::string fileName = "nodes_" + std::to_string(i + 1) + ".json";

                std::string data;
                if (!FileUtils::readContent(args._jsonDir / fileName, data)) {
                    promise->set_value(false);
                    return;
                }

                const bool isLastFile = (i == nodeSteps - 1);
                const size_t currentCount = isLastFile
                                         ? (nodeCountRemainder == 0 ? nodeCountPerFile : nodeCountRemainder)
                                         : nodeCountPerFile;
                spdlog::info("Retrieving node range [{}-{}] / {}",
                             i * nodeCountPerFile,
                             currentCount + nodeCountPerFile * i,
                             stats.nodeCount);
                promise->set_value(parser.parseNodes(data, buf));
            });
    }

    if (!edgePropertiesRes.get()) {
        spdlog::error("Could not retrieve edge properties");
        jobs.wait();
        return false;
    }

    for (auto& nodeResult : nodeResults) {
        nodeResult.wait();
        if (!nodeResult.get()) {
            spdlog::error("Could not retrieve nodes");
            jobs.wait();
            return false;
        }
    }

    jobs.wait();
    {
        TimerStat t {"Build and push DataPart for nodes"};
        parser.pushDataParts(*graph, jobSystem);
    }

    parser.resetGraphView();

    // Query and parse edges
    const size_t edgeSteps = stats.edgeCount / edgeCountPerFile
                           + (size_t)((stats.edgeCount % edgeCountPerFile) != 0);
    const size_t edgeCountRemainder = stats.edgeCount % edgeCountPerFile;
    std::vector<SharedFuture<bool>> edgeResults(edgeSteps);

    for (size_t i = 0; i < edgeSteps; i++) {
        // Create the buffer that will hold the edge info
        DataPartBuilder& buf = parser.newDataBuffer(0, edgeCountPerFile);

        edgeResults[i] = jobs.submit<bool>(
            [&, i](Promise* p) {
                auto* promise = p->cast<bool>();
                std::string fileName = "edges_" + std::to_string(i + 1) + ".json";

                std::string data;
                if (!FileUtils::readContent(args._jsonDir / fileName, data)) {
                    promise->set_value(false);
                    return;
                }

                const bool isLastFile = (i == nodeSteps - 1);
                const size_t currentCount = isLastFile
                                         ? (edgeCountRemainder == 0 ? edgeCountPerFile : edgeCountRemainder)
                                         : edgeCountPerFile;
                spdlog::info("Retrieving edge range [{}-{}] / {}",
                             i * edgeCountPerFile,
                             currentCount + edgeCountPerFile * i,
                             stats.edgeCount);
                promise->set_value(parser.parseEdges(data, buf));
            });
    }

    // Wait for the end before pushing the datapart
    // (json files are still being written in the bg)
    for (auto& edgeResult : edgeResults) {
        edgeResult.wait();
        if (!edgeResult.get()) {
            spdlog::error("Could not retrieve edges");
            jobs.wait();
            return false;
        }
    }

    jobs.wait();

    {
        TimerStat t {"Build and push DataPart for edges"};
        parser.pushDataParts(*graph, jobSystem);
    }

    return true;
}

bool Neo4jImporter::importDumpFile(JobSystem& jobSystem,
                                   Graph* graph,
                                   std::size_t nodeCountPerQuery,
                                   std::size_t edgeCountPerQuery,
                                   const ImportDumpFileArgs& args) {
    Neo4jInstance instance(args._workDir);

    if (Neo4jInstance::isRunning()) {
        instance.stop();
    }

    instance.setup();
    instance.importDumpedDB(args._dumpFilePath);
    instance.start();

    FileUtils::Path jsonDir = args._workDir / "json";

    if (!FileUtils::exists(jsonDir)) {
        if (!FileUtils::createDirectory(jsonDir)) {
            logt::CanNotCreateDir(jsonDir);
            return false;
        }
    }

    ImportUrlArgs urlArgs {
        ._url = "localhost",
        ._urlSuffix = "/db/data/transaction/commit",
        ._username = "neo4j",
        ._password = "turing",
        ._port = 7474,
        ._writeFiles = args._writeFiles,
        ._writeFilesOnly = args._writeFilesOnly,
        ._workDir = args._workDir,
    };

    const bool res = Neo4jImporter::importUrl(jobSystem, graph, nodeCountPerQuery, edgeCountPerQuery, urlArgs);

    instance.destroy();
    return res;
}
