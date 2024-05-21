#include "Neo4jImport.h"

#include <spdlog/spdlog.h>

#include "DBDumper.h"
#include "JsonParser.h"
#include "Neo4JInstance.h"
#include "ThreadHandler.h"
#include "TimerStat.h"

#include "LogUtils.h"

Neo4jImport::Neo4jImport(db::DB* db, const Path& outDir)
    : _db(db),
      _outDir(outDir)
{
}

Neo4jImport::~Neo4jImport() {
}

bool Neo4jImport::importNeo4j(const Path& filepath, const std::string& networkName) {
    spdlog::info("Importing Neo4J dump file {}", filepath.string());
    TimerStat timer {"Neo4j: import"};

    const FileUtils::Path jsonDir = _outDir / "json";
    const FileUtils::Path statsFile = jsonDir / "stats.json";
    const FileUtils::Path nodePropertiesFile = jsonDir / "nodeProperties.json";
    const FileUtils::Path edgePropertiesFile = jsonDir / "edgeProperties.json";

    Neo4jInstance instance(_outDir);
    if (Neo4jInstance::isRunning()) {
        instance.stop();
    }
    instance.setup();
    instance.importDumpedDB(filepath);
    instance.start();

    if (!FileUtils::exists(jsonDir)) {
        if (!FileUtils::createDirectory(jsonDir)) {
            logt::CanNotCreateDir(jsonDir.string());
            return false;
        }
    }

    // Initialization of the parser
    JsonParser parser(_db, networkName);
    parser.setReducedOutput(true);
    const JsonParsingStats& stats = parser.getStats();

    // Preparing the requests
    ThreadHandler handler {};
    handler.initialize();

    // Start the request queue
    if (!handler.start(parser)) {
        instance.destroy();
        return false;
    }

    handler.requests.stats.writeToFile(statsFile);

    // Done, clearing stats data
    handler.requests.stats.clear();

    spdlog::info("Imported {} nodes from Neo4J", stats.nodeCount);
    spdlog::info("Imported {} edges from Neo4J", stats.edgeCount);

    // Waiting for the node properties request to finish
    handler.requests.nodeProperties.waitReady();

    if (!handler.requests.nodeProperties.success()) {
        instance.destroy();
        handler.requests.nodeProperties.reportError();
        return false;
    }
    handler.requests.nodeProperties.writeToFile(nodePropertiesFile);

    // Parsing node properties data
    if (!parser.parse(handler.requests.nodeProperties.getData(),
                      JsonParser::FileFormat::Neo4j4_NodeProperties)) {
        instance.destroy();
        return false;
    }

    // Done, clearing node properties data
    handler.requests.nodeProperties.clear();

    // Nodes
    size_t i = 1;

    for (auto& nodeRequest : handler.requests.nodes) {
        // Waiting for the nodes request to finish
        nodeRequest.waitReady();

        if (!nodeRequest.success()) {
            instance.destroy();
            return false;
        }

        // Parsing the nodes data
        if (!parser.parse(nodeRequest.getData(),
                          JsonParser::FileFormat::Neo4j4_Nodes)) {
            instance.destroy();
            return false;
        }

        std::string filename = "nodes_" + std::to_string(i) + ".json";
        nodeRequest.writeToFile(jsonDir / filename);
        nodeRequest.clear();
        i += 1;
    }

    // Done, clearing nodes data
    handler.requests.nodes.clear();

    // Waiting for the edge properties request to finish
    handler.requests.edgeProperties.waitReady();

    if (!handler.requests.edgeProperties.success()) {
        instance.destroy();
        handler.requests.edgeProperties.reportError();
        return false;
    }
    handler.requests.edgeProperties.writeToFile(edgePropertiesFile);

    // Parsing edge properties data
    if (!parser.parse(handler.requests.edgeProperties.getData(),
                      JsonParser::FileFormat::Neo4j4_EdgeProperties)) {
        instance.destroy();
        return false;
    }

    // Done, clearing edge properties data
    handler.requests.edgeProperties.clear();

    // Nodes
    i = 1;

    for (auto& edgeRequest : handler.requests.edges) {
        // Waiting for the edges request to finish
        edgeRequest.waitReady();

        if (!edgeRequest.success()) {
            instance.destroy();
            return false;
        }

        // Parsing the edges data
        if (!parser.parse(edgeRequest.getData(),
                          JsonParser::FileFormat::Neo4j4_Edges)) {
            instance.destroy();
            return false;
        }

        std::string filename = "edges_" + std::to_string(i) + ".json";
        edgeRequest.writeToFile(jsonDir / filename);
        edgeRequest.clear();
        i += 1;
    }

    // Done, clearing edges data
    handler.requests.edges.clear();

    spdlog::info("{} node property errors occured during Neo4J db load",
                 stats.nodePropErrors);
    spdlog::info("{} edge property errors occured during Neo4J db load",
                 stats.edgePropErrors);

    spdlog::info("{} node property warnings occured during Neo4J db load",
                 stats.nodePropWarnings);
    spdlog::info("{} edge property warnings occured during Neo4J db load",
                 stats.edgePropWarnings);

    spdlog::info("{} unsupported node properties occured during Neo4J db load",
                 stats.unsupportedNodeProps);
    spdlog::info("{} unsupported edge properties occured during Neo4J db load",
                 stats.unsupportedEdgeProps);

    spdlog::info("{} illformed node properties occured during Neo4J db load",
                 stats.illformedNodeProps);
    spdlog::info("{} illformed edge properties occured during Neo4J db load",
                 stats.illformedNodeProps);

    spdlog::info("Parsed {} Neo4J nodes", stats.parsedNodes);
    spdlog::info("Parsed {} Neo4J edges", stats.parsedEdges);

    handler.join();
    instance.destroy();

    return true;
}

bool Neo4jImport::importNeo4jUrl(const std::string& url,
                                uint64_t port,
                                const std::string& username,
                                const std::string& password,
                                const std::string& urlSuffix,
                                const std::string& networkName) {
    TimerStat timer {"Neo4j: import Url"};

    const FileUtils::Path jsonDir = _outDir / "json";
    const FileUtils::Path statsFile = jsonDir / "stats.json";
    const FileUtils::Path nodePropertiesFile = jsonDir / "nodeProperties.json";
    const FileUtils::Path edgePropertiesFile = jsonDir / "edgeProperties.json";

    if (!FileUtils::exists(jsonDir)) {
        if (!FileUtils::createDirectory(jsonDir)) {
            logt::CanNotCreateDir(jsonDir.string());
            return false;
        }
    }

    // Initialization of the parser
    JsonParser parser(_db, networkName);
    parser.setReducedOutput(true);
    const JsonParsingStats& stats = parser.getStats();

    // Preparing the requests
    ThreadHandler handler;
    handler.setUrl(url);
    handler.setUrlSuffix(urlSuffix);
    handler.setPort(port);
    handler.setUsername(username);
    handler.setPassword(password);
    handler.initialize();

    // Start the request queue
    if (!handler.start(parser)) {
        return false;
    }

    handler.requests.stats.writeToFile(statsFile);

    // Done, clearing stats data
    handler.requests.stats.clear();

    spdlog::info("Database has {} nodes", stats.nodeCount);
    spdlog::info("Database has {} edges", stats.edgeCount);

    // Waiting for the node properties request to finish
    handler.requests.nodeProperties.waitReady();

    if (!handler.requests.nodeProperties.success()) {
        handler.requests.nodeProperties.reportError();
        return false;
    }
    handler.requests.nodeProperties.writeToFile(nodePropertiesFile);

    // Parsing node properties data
    if (!parser.parse(handler.requests.nodeProperties.getData(),
                      JsonParser::FileFormat::Neo4j4_NodeProperties)) {
        return false;
    }

    // Done, clearing node properties data
    handler.requests.nodeProperties.clear();

    // Nodes
    size_t i = 1;

    for (auto& nodeRequest : handler.requests.nodes) {
        // Waiting for the nodes request to finish
        nodeRequest.waitReady();

        if (!nodeRequest.success()) {
            return false;
        }

        // Parsing the nodes data
        if (!parser.parse(nodeRequest.getData(),
                          JsonParser::FileFormat::Neo4j4_Nodes)) {
            return false;
        }

        std::string filename = "nodes_" + std::to_string(i) + ".json";
        nodeRequest.writeToFile(jsonDir / filename);
        nodeRequest.clear();
        i += 1;
    }

    // Done, clearing nodes data
    handler.requests.nodes.clear();

    // Waiting for the edge properties request to finish
    handler.requests.edgeProperties.waitReady();

    if (!handler.requests.edgeProperties.success()) {
        handler.requests.edgeProperties.reportError();
        return false;
    }
    handler.requests.edgeProperties.writeToFile(edgePropertiesFile);

    // Parsing edge properties data
    if (!parser.parse(handler.requests.edgeProperties.getData(),
                      JsonParser::FileFormat::Neo4j4_EdgeProperties)) {
        return false;
    }

    // Done, clearing edge properties data
    handler.requests.edgeProperties.clear();

    // Nodes
    i = 1;

    for (auto& edgeRequest : handler.requests.edges) {
        // Waiting for the edges request to finish
        edgeRequest.waitReady();

        if (!edgeRequest.success()) {
            return false;
        }

        // Parsing the edges data
        if (!parser.parse(edgeRequest.getData(),
                          JsonParser::FileFormat::Neo4j4_Edges)) {
            return false;
        }

        std::string filename = "edges_" + std::to_string(i) + ".json";
        edgeRequest.writeToFile(jsonDir / filename);
        edgeRequest.clear();
        i += 1;
    }

    // Done, clearing edges data
    handler.requests.edges.clear();

    spdlog::info("{} node property errors occured during Neo4J loading",
                 stats.nodePropErrors);
    spdlog::info("{} edge property errors occured during Neo4J loading",
                 stats.edgePropErrors);

    spdlog::info("{} node property warnings occured during Neo4J loading",
                 stats.nodePropWarnings);
    spdlog::info("{} edge property warnings occured during Neo4J loading",
                 stats.edgePropWarnings);

    spdlog::info("{} unsupported node properties encountered during Neo4J loading",
                 stats.unsupportedNodeProps);
    spdlog::info("{} unsupported edge properties encountered during Neo4J loading",
                 stats.unsupportedEdgeProps);

    spdlog::info("{} illformed node properties encountered during Neo4J loading",
                 stats.illformedNodeProps);
    spdlog::info("{} illformed edge properties encountered during Neo4J loading",
                 stats.illformedEdgeProps);

    spdlog::info("Neo4J nodes parsed: {}", stats.parsedNodes);
    spdlog::info("Neo4J edges parsed: {}", stats.parsedEdges);

    handler.join();
    return true;
}

bool Neo4jImport::importJsonNeo4j(const Path& jsonDir, const std::string& networkName) {
    spdlog::info("Import Neo4J json files in {}", jsonDir.string());
    TimerStat timer {"Neo4j: import"};

    const FileUtils::Path statsFile = jsonDir / "stats.json";
    const FileUtils::Path nodePropertiesFile = jsonDir / "nodeProperties.json";
    const FileUtils::Path edgePropertiesFile = jsonDir / "edgeProperties.json";

    if (!FileUtils::exists(jsonDir)) {
        logt::DirectoryDoesNotExist(jsonDir.string());
        return false;
    }

    // Initialization of the parser
    JsonParser parser(_db, networkName);
    parser.setReducedOutput(true);
    const JsonParsingStats& stats = parser.getStats();

    if (!parser.parseJsonDir(jsonDir, JsonParser::DirFormat::Neo4j4)) {
        return false;
    }

    spdlog::info("{} node property errors occured during Neo4J db load",
                 stats.nodePropErrors);
    spdlog::info("{} edge property errors occured during Neo4J db load",
                 stats.edgePropErrors);

    spdlog::info("{} node property warnings occured during Neo4J db load",
                 stats.nodePropWarnings);
    spdlog::info("{} edge property warnings occured during Neo4J db load",
                 stats.edgePropWarnings);

    spdlog::info("{} unsupported node properties occured during Neo4J db load",
                 stats.unsupportedNodeProps);
    spdlog::info("{} unsupported edge properties occured during Neo4J db load",
                 stats.unsupportedEdgeProps);

    spdlog::info("{} illformed node properties occured during Neo4J db load",
                 stats.illformedNodeProps);
    spdlog::info("{} illformed edge properties occured during Neo4J db load",
                 stats.illformedNodeProps);

    spdlog::info("Parsed {} Neo4J nodes", stats.parsedNodes);
    spdlog::info("Parsed {} Neo4J edges", stats.parsedEdges);

    return true;
}
