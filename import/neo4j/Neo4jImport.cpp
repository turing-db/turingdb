#include "Neo4jImport.h"
#include "BioLog.h"
#include "DBDumper.h"
#include "JsonParser.h"
#include "MsgCommon.h"
#include "MsgImport.h"
#include "Neo4JInstance.h"
#include "ThreadHandler.h"
#include "TimerStat.h"

using namespace Log;

Neo4jImport::Neo4jImport(db::DB* db, const Path& outDir)
    : _db(db),
      _outDir(outDir)
{
}

Neo4jImport::~Neo4jImport() {
}

bool Neo4jImport::importNeo4j(const Path& filepath, const std::string& networkName) {
    BioLog::log(msg::INFO_NEO4J_IMPORT_DUMP_FILE() << filepath.string());
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
            BioLog::log(msg::ERROR_FAILED_TO_CREATE_DIRECTORY() << jsonDir);
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

    Log::BioLog::log(msg::INFO_NEO4J_NODE_COUNT() << stats.nodeCount);
    Log::BioLog::log(msg::INFO_NEO4J_EDGE_COUNT() << stats.edgeCount);

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

    Log::BioLog::log(msg::INFO_NEO4J_NODE_PROP_ERROR_COUNT()
                     << stats.nodePropErrors);
    Log::BioLog::log(msg::INFO_NEO4J_EDGE_PROP_ERROR_COUNT()
                     << stats.edgePropErrors);

    Log::BioLog::log(msg::INFO_NEO4J_NODE_PROP_WARNING_COUNT()
                     << stats.nodePropWarnings);
    Log::BioLog::log(msg::INFO_NEO4J_EDGE_PROP_WARNING_COUNT()
                     << stats.edgePropWarnings);

    Log::BioLog::log(msg::INFO_NEO4J_UNSUPPORTED_NODE_PROP_COUNT()
                     << stats.unsupportedNodeProps);
    Log::BioLog::log(msg::INFO_NEO4J_UNSUPPORTED_EDGE_PROP_COUNT()
                     << stats.unsupportedEdgeProps);

    Log::BioLog::log(msg::INFO_NEO4J_ILLFORMED_NODE_PROP_COUNT()
                     << stats.illformedNodeProps);
    Log::BioLog::log(msg::INFO_NEO4J_ILLFORMED_EDGE_PROP_COUNT()
                     << stats.illformedEdgeProps);

    Log::BioLog::log(msg::INFO_NEO4J_PARSED_NODE_COUNT() << stats.parsedNodes);
    Log::BioLog::log(msg::INFO_NEO4J_PARSED_EDGE_COUNT() << stats.parsedEdges);

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
            BioLog::log(msg::ERROR_FAILED_TO_CREATE_DIRECTORY() << jsonDir);
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

    Log::BioLog::log(msg::INFO_NEO4J_NODE_COUNT() << stats.nodeCount);
    Log::BioLog::log(msg::INFO_NEO4J_EDGE_COUNT() << stats.edgeCount);

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

    Log::BioLog::log(msg::INFO_NEO4J_NODE_PROP_ERROR_COUNT()
                     << stats.nodePropErrors);
    Log::BioLog::log(msg::INFO_NEO4J_EDGE_PROP_ERROR_COUNT()
                     << stats.edgePropErrors);

    Log::BioLog::log(msg::INFO_NEO4J_NODE_PROP_WARNING_COUNT()
                     << stats.nodePropWarnings);
    Log::BioLog::log(msg::INFO_NEO4J_EDGE_PROP_WARNING_COUNT()
                     << stats.edgePropWarnings);

    Log::BioLog::log(msg::INFO_NEO4J_UNSUPPORTED_NODE_PROP_COUNT()
                     << stats.unsupportedNodeProps);
    Log::BioLog::log(msg::INFO_NEO4J_UNSUPPORTED_EDGE_PROP_COUNT()
                     << stats.unsupportedEdgeProps);

    Log::BioLog::log(msg::INFO_NEO4J_ILLFORMED_NODE_PROP_COUNT()
                     << stats.illformedNodeProps);
    Log::BioLog::log(msg::INFO_NEO4J_ILLFORMED_EDGE_PROP_COUNT()
                     << stats.illformedEdgeProps);

    Log::BioLog::log(msg::INFO_NEO4J_PARSED_NODE_COUNT() << stats.parsedNodes);
    Log::BioLog::log(msg::INFO_NEO4J_PARSED_EDGE_COUNT() << stats.parsedEdges);

    handler.join();
    return true;
}

bool Neo4jImport::importJsonNeo4j(const Path& jsonDir, const std::string& networkName) {
    BioLog::log(msg::INFO_NEO4J_IMPORT_JSON_FILES() << jsonDir.string());
    TimerStat timer {"Neo4j: import"};

    const FileUtils::Path statsFile = jsonDir / "stats.json";
    const FileUtils::Path nodePropertiesFile = jsonDir / "nodeProperties.json";
    const FileUtils::Path edgePropertiesFile = jsonDir / "edgeProperties.json";

    if (!FileUtils::exists(jsonDir)) {
        BioLog::log(msg::ERROR_DIRECTORY_NOT_EXISTS() << jsonDir);
        return false;
    }

    // Initialization of the parser
    JsonParser parser(_db, networkName);
    parser.setReducedOutput(true);
    const JsonParsingStats& stats = parser.getStats();

    if (!parser.parseJsonDir(jsonDir, JsonParser::DirFormat::Neo4j4)) {
        return false;
    }

    Log::BioLog::log(msg::INFO_NEO4J_NODE_COUNT() << stats.nodeCount);
    Log::BioLog::log(msg::INFO_NEO4J_EDGE_COUNT() << stats.edgeCount);

    Log::BioLog::log(msg::INFO_NEO4J_NODE_PROP_ERROR_COUNT()
                     << stats.nodePropErrors);
    Log::BioLog::log(msg::INFO_NEO4J_EDGE_PROP_ERROR_COUNT()
                     << stats.edgePropErrors);

    Log::BioLog::log(msg::INFO_NEO4J_NODE_PROP_WARNING_COUNT()
                     << stats.nodePropWarnings);
    Log::BioLog::log(msg::INFO_NEO4J_EDGE_PROP_WARNING_COUNT()
                     << stats.edgePropWarnings);

    Log::BioLog::log(msg::INFO_NEO4J_UNSUPPORTED_NODE_PROP_COUNT()
                     << stats.unsupportedNodeProps);
    Log::BioLog::log(msg::INFO_NEO4J_UNSUPPORTED_EDGE_PROP_COUNT()
                     << stats.unsupportedEdgeProps);

    Log::BioLog::log(msg::INFO_NEO4J_ILLFORMED_NODE_PROP_COUNT()
                     << stats.illformedNodeProps);
    Log::BioLog::log(msg::INFO_NEO4J_ILLFORMED_EDGE_PROP_COUNT()
                     << stats.illformedEdgeProps);

    Log::BioLog::log(msg::INFO_NEO4J_PARSED_NODE_COUNT() << stats.parsedNodes);
    Log::BioLog::log(msg::INFO_NEO4J_PARSED_EDGE_COUNT() << stats.parsedEdges);

    return true;
}
