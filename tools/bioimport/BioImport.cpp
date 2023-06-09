#include "BioLog.h"
#include "FileUtils.h"
#include "JsonParser.h"
#include "MsgCommon.h"
#include "MsgImport.h"
#include "Neo4JHttpRequest.h"
#include "Neo4JInstance.h"
#include "PerfStat.h"
#include "TimerStat.h"
#include "ToolInit.h"

#include <optional>
#include <regex>
#include <thread>
#include <vector>

#define BIOIMPORT_TOOL_NAME "bioimport"

using namespace Log;
using namespace std::literals;

class ThreadHandler {
public:
    struct Requests {
        Neo4JHttpRequest stats{
            "CALL apoc.meta.stats() "
            "YIELD nodeCount, relCount RETURN *",
        };

        Neo4JHttpRequest nodeProperties{
            "CALL apoc.meta.nodeTypeProperties()",
        };

        std::vector<Neo4JHttpRequest> nodes;

        Neo4JHttpRequest edgeProperties{
            "CALL apoc.meta.relTypeProperties();",
        };

        std::vector<Neo4JHttpRequest> edges;
    } requests;

    bool start(JsonParser& parser) {
        // Retrieving the node and edge counts
        requests.stats.exec();
        if (!requests.stats.success()) {
            requests.stats.reportError();
            return false;
        }

        if (!parser.parse(requests.stats.getData(),
                          JsonParser::Format::Neo4j4_Stats)) {
            return false;
        }

        const auto& stats = parser.getStats();
        uint64_t nodeRequested = 0;

        // Preparing node requests
        while (nodeRequested < stats.nodeCount) {
            requests.nodes.emplace_back(Neo4JHttpRequest::RequestProps{
                .statement = "MATCH(n) "
                             "RETURN labels(n), ID(n), properties(n) SKIP " +
                             std::to_string(nodeRequested) + " LIMIT " +
                             std::to_string(_nodeCountLimit) + ";",
                .silent = true,
            });

            nodeRequested += _nodeCountLimit;
        }

        uint64_t edgeRequested = 0;

        // Preparing node requests
        while (edgeRequested < stats.edgeCount) {
            requests.edges.emplace_back(Neo4JHttpRequest::RequestProps{
                .statement =
                    "MATCH (n1)-[e]->(n2) "
                    "RETURN type(e), ID(n1), ID(n2), properties(e) SKIP " +
                    std::to_string(edgeRequested) + " LIMIT " +
                    std::to_string(_edgeCountLimit) + ";",
                .silent = true,
            });

            edgeRequested += _edgeCountLimit;
        }

        _thread = std::thread(&ThreadHandler::exec, this);
        return true;
    }

    void join() { _thread.join(); }

private:
    std::thread _thread;
    static constexpr size_t _nodeCountLimit = 1000000;
    static constexpr size_t _edgeCountLimit = 3000000;

    void exec() {
        requests.nodeProperties.exec();
        if (!requests.nodeProperties.success()) {
            return;
        }

        for (auto& nodeRequest : requests.nodes) {
            nodeRequest.exec();
            if (!nodeRequest.success()) {
                return;
            }
        }

        requests.edgeProperties.exec();
        if (!requests.edgeProperties.success()) {
            return;
        }

        for (auto& edgeRequest : requests.edges) {
            edgeRequest.exec();
            if (!edgeRequest.success()) {
                return;
            }
        }
    }
};

bool importNeo4j(const ToolInit& tool, const FileUtils::Path& filepath) {
    BioLog::log(msg::INFO_NEO4J_IMPORT_DUMP_FILE() << filepath.string());
    TimerStat timer{"Neo4j: import"};
    Neo4JInstance instance;

    const FileUtils::Path& outDir = tool.getOutputsDir();
    const FileUtils::Path jsonDir = outDir / "json";
    const FileUtils::Path statsFile = jsonDir / "stats.json";
    const FileUtils::Path nodePropertiesFile = jsonDir / "nodeProperties.json";
    const FileUtils::Path edgePropertiesFile = jsonDir / "edgeProperties.json";

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
    JsonParser parser{};
    parser.setReducedOutput(true);
    const JsonParsingStats& stats = parser.getStats();

    // Preparing the requests
    ThreadHandler handler{};

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
    while (!handler.requests.nodeProperties.finished()) {
        std::this_thread::sleep_for(100ms);
    }

    if (!handler.requests.nodeProperties.success()) {
        instance.destroy();
        handler.requests.nodeProperties.reportError();
        return false;
    }
    handler.requests.nodeProperties.writeToFile(nodePropertiesFile);

    // Parsing node properties data
    if (!parser.parse(handler.requests.nodeProperties.getData(),
                      JsonParser::Format::Neo4j4_NodeProperties)) {
        instance.destroy();
        return false;
    }

    // Done, clearing node properties data
    handler.requests.nodeProperties.clear();

    // Nodes
    size_t i = 1;

    for (auto& nodeRequest : handler.requests.nodes) {
        // Waiting for the nodes request to finish
        while (!nodeRequest.finished()) {
            std::this_thread::sleep_for(100ms);
        }

        if (!nodeRequest.success()) {
            instance.destroy();
            return false;
        }

        // Parsing the nodes data
        if (!parser.parse(nodeRequest.getData(),
                          JsonParser::Format::Neo4j4_Nodes)) {
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
    while (!handler.requests.edgeProperties.finished()) {
        std::this_thread::sleep_for(100ms);
    }

    if (!handler.requests.edgeProperties.success()) {
        instance.destroy();
        handler.requests.edgeProperties.reportError();
        return false;
    }
    handler.requests.edgeProperties.writeToFile(edgePropertiesFile);

    // Parsing edge properties data
    if (!parser.parse(handler.requests.edgeProperties.getData(),
                      JsonParser::Format::Neo4j4_EdgeProperties)) {
        instance.destroy();
        return false;
    }

    // Done, clearing edge properties data
    handler.requests.edgeProperties.clear();

    // Nodes
    i = 1;

    for (auto& edgeRequest : handler.requests.edges) {
        // Waiting for the edges request to finish
        while (!edgeRequest.finished()) {
            std::this_thread::sleep_for(100ms);
        }

        if (!edgeRequest.success()) {
            instance.destroy();
            return false;
        }

        // Parsing the edges data
        if (!parser.parse(edgeRequest.getData(),
                          JsonParser::Format::Neo4j4_Edges)) {
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

bool importJsonNeo4j(const ToolInit& tool, const FileUtils::Path& jsonDir) {
    BioLog::log(msg::INFO_NEO4J_IMPORT_JSON_FILES() << jsonDir.string());
    TimerStat timer{"Neo4j: import"};

    const FileUtils::Path statsFile = jsonDir / "stats.json";
    const FileUtils::Path nodePropertiesFile = jsonDir / "nodeProperties.json";
    const FileUtils::Path edgePropertiesFile = jsonDir / "edgeProperties.json";

    if (!FileUtils::exists(jsonDir)) {
        BioLog::log(msg::ERROR_DIRECTORY_NOT_EXISTS() << jsonDir);
        return false;
    }

    // Initialization of the parser
    JsonParser parser{};
    parser.setReducedOutput(true);
    const JsonParsingStats& stats = parser.getStats();
    std::string data;

    FileUtils::readContent(statsFile, data);

    if (!parser.parse(data, JsonParser::Format::Neo4j4_Stats)) {
        return false;
    }

    Log::BioLog::log(msg::INFO_NEO4J_NODE_COUNT() << stats.nodeCount);
    Log::BioLog::log(msg::INFO_NEO4J_EDGE_COUNT() << stats.edgeCount);

    // Parsing node properties data
    FileUtils::readContent(nodePropertiesFile, data);

    if (!parser.parse(data, JsonParser::Format::Neo4j4_NodeProperties)) {
        return false;
    }

    // Nodes
    std::vector<FileUtils::Path> nodeFiles;
    FileUtils::listFiles(jsonDir, nodeFiles);
    std::regex nodeRegex{"nodes_[0-9]*.json"};

    for (const FileUtils::Path& path : nodeFiles) {
        if (std::regex_search(path.string(), nodeRegex)) {
            FileUtils::readContent(path, data);

            if (!parser.parse(data, JsonParser::Format::Neo4j4_Nodes)) {
                return false;
            }
        }
    }

    // Parsing edge properties data
    FileUtils::readContent(edgePropertiesFile, data);

    if (!parser.parse(data, JsonParser::Format::Neo4j4_EdgeProperties)) {
        return false;
    }

    // Edges
    std::vector<FileUtils::Path> edgeFiles;
    FileUtils::listFiles(jsonDir, edgeFiles);
    std::regex edgeRegex{"edges_[0-9]*.json"};

    for (const FileUtils::Path& path : edgeFiles) {
        if (std::regex_search(path.string(), edgeRegex)) {
            FileUtils::readContent(path, data);

            if (!parser.parse(data, JsonParser::Format::Neo4j4_Edges)) {
                return false;
            }
        }
    }

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

int main(int argc, const char** argv) {
    ToolInit toolInit(BIOIMPORT_TOOL_NAME);

    ArgParser& argParser = toolInit.getArgParser();
    argParser.addOption("neo4j", "imports a neo4j.dump file", true);
    argParser.addOption("jsonNeo4j",
                        "imports json files from a json/ directory (built "
                        "during a previous neo4j.dump import)",
                        true);

    toolInit.init(argc, argv);

    std::optional<std::string> neo4jFile;
    std::optional<std::string> jsonNeo4jDir;

    for (const auto& option : argParser.options()) {
        const auto& optName = option.first;
        if (optName == "neo4j") {
            neo4jFile = option.second;
        }
        if (optName == "jsonNeo4j") {
            jsonNeo4jDir = option.second;
        }
    }

    if (neo4jFile) {
        importNeo4j(toolInit, neo4jFile.value());
    }

    if (jsonNeo4jDir) {
        importJsonNeo4j(toolInit, jsonNeo4jDir.value());
    }

    BioLog::printSummary();
    BioLog::destroy();
    PerfStat::destroy();

    return EXIT_SUCCESS;
}
