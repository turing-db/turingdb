#include "BioLog.h"
#include "FileUtils.h"
#include "JsonParser.h"
#include "JsonParsingStats.h"
#include "MsgCommon.h"
#include "MsgImport.h"
#include "Neo4JInstance.h"
#include "PerfStat.h"
#include "TimerStat.h"
#include "ToolInit.h"
#include "tools/bioimport/ThreadHandler.h"

#include <regex>

#define BIOIMPORT_TOOL_NAME "bioimport"

using namespace Log;
using namespace std::literals;

bool importNeo4j(const ToolInit& tool, const FileUtils::Path& filepath) {
    BioLog::log(msg::INFO_NEO4J_IMPORT_DUMP_FILE() << filepath.string());
    TimerStat timer{"Neo4j: import"};

    const FileUtils::Path& outDir = tool.getOutputsDir();
    const FileUtils::Path jsonDir = outDir / "json";
    const FileUtils::Path statsFile = jsonDir / "stats.json";
    const FileUtils::Path nodePropertiesFile = jsonDir / "nodeProperties.json";
    const FileUtils::Path edgePropertiesFile = jsonDir / "edgeProperties.json";

    Neo4JInstance instance{outDir};
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
    handler.requests.nodeProperties.waitReady();

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
        nodeRequest.waitReady();

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
    handler.requests.edgeProperties.waitReady();

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
        edgeRequest.waitReady();

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

    std::string neo4jFile;
    std::string jsonNeo4jDir;

    for (const auto& option : argParser.options()) {
        const auto& optName = option.first;
        if (optName == "neo4j") {
            neo4jFile = option.second;
        }
        if (optName == "jsonNeo4j") {
            jsonNeo4jDir = option.second;
        }
    }

    if (!neo4jFile.empty()) {
        importNeo4j(toolInit, neo4jFile);
    }

    if (!jsonNeo4jDir.empty()) {
        importJsonNeo4j(toolInit, jsonNeo4jDir);
    }

    BioLog::printSummary();
    BioLog::destroy();
    PerfStat::destroy();

    return EXIT_SUCCESS;
}
