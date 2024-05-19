#include "JsonParser.h"

#include <regex>
#include <spdlog/spdlog.h>

#include "DB.h"
#include "Neo4j4JsonParser.h"

JsonParser::JsonParser(db::DB* db, const std::string& networkName)
    : _db(db),
      _networkName(networkName),
      _neo4j4Parser(_db, _stats)
{
}

bool JsonParser::parseJsonDir(const FileUtils::Path& jsonDir, DirFormat format) {
    try {
        switch (format) {
            case DirFormat::Neo4j4:
                return parseNeo4jJsonDir(jsonDir);
        }

    } catch (const std::exception& e) {
        spdlog::error("Failed to parse json {}", e.what());
    }

    return false;
}

bool JsonParser::parse(const std::string& data, FileFormat format) {
    try {
        switch (format) {
            case FileFormat::Neo4j4_Stats:
                return _neo4j4Parser.parseStats(data);

            case FileFormat::Neo4j4_NodeProperties:
                return _neo4j4Parser.parseNodeProperties(data);

            case FileFormat::Neo4j4_Nodes:
                spdlog::info("Parsing nodes {}/{}", _stats.parsedNodes, _stats.nodeCount);
                return _neo4j4Parser.parseNodes(data, _networkName);

            case FileFormat::Neo4j4_EdgeProperties:
                return _neo4j4Parser.parseEdgeProperties(data);

            case FileFormat::Neo4j4_Edges:
                spdlog::info("Parsing edges {}/{}", _stats.parsedEdges, _stats.edgeCount);
                return _neo4j4Parser.parseEdges(data);
        }

    } catch (const std::exception& e) {
        spdlog::error("Failed to parse json {}", e.what());
    }

    return false;
}

db::DB* JsonParser::getDB() const {
    return _db;
}

bool JsonParser::parseNeo4jJsonDir(const FileUtils::Path& jsonDir) {
    const FileUtils::Path statsFile = jsonDir / "stats.json";
    const FileUtils::Path nodePropertiesFile = jsonDir / "nodeProperties.json";
    const FileUtils::Path edgePropertiesFile = jsonDir / "edgeProperties.json";
    std::string data;

    FileUtils::readContent(statsFile, data);

    if (!parse(data, JsonParser::FileFormat::Neo4j4_Stats)) {
        return false;
    }

    // Parsing node properties data
    FileUtils::readContent(nodePropertiesFile, data);

    if (!parse(data, JsonParser::FileFormat::Neo4j4_NodeProperties)) {
        return false;
    }

    // Nodes
    std::vector<FileUtils::Path> nodeFiles;
    FileUtils::listFiles(jsonDir, nodeFiles);
    std::regex nodeRegex {"nodes_([0-9]*).json"};
    std::map<size_t, FileUtils::Path> nodeFilesOrdered;

    for (const FileUtils::Path& path : nodeFiles) {
        if (std::regex_search(path.string(), nodeRegex)) {
            size_t count = std::stoul(std::regex_replace(
                path.filename().string(),
                nodeRegex, "$1"));
            nodeFilesOrdered[count] = path;
        }
    }

    for (const auto& [id, path] : nodeFilesOrdered) {
        FileUtils::readContent(path, data);

        if (!parse(data, JsonParser::FileFormat::Neo4j4_Nodes)) {
            return false;
        }
    }

    // Parsing edge properties data
    FileUtils::readContent(edgePropertiesFile, data);

    if (!parse(data, JsonParser::FileFormat::Neo4j4_EdgeProperties)) {
        return false;
    }

    // Edges
    std::vector<FileUtils::Path> edgeFiles;
    FileUtils::listFiles(jsonDir, edgeFiles);
    std::regex edgeRegex {"edges_([0-9]*).json"};
    std::map<size_t, FileUtils::Path> edgeFilesOrdered;

    for (const FileUtils::Path& path : edgeFiles) {
        if (std::regex_search(path.string(), edgeRegex)) {
            size_t count = std::stoul(std::regex_replace(
                path.filename().string(),
                edgeRegex, "$1"));
            edgeFilesOrdered[count] = path;
        }
    }

    for (const auto& [id, path] : edgeFilesOrdered) {
        FileUtils::readContent(path, data);

        if (!parse(data, JsonParser::FileFormat::Neo4j4_Edges)) {
            return false;
        }
    }

    return true;
}
