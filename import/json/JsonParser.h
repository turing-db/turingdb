#pragma once

#include "JsonParsingStats.h"
#include "Neo4j4JsonParser.h"
#include "FileUtils.h"

class JsonParser {
public:
    enum class FileFormat {
        Neo4j4_NodeProperties = 0,
        Neo4j4_Nodes,
        Neo4j4_EdgeProperties,
        Neo4j4_Edges,
        Neo4j4_Stats,
    };

    enum class DirFormat {
        Neo4j4 = 0,
    };

    JsonParser();
    JsonParser(db::DB* db);

    bool parseJsonDir(const FileUtils::Path& jsonDir, DirFormat format);
    bool parse(const std::string& data, FileFormat format);
    db::DB* getDB() const;

    const JsonParsingStats& getStats() const { return _stats; };

    void setReducedOutput(bool value) { _neo4j4Parser.setReducedOutput(value); }

private:
    db::DB* _db {nullptr};
    JsonParsingStats _stats;
    Neo4j4JsonParser _neo4j4Parser;
    bool _reducedOutput = false;

    bool parseNeo4jJsonDir(const FileUtils::Path& jsonDir);
};
