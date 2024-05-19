#include "JsonExamples.h"

#include <iostream>
#include <regex>

#include "DB.h"
#include "FileUtils.h"
#include "JsonParser.h"
#include "LogUtils.h"

db::DB* getNeo4j4DB(const std::string& dbName) {
    db::DB* db = db::DB::create();
    JsonParser parser(db, dbName);
    parser.setReducedOutput(true);

    std::string turingHome = std::getenv("TURING_HOME");
    const FileUtils::Path jsonDir = FileUtils::Path {turingHome} / "neo4j" / dbName;

    if (!FileUtils::exists(jsonDir)) {
        logt::DirectoryDoesNotExist(jsonDir.string());
        std::cout << std::flush;
        return nullptr;
    }

    if (!parser.parseJsonDir(jsonDir, JsonParser::DirFormat::Neo4j4)) {
        return nullptr;
    }

    return db;
}

db::DB* getMultiNetNeo4j4DB(std::initializer_list<std::string> dbNames) {
    db::DB* db = db::DB::create();

    for (const auto& dbName : dbNames) {
        JsonParser parser(db, dbName);
        parser.setReducedOutput(true);

        std::string turingHome = std::getenv("TURING_HOME");
        const FileUtils::Path jsonDir = FileUtils::Path {turingHome} / "neo4j" / dbName;

        if (!FileUtils::exists(jsonDir)) {
            logt::DirectoryDoesNotExist(jsonDir.string());
            std::cout << std::flush;
            return nullptr;
        }

        if (!parser.parseJsonDir(jsonDir, JsonParser::DirFormat::Neo4j4)) {
            return nullptr;
        }
    }

    return db;
}

db::DB* cyberSecurityDB() {
    return getNeo4j4DB("cyber-security-db");
}

db::DB* networkDB() {
    return getNeo4j4DB("network-db");
}

db::DB* poleDB() {
    return getNeo4j4DB("pole-db");
}

db::DB* stackoverflowDB() {
    return getNeo4j4DB("stackoverflow-db");
}

db::DB* recommendationsDB() {
    return getNeo4j4DB("recommendations-db");
}
