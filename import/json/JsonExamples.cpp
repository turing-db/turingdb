#include "JsonExamples.h"
#include "BioLog.h"
#include "DB.h"
#include "FileUtils.h"
#include "JsonParser.h"
#include "MsgCommon.h"

#include <iostream>
#include <regex>

db::DB* cyberSecurityDB() {
    JsonParser parser {};
    parser.setReducedOutput(true);

    std::string turingHome = std::getenv("TURING_HOME");
    const FileUtils::Path jsonDir = FileUtils::Path {turingHome}  /
                                    "neo4j" / "cyber-security-db";

    if (!FileUtils::exists(jsonDir)) {
        Log::BioLog::log(msg::ERROR_DIRECTORY_NOT_EXISTS() << jsonDir);
        std::cout << std::flush;
        return nullptr;
    }

    if (!parser.parseJsonDir(jsonDir, JsonParser::DirFormat::Neo4j4)) {
        return nullptr;
    }

    return parser.getDB();
}

db::DB* recommendationsDB() {
    JsonParser parser {};
    parser.setReducedOutput(true);

    std::string turingHome = std::getenv("TURING_HOME");
    const FileUtils::Path jsonDir = FileUtils::Path {turingHome} /
                                    "neo4j" / "recommendations-db";

    if (!FileUtils::exists(jsonDir)) {
        Log::BioLog::log(msg::ERROR_DIRECTORY_NOT_EXISTS() << jsonDir);
        return nullptr;
    }

    if (!parser.parseJsonDir(jsonDir, JsonParser::DirFormat::Neo4j4)) {
        return nullptr;
    }

    return parser.getDB();
}
