#include "Neo4JInstance.h"

#include <stdlib.h>
#include <filesystem>
#include <boost/process.hpp>
#include <thread>
#include <chrono>

#include "FileUtils.h"

#include "BioLog.h"
#include "MsgImport.h"
#include "MsgCommon.h"

#define NEO4J_FILE  "neo4j-3.5.30.tar.gz"

using namespace Log;

Neo4JInstance::Neo4JInstance(const std::string& path)
    : _neo4jDir(std::filesystem::path(path)/"neo4j")
{
}

Neo4JInstance::~Neo4JInstance() {
}

bool Neo4JInstance::setup() {
    const char* bioHome = getenv("BIO_HOME");
    if (!bioHome) {
        return false;
    }

    // Check that neo4j archive exists in installation
    const auto neo4jArchivePath = std::filesystem::path(bioHome)/"share"/NEO4J_FILE;
    if (!FileUtils::exists(neo4jArchivePath)) {
        BioLog::log(msg::ERROR_IMPORT_FAILED_FIND_NEO4J_ARCHIVE() << neo4jArchivePath.string());
        return false;
    }

    // Create neo4j directory
    if (FileUtils::exists(_neo4jDir)) {
        FileUtils::removeDirectory(_neo4jDir);
    }

    if (!FileUtils::createDirectory(_neo4jDir)) {
        BioLog::log(msg::ERROR_FAILED_TO_CREATE_DIRECTORY() << _neo4jDir);
        return false;
    }

    // Decompress neo4j archive
    BioLog::log(msg::INFO_DECOMPRESS_NEO4J_ARCHIVE() << _neo4jDir.string());
    const int tarRes = boost::process::system("/usr/bin/tar",
                                              "zxf",
                                              neo4jArchivePath.string(),
                                              "-C",
                                              _neo4jDir.string(),
                                              "--strip-components=1");
    if (tarRes != 0) {
        BioLog::log(msg::ERROR_IMPORT_FAILED_DECOMPRESS_NEO4J_ARCHIVE() << neo4jArchivePath.string());
        return false;
    }

    return true;
}

bool Neo4JInstance::stop() {
    if (FileUtils::exists(_neo4jDir)) {
        BioLog::log(msg::INFO_STOPPING_NEO4J());
        const int stopRes = boost::process::system(getNeo4jBinary().string(), "stop");
        if (stopRes != 0) {
            BioLog::log(msg::ERROR_FAILED_TO_STOP_NEO4J());
            return false;
        }
    }

    return true;
}

void Neo4JInstance::destroy() {
    if (FileUtils::exists(_neo4jDir)) {
        stop();

        // Remove neo4j directory
        BioLog::log(msg::INFO_CLEAN_NEO4J_SETUP());
        FileUtils::removeDirectory(_neo4jDir);
    }
}

std::filesystem::path Neo4JInstance::getNeo4jBinary() const {
    return _neo4jDir/"bin"/"neo4j";
}

bool Neo4JInstance::start() {
    // Start daemon
    BioLog::log(msg::INFO_STARTING_NEO4J() << _neo4jDir.string());
    const int startRes = boost::process::system(getNeo4jBinary().string(), "start");
    if (startRes != 0) {
        BioLog::log(msg::ERROR_FAILED_TO_START_NEO4J());
        return false;
    }

    // Wait for warmup
    const unsigned waitTime = 90;
    BioLog::log(msg::INFO_NEO4J_WAIT_WARMUP() << std::to_string(waitTime));
    std::this_thread::sleep_for(std::chrono::seconds(waitTime));

    return true;
}

bool Neo4JInstance::changePassword(const std::string& oldPassword,
                                   const std::string& newPassword) {
    std::string curlString = "curl -H \"Content-Type: application/json\" -X POST -d '{\"password\":\"";
    curlString += newPassword;
    curlString += "\"}' -u neo4j:neo4j http://localhost:7474/user/neo4j/password";
    const int res = std::system(curlString.c_str());
    if (res != 0) {
        return false;
    }

    return true;
}

bool Neo4JInstance::importDBDir(const std::string& dbPath) {
    if (dbPath.empty()) {
        return false;
    }
    
    // Get neo4j databases directory
    const auto neo4jDBDir = _neo4jDir/"data"/"databases";
    if (!FileUtils::exists(neo4jDBDir)) {
        BioLog::log(msg::ERROR_DIRECTORY_NOT_EXISTS() << neo4jDBDir.string());
        return false;
    }

    // Get db name
    const std::string dbName = std::filesystem::path(dbPath).filename();
    if (dbName.empty()) {
        BioLog::log(msg::ERROR_EMPTY_DB_NAME() << dbPath);
        return false;
    }

    // Remove a db with the same name if it already exists
    const auto pathInDBDir = neo4jDBDir/dbName;
    if (FileUtils::exists(pathInDBDir)) {
        FileUtils::removeDirectory(pathInDBDir);
    }

    // Copy db to database directory
    BioLog::log(msg::INFO_COPY_DB() << dbPath << pathInDBDir.string());
    if (!FileUtils::copy(dbPath, pathInDBDir)) {
        BioLog::log(msg::ERROR_FAILED_TO_COPY() << dbPath << pathInDBDir.string());
        return false;
    }

    return true;
}
