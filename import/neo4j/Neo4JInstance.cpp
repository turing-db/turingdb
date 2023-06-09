#include "Neo4JInstance.h"
#include "BioLog.h"
#include "FileUtils.h"
#include "MsgCommon.h"
#include "MsgImport.h"

#include <boost/process.hpp>
#include <chrono>
#include <filesystem>
#include <stdlib.h>
#include <thread>

#define NEO4J_BASE_DIR "/turing/neo4j"
#define NEO4J_DIR "/turing/neo4j/neo4j"
#define NEO4J_ARCHIVE_PATH "/turing/neo4j/neo4j-4.3.23.tar.gz"

using namespace Log;

Neo4JInstance::Neo4JInstance()
    : _neo4jDir(NEO4J_DIR),
      _neo4jArchive(NEO4J_ARCHIVE_PATH),
      _neo4jBinary(_neo4jDir / "bin" / "neo4j"),
      _neo4jAdminBinary(_neo4jDir / "bin" / "neo4j-admin")
{
}

Neo4JInstance::~Neo4JInstance() {
}

bool Neo4JInstance::setup() {
    // Check that neo4j archive exists in installation
    if (!FileUtils::exists(_neo4jArchive)) {
        BioLog::log(msg::ERROR_NEO4J_CANNOT_FIND_ARCHIVE()
                    << _neo4jArchive.string());
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
    BioLog::log(msg::INFO_NEO4J_DECOMPRESS_ARCHIVE() << _neo4jDir.string());
    const int tarRes =
        boost::process::system("/usr/bin/tar", "xf", _neo4jArchive.string(),
                               "-C", NEO4J_DIR, "--strip-components=1");

    if (tarRes != 0) {
        BioLog::log(msg::ERROR_NEO4J_CANNOT_DECOMPRESS_ARCHIVE()
                    << _neo4jArchive.string());
        return false;
    }

    return true;
}

bool Neo4JInstance::stop() {
    if (FileUtils::exists(_neo4jDir)) {
        BioLog::log(msg::INFO_NEO4J_STOPPING());
        const int stopRes =
            boost::process::system(_neo4jBinary.string(), "stop");
        if (stopRes != 0) {
            BioLog::log(msg::ERROR_NEO4J_FAILED_TO_STOP());
            return false;
        }
    }

    return true;
}

void Neo4JInstance::destroy() {
    if (FileUtils::exists(_neo4jDir)) {
        stop();

        // Remove neo4j directory
        BioLog::log(msg::INFO_NEO4J_CLEAN_SETUP());
        FileUtils::removeDirectory(_neo4jDir);
    }
}

bool Neo4JInstance::isReady() const {
    const bool res =
        !std::system("curl --request GET --url 127.0.0.1:7474 -s > /dev/null");

    return res;
}

bool Neo4JInstance::start() {
    if (isReady()) {
        BioLog::log(msg::ERROR_NEO4J_ALREADY_RUNNING());
        return false;
    }

    // Setting initial password. This is required by Neo4j
    boost::process::system(_neo4jAdminBinary.c_str(), "set-initial-password",
                           "turing");

    // Start daemon
    BioLog::log(msg::INFO_NEO4J_STARTING() << _neo4jBinary.string());

    if (!FileUtils::exists(_neo4jBinary)) {
        BioLog::log(msg::ERROR_FILE_NOT_EXISTS() << _neo4jBinary.string());
        return false;
    }

    const int startRes = boost::process::system(_neo4jBinary.string(), "start");
    if (startRes != 0) {
        BioLog::log(msg::ERROR_NEO4J_FAILED_TO_START());
        return false;
    }

    // Wait for warmup
    BioLog::log(msg::INFO_NEO4J_WARMING_UP());
    while (!isReady()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    BioLog::log(msg::INFO_NEO4J_READY());

    return true;
}

bool Neo4JInstance::changePassword(const std::string& oldPassword,
                                   const std::string& newPassword) {
    std::string curlString = "curl -H \"Content-Type: application/json\" -X "
                             "POST -d '{\"password\":\"";
    curlString += newPassword;
    curlString +=
        "\"}' -u neo4j:neo4j http://localhost:7474/user/neo4j/password";
    const int res = std::system(curlString.c_str());
    if (res != 0) {
        return false;
    }

    return true;
}

bool Neo4JInstance::importDumpedDB(
    const std::filesystem::path& dbFilePath) const {
    if (!FileUtils::exists(dbFilePath)) {
        BioLog::log(msg::ERROR_FILE_NOT_EXISTS() << dbFilePath.string());
        return false;
    }

    const std::string dbName = dbFilePath.stem();

    boost::process::system(_neo4jAdminBinary.string(), "load",
                           "--database=neo4j", "--from=" + dbFilePath.string(),
                           "--force");

    return true;
}

bool Neo4JInstance::importDBDir(const std::string& dbPath) {
    if (dbPath.empty()) {
        return false;
    }

    // Get neo4j databases directory
    const auto neo4jDBDir = _neo4jDir / "data" / "databases";
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
    const auto pathInDBDir = neo4jDBDir / dbName;
    if (FileUtils::exists(pathInDBDir)) {
        FileUtils::removeDirectory(pathInDBDir);
    }

    // Copy db to database directory
    BioLog::log(msg::INFO_COPY_DB() << dbPath << pathInDBDir.string());
    if (!FileUtils::copy(dbPath, pathInDBDir)) {
        BioLog::log(msg::ERROR_FAILED_TO_COPY()
                    << dbPath << pathInDBDir.string());
        return false;
    }

    return true;
}
