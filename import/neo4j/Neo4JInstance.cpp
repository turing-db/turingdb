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

#define NEO4J_ARCHIVE_NAME "neo4j-4.3.23.tar.gz"

using namespace Log;

Neo4jInstance::Neo4jInstance(const FileUtils::Path& baseDir)
    : _neo4jDir(baseDir / "neo4j"),
      _neo4jBinary(_neo4jDir / "bin" / "neo4j"),
      _neo4jAdminBinary(_neo4jDir / "bin" / "neo4j-admin")
{
}

Neo4jInstance::~Neo4jInstance() {
}

bool Neo4jInstance::setup() {
    // Check that neo4j archive exists in installation
    std::string turingHome = std::getenv("TURING_HOME");
    if (turingHome.empty()) {
        BioLog::log(msg::ERROR_INCORRECT_ENV_SETUP());
        return false;
    }

    const FileUtils::Path neo4jArchive =
        FileUtils::Path {turingHome} / "neo4j" / NEO4J_ARCHIVE_NAME;

    if (!FileUtils::exists(neo4jArchive)) {
        BioLog::log(msg::ERROR_NEO4J_CANNOT_FIND_ARCHIVE()
                    << neo4jArchive.string());
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
    const int tarRes = boost::process::system(
        "/usr/bin/tar", "xf", neo4jArchive.string(), "-C", _neo4jDir.string(),
        "--strip-components=1");

    if (tarRes != 0) {
        BioLog::log(msg::ERROR_NEO4J_CANNOT_DECOMPRESS_ARCHIVE()
                    << neo4jArchive.string());
        return false;
    }

    return true;
}

bool Neo4jInstance::stop() {
    BioLog::log(msg::INFO_NEO4J_STOPPING());
    if (FileUtils::exists(_neo4jDir)) {
        const int stopRes =
            boost::process::system(_neo4jBinary.string(), "stop");
        if (stopRes != 0) {
            BioLog::log(msg::ERROR_NEO4J_FAILED_TO_STOP());
            killJava();
            return false;
        }
    } else {
        killJava();
    }

    return true;
}

void Neo4jInstance::destroy() {
    if (FileUtils::exists(_neo4jDir)) {
        stop();

        // Remove neo4j directory
        BioLog::log(msg::INFO_NEO4J_CLEAN_SETUP());
        FileUtils::removeDirectory(_neo4jDir);
    }
}

bool Neo4jInstance::isRunning() {
    const bool res =
        !std::system("curl --request GET --url 127.0.0.1:7474 -s > /dev/null");

    return res;
}

void Neo4jInstance::killJava() {
    boost::process::system("pkill java");
}

bool Neo4jInstance::start() {
    if (isRunning()) {
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
    while (!isRunning()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    BioLog::log(msg::INFO_NEO4J_READY());

    return true;
}

bool Neo4jInstance::importDumpedDB(
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
