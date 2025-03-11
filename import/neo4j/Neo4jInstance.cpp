#include "Neo4jInstance.h"

#include <boost/process.hpp>
#include <chrono>
#include <filesystem>
#include <spdlog/spdlog.h>
#include <stdlib.h>
#include <thread>

#include "FileUtils.h"
#include "LogUtils.h"

#define NEO4J_ARCHIVE_NAME "neo4j-4.3.23.tar.gz"

Neo4jInstance::Neo4jInstance(const FileUtils::Path& baseDir)
    : _neo4jDir(baseDir / "neo4j"),
      _neo4jBinary(_neo4jDir / "bin" / "neo4j"),
      _neo4jAdminBinary(_neo4jDir / "bin" / "neo4j-admin")
{
}

Neo4jInstance::~Neo4jInstance() {
    destroy();
}

bool Neo4jInstance::setup() {
    // Check that neo4j archive exists in installation
    std::string turingHome = std::getenv("TURING_HOME");
    if (turingHome.empty()) {
        logt::TuringHomeUndefined();
        return false;
    }

    const FileUtils::Path neo4jArchive =
        FileUtils::Path {turingHome} / "neo4j" / NEO4J_ARCHIVE_NAME;

    if (!FileUtils::exists(neo4jArchive)) {
        spdlog::error("Failed to find Neo4j archive '{}'", neo4jArchive.c_str());
        return false;
    }

    // Create neo4j directory
    if (FileUtils::exists(_neo4jDir)) {
        FileUtils::removeDirectory(_neo4jDir);
    }

    if (!FileUtils::createDirectory(_neo4jDir)) {
        logt::CanNotCreateDir(_neo4jDir);
        return false;
    }

    // Decompress neo4j archive
    spdlog::info("Decompressing Neo4j archive in {}", _neo4jDir.c_str());
    const int tarRes = boost::process::system(
        "/usr/bin/tar", "xf", neo4jArchive.string(), "-C", _neo4jDir.string(),
        "--strip-components=1");

    if (tarRes != 0) {
        spdlog::error("Failed to decompress Neo4j archive '{}'", neo4jArchive.c_str());
        return false;
    }

    return true;
}

bool Neo4jInstance::stop() {
    spdlog::info("Stopping Neo4j");
    if (FileUtils::exists(_neo4jDir)) {
        const int stopRes =
            boost::process::system(_neo4jBinary.string(), "stop");
        if (stopRes != 0) {
            spdlog::error("Failed to stop Neo4j. Killing java process");
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
        spdlog::info("Clean Neo4j setup");
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
        spdlog::info("Neo4j server is already running");
        return false;
    }

    // Setting initial password. This is required by Neo4j
    boost::process::system(_neo4jAdminBinary.c_str(), "set-initial-password",
                           "turing");

    // Start daemon
    spdlog::info("Starting Neo4j from binary {}", _neo4jBinary.c_str());

    if (!FileUtils::exists(_neo4jBinary)) {
        logt::FileNotFound(_neo4jBinary.string());
        return false;
    }

    const int startRes = boost::process::system(_neo4jBinary.string(), "start");
    if (startRes != 0) {
        spdlog::error("Failed to start Neo4j");
        return false;
    }

    // Wait for warmup
    spdlog::info("Waiting for Neo4j to warmup");
    while (!isRunning()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    spdlog::info("Neo4j server is ready");

    return true;
}

bool Neo4jInstance::importDumpedDB(
    const std::filesystem::path& dbFilePath) const {
    if (!FileUtils::exists(dbFilePath)) {
        logt::FileNotFound(dbFilePath);
        return false;
    }

    const std::string dbName = dbFilePath.stem();

    boost::process::system(_neo4jAdminBinary.string(), "load",
                           "--database=neo4j", "--from=" + dbFilePath.string(),
                           "--force");

    return true;
}
