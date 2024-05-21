#include "Neo4JInstance.h"

#include <boost/process.hpp>
#include <chrono>
#include <filesystem>
#include <stdlib.h>
#include <thread>
#include <spdlog/spdlog.h>

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
}

bool Neo4jInstance::setup() {
    // Check that neo4j archive exists in installation
    std::string turingHome = std::getenv("TURING_HOME");
    if (turingHome.empty()) {
        spdlog::error("Can not find TURING_HOME environment variable");
        return false;
    }

    const FileUtils::Path neo4jArchive =
        FileUtils::Path {turingHome} / "neo4j" / NEO4J_ARCHIVE_NAME;

    if (!FileUtils::exists(neo4jArchive)) {
        spdlog::error("Can not find Neo4J archive {}",
                      neo4jArchive.string());
        return false;
    }

    // Create neo4j directory
    if (FileUtils::exists(_neo4jDir)) {
        FileUtils::removeDirectory(_neo4jDir);
    }

    if (!FileUtils::createDirectory(_neo4jDir)) {
        logt::CanNotCreateDir(_neo4jDir.string());
        return false;
    }

    // Decompress neo4j archive
    spdlog::error("Decompressing Neo4J archive in {}", _neo4jDir.string());
    const int tarRes = boost::process::system(
        "/usr/bin/tar", "xf", neo4jArchive.string(), "-C", _neo4jDir.string(),
        "--strip-components=1");

    if (tarRes != 0) {
        spdlog::error("Can not decompress Neo4J archive {}", neo4jArchive.string());
        return false;
    }

    return true;
}

bool Neo4jInstance::stop() {
    spdlog::info("Neo4J stopping");
    if (FileUtils::exists(_neo4jDir)) {
        const int stopRes =
            boost::process::system(_neo4jBinary.string(), "stop");
        if (stopRes != 0) {
            spdlog::error("Failed to stop Neo4J");
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
        spdlog::info("Cleaning Neo4J directory");
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
        spdlog::error("Neo4J is already running");
        return false;
    }

    // Setting initial password. This is required by Neo4j
    boost::process::system(_neo4jAdminBinary.c_str(), "set-initial-password",
                           "turing");

    // Start daemon
    spdlog::info("Starting Neo4J {}", _neo4jBinary.string());

    if (!FileUtils::exists(_neo4jBinary)) {
        logt::FileNotFound(_neo4jBinary.string());
        return false;
    }

    const int startRes = boost::process::system(_neo4jBinary.string(), "start");
    if (startRes != 0) {
        spdlog::error("Neo4J failed to start");
        return false;
    }

    // Wait for warmup
    spdlog::info("Neo4J warming up");
    while (!isRunning()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    spdlog::info("Neo4J is running and ready");

    return true;
}

bool Neo4jInstance::importDumpedDB(
    const std::filesystem::path& dbFilePath) const {
    if (!FileUtils::exists(dbFilePath)) {
        logt::FileNotFound(dbFilePath.string());
        return false;
    }

    const std::string dbName = dbFilePath.stem();

    boost::process::system(_neo4jAdminBinary.string(), "load",
                           "--database=neo4j", "--from=" + dbFilePath.string(),
                           "--force");

    return true;
}
