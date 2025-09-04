#include "SystemConfig.h"

#include <stdlib.h>

#include <spdlog/spdlog.h>

#include "Panic.h"

using namespace db;

SystemConfig::SystemConfig()
{
    init();
}

SystemConfig::~SystemConfig() {
}

void SystemConfig::init() {
    const char* homeEnv = getenv("HOME");
    if (!homeEnv || !*homeEnv) {
        panic("Environment variable $HOME undefined");
    }

    // Create turing directory if it does not exist
    const fs::Path turingDir = fs::Path(homeEnv)/".turing";

    if (!turingDir.exists()) {
        spdlog::info("Creating turing directory in {}", turingDir.c_str());
        if (auto res = turingDir.mkdir(); !res) {
            panic("Can not create turing directory: {}", res.error().fmtMessage());
        }
    }

    // Create turing/graphs directory if it does not exist
    const fs::Path graphsDir = turingDir/"graphs";
    if (!graphsDir.exists()) {
        spdlog::info("Create directory {}", graphsDir.c_str());
        if (auto res = graphsDir.mkdir(); !res) {
            panic("Can not create directory {} {}", graphsDir.c_str(), res.error().fmtMessage());
        }
    }

    // Create turing/data directory if it does not exist
    const fs::Path dataDir = turingDir/"data";
    if (!dataDir.exists()) {
        spdlog::info("Create directory {}", dataDir.c_str());
        if (auto res = dataDir.mkdir(); !res) {
            panic("Can not create directory {} {}", dataDir.c_str(), res.error().fmtMessage());
        }
    }

    _turingDir = turingDir;
    _graphsDir = graphsDir;
    _dataDir = dataDir;
}

void SystemConfig::setTuringDirectory(const fs::Path& turingDir) {
    _turingDir = turingDir;
    _graphsDir = _turingDir/"graphs";
    _dataDir = _turingDir/"data";
}
