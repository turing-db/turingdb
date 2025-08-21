#include "SystemConfig.h"

#include <spdlog/spdlog.h>

#include "Panic.h"

using namespace db;

SystemConfig::SystemConfig(const fs::Path& turingDir)
    : _turingDir(turingDir)
{
}

SystemConfig::~SystemConfig() {
}

void SystemConfig::init() {
    // Create turingDir if does not exist
    if (!_turingDir.exists()) {
       const auto res = _turingDir.mkdir();
       if (!res) {
           spdlog::error(res.error().fmtMessage());
           panic("Could not create turing directory");
       }
    }

    // Create graphsDir
    _graphsDir = _turingDir/"graphs";
    if (!_graphsDir.exists()) {
        const auto res = _graphsDir.mkdir();
        if (!res) {
            spdlog::error(res.error().fmtMessage());
            panic("Could not create turing graphs directory");
        }
    }

    // Create dataDir
    _dataDir = _turingDir/"data";
    if (!_dataDir.exists()) {
        const auto res = _dataDir.mkdir();
        if (!res) {
            spdlog::error(res.error().fmtMessage());
            panic("Could not create turing data directory");
        }
    }
}
