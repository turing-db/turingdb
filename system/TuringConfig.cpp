#include "TuringConfig.h"

#include <stdlib.h>

#include <spdlog/spdlog.h>

#include "Panic.h"

using namespace db;

TuringConfig::TuringConfig() {
}

TuringConfig::~TuringConfig() {
}

void TuringConfig::init() {
    // Create turing directory if it does not exist
    if (_turingDir.get().empty()) {
        const char* homeEnv = getenv("HOME");
        if (!homeEnv || !*homeEnv) {
            panic("Environment variable $HOME undefined");
        }

        _turingDir = fs::Path(homeEnv) / ".turing";
    }

    if (!_turingDir.exists()) {
        spdlog::info("Creating turing directory in {}", _turingDir.c_str());
        if (auto res = _turingDir.mkdir(); !res) {
            panic("Can not create turing directory: {}", res.error().fmtMessage());
        }
    }

    if (_graphsDir.get().empty()) {
        _graphsDir = _turingDir / "graphs";
    }

    if (!_graphsDir.exists()) {
        spdlog::info("Create directory {}", _graphsDir.c_str());
        if (auto res = _graphsDir.mkdir(); !res) {
            panic("Can not create directory {} {}", _graphsDir.c_str(), res.error().fmtMessage());
        }
    }

    if (_dataDir.get().empty()) {
        _dataDir = _turingDir / "data";
    }

    // Create turing/data directory if it does not exist
    if (!_dataDir.exists()) {
        spdlog::info("Create directory {}", _dataDir.c_str());
        if (auto res = _dataDir.mkdir(); !res) {
            panic("Can not create directory {} {}", _dataDir.c_str(), res.error().fmtMessage());
        }
    }
}

void TuringConfig::setTuringDirectory(const fs::Path& turingDir) {
    _turingDir = turingDir;
    _graphsDir = _turingDir / "graphs";
    _dataDir = _turingDir / "data";
}
