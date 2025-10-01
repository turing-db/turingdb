#include "TuringConfig.h"
#include "Panic.h"

#include <stdlib.h>

#include <spdlog/spdlog.h>

using namespace db;

TuringConfig::TuringConfig()
{
}

TuringConfig::~TuringConfig() {
}

TuringConfig::TuringConfig(const TuringConfig&) = default;

TuringConfig::TuringConfig(TuringConfig&&) noexcept = default;

TuringConfig& TuringConfig::operator=(const TuringConfig&) = default;

TuringConfig& TuringConfig::operator=(TuringConfig&&) noexcept = default;

void TuringConfig::setTuringDirectory(const fs::Path& turingDir) {
    _turingDir = turingDir;
    _graphsDir = _turingDir / "graphs";
    _dataDir = _turingDir / "data";
}

TuringConfig TuringConfig::createDefault() {
    TuringConfig config;

    // Create turing directory if it does not exist
    const char* homeEnv = getenv("HOME");
    if (!homeEnv || !*homeEnv) {
        panic("Environment variable $HOME undefined");
    }

    config._turingDir = fs::Path(homeEnv) / ".turing";
    config._graphsDir = config._turingDir / "graphs";
    config._dataDir = config._turingDir / "data";

    return config;
}
