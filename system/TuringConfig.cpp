#include "TuringConfig.h"

#include <stdlib.h>

#include "BioAssert.h"

using namespace db;

TuringConfig::TuringConfig()
{
    // Create turing directory if it does not exist
    const char* homeEnv = getenv("HOME");
    bioassert(homeEnv && *homeEnv, "$HOME is undefined");

    _turingDir = fs::Path(homeEnv) / ".turing";
    _graphsDir = _turingDir / "graphs";
    _dataDir = _turingDir / "data";
    _vectorDir = _turingDir / "vector";
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
    _vectorDir = _turingDir / "vector";
}
