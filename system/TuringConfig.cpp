#include "TuringConfig.h"

#include <stdlib.h>
#include <unistd.h>

#ifdef __APPLE__
#include <mach-o/dyld.h>
#endif

#include <spdlog/spdlog.h>

#include "BioAssert.h"

using namespace db;

namespace {

void resolveInstallExtensionsDir(fs::Path& result) {
    const char* envDir = getenv("TURING_EXTENSIONS_DIR");
    if (envDir && *envDir) {
        result = fs::Path(std::string(envDir));
        return;
    }

    constexpr size_t MAX_PATH_LENGTH = 4096;
    char buf[MAX_PATH_LENGTH];
#ifdef __APPLE__
    uint32_t bufSize = sizeof(buf);
    const int ok = _NSGetExecutablePath(buf, &bufSize) == 0;
#else
    ssize_t bufSize = readlink("/proc/self/exe", buf, sizeof(buf) - 1);
    const int ok = bufSize > 0;
    if (ok) {
        buf[bufSize] = '\0';
    }
#endif
    if (ok) {
        const fs::Path exePath(std::string{buf});
        result = exePath.parent().parent() / "lib" / "turingdb" / "extensions";
        return;
    }

    spdlog::warn("Could not resolve install extensions directory");
}

void resolveMaxDataParts(size_t& result) {
    const char* envValue = getenv("TURING_MAX_DATAPARTS");
    if (!envValue || !*envValue) {
        return;
    }

    char* parseEnd = nullptr;
    const unsigned long long parsed = strtoull(envValue, &parseEnd, 10);

    const bool fullyParsed = parseEnd && *parseEnd == '\0';
    if (!fullyParsed || parsed == 0) {
        spdlog::warn("Ignoring invalid TURING_MAX_DATAPARTS value '{}'; keeping default of {}",
                     envValue,
                     result);
        return;
    }

    result = static_cast<size_t>(parsed);
}

} // namespace

TuringConfig::TuringConfig()
    : _onStopRequest([] {})
{
    // Create turing directory if it does not exist
    const char* homeEnv = getenv("HOME");
    bioassert(homeEnv && *homeEnv, "$HOME is undefined");

    setTuringDirectory(fs::Path(homeEnv) / ".turing");

    resolveMaxDataParts(_maxDataParts);
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
    _userExtensionsDir = _turingDir / "extensions";
    _logsDir = _turingDir / "logs";
    _lockFilePath = _turingDir / "turingdb.lock";
    _socketPath = _turingDir / "turingdb.sock";
    resolveInstallExtensionsDir(_installExtensionsDir);
}
