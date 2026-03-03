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

    char buf[4096];
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

} // namespace

TuringConfig::TuringConfig()
{
    // Create turing directory if it does not exist
    const char* homeEnv = getenv("HOME");
    bioassert(homeEnv && *homeEnv, "$HOME is undefined");

    _turingDir = fs::Path(homeEnv) / ".turing";
    _graphsDir = _turingDir / "graphs";
    _dataDir = _turingDir / "data";
    _vectorDir = _turingDir / "vector";
    _userExtensionsDir = _turingDir / "extensions";
    resolveInstallExtensionsDir(_installExtensionsDir);
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
}
