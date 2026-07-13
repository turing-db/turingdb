#pragma once

#include "Path.h"

namespace db {

class TuringConfig {
public:
    explicit TuringConfig();
    ~TuringConfig();

    TuringConfig(const TuringConfig&);
    TuringConfig(TuringConfig&&) noexcept;
    TuringConfig& operator=(const TuringConfig&);
    TuringConfig& operator=(TuringConfig&&) noexcept;

    const fs::Path& getTuringDir() const { return _turingDir; }
    const fs::Path& getGraphsDir() const { return _graphsDir; }
    const fs::Path& getDataDir() const { return _dataDir; }
    const fs::Path& getVectorDir() const { return _vectorDir; }
    const fs::Path& getUserExtensionsDir() const { return _userExtensionsDir; }
    const fs::Path& getInstallExtensionsDir() const { return _installExtensionsDir; }
    const fs::Path& getLogsDir() const { return _logsDir; }
    const fs::Path& getLockFilePath() const { return _lockFilePath; }
    const fs::Path& getSocketPath() const { return _socketPath; }

    bool isSyncedOnDisk() const { return _syncedOnDisk; }
    bool usingSystemEvents() const { return _systemEvents; }

    size_t getMaxDataParts() const { return _maxDataParts; }

    void setTuringDirectory(const fs::Path& turingDir);
    void setSyncedOnDisk(bool syncedOnDisk) { _syncedOnDisk = syncedOnDisk; }
    void useSystemEvents(bool systemEvents) { _systemEvents = systemEvents; }
    void setMaxDataParts(size_t maxDataParts) { _maxDataParts = maxDataParts; }

    const std::function<void()>& getOnStopRequest() const { return _onStopRequest; }
    void setOnStopRequest(const std::function<void()>& onStopRequest) {
        _onStopRequest = onStopRequest;
    }

private:
    fs::Path _turingDir;
    fs::Path _graphsDir;
    fs::Path _dataDir;
    fs::Path _vectorDir;
    fs::Path _userExtensionsDir;
    fs::Path _installExtensionsDir;
    fs::Path _logsDir;
    fs::Path _lockFilePath;
    fs::Path _socketPath;

    std::function<void()> _onStopRequest;

    // Maximum number of data parts a graph may hold before COMMIT / CHANGE SUBMIT are
    // rejected and the user is required to run MERGE_DATAPARTS. Overridable via the
    // TURING_MAX_DATAPARTS environment variable.
    size_t _maxDataParts {32};

    bool _syncedOnDisk {true};
    bool _systemEvents {true};
};

}
