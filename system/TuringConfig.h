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
    const std::function<void()>& getOnStopRequest() const { return _onStopRequest; }

    bool isSyncedOnDisk() const { return _syncedOnDisk; }
    void setTuringDirectory(const fs::Path& turingDir);
    void setSyncedOnDisk(bool syncedOnDisk) { _syncedOnDisk = syncedOnDisk; }

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

    bool _syncedOnDisk {true};
};

}
