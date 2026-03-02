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

    bool isSyncedOnDisk() const { return _syncedOnDisk; }
    void setTuringDirectory(const fs::Path& turingDir);
    void setSyncedOnDisk(bool syncedOnDisk) { _syncedOnDisk = syncedOnDisk; }

private:
    fs::Path _turingDir;
    fs::Path _graphsDir;
    fs::Path _dataDir;
    fs::Path _vectorDir;
    fs::Path _userExtensionsDir;
    fs::Path _installExtensionsDir;

    bool _syncedOnDisk {true};
};

}
