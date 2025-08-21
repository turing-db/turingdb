#pragma once

#include "Path.h"

namespace db {

class SystemConfig {
public:
    SystemConfig(const fs::Path& turingDir);
    ~SystemConfig();

    void init();

    const fs::Path& getGraphsDir() const { return _graphsDir; }
    const fs::Path& getDataDir() const { return _dataDir; }

private:
    fs::Path _turingDir;
    fs::Path _graphsDir;
    fs::Path _dataDir;
};

}
