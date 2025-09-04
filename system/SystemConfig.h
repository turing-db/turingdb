#pragma once

#include "Path.h"

namespace db {

class SystemConfig {
public:
    SystemConfig();
    ~SystemConfig();

    const fs::Path& getTuringDir() const { return _turingDir; }
    const fs::Path& getGraphsDir() const { return _graphsDir; }
    const fs::Path& getDataDir() const { return _dataDir; }

    void setTuringDirectory(const fs::Path& turingDir);

private:
    fs::Path _turingDir;
    fs::Path _graphsDir;
    fs::Path _dataDir;

    void init();
};

}
