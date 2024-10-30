#pragma once

#include "FileUtils.h"

class Process;

namespace app {

class TuringApp2Server {
public:
    explicit TuringApp2Server(const FileUtils::Path& outDir);
    ~TuringApp2Server();

    TuringApp2Server(const TuringApp2Server&) = delete;
    TuringApp2Server(TuringApp2Server&&) = delete;
    TuringApp2Server& operator=(const TuringApp2Server&) = delete;
    TuringApp2Server& operator=(TuringApp2Server&&) = delete;

    void setDevMode(bool enabled) { _devMode = enabled; }
    void setBuildRequested(bool req) { _buildRequested = req; }

    bool run();
    void terminate();
    void wait();

private:
    bool _devMode {false};
    bool _buildRequested {false};
    FileUtils::Path _outDir;

    std::unique_ptr<Process> _bunProcess;
};

}
