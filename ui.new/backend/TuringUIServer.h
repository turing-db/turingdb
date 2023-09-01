#pragma once

#include "FileUtils.h"
#include "ServerType.h"

namespace ui {

class ServerThreadEngine;

class TuringUIServer {
public:
    TuringUIServer(const FileUtils::Path& outDir);
    ~TuringUIServer();

    bool start();
    bool startDev();
    void wait();

    int getReturnCode(ServerType serverType) const;
    void getOutput(ServerType serverType, std::string& output) const;

private:
    FileUtils::Path _outDir;
    std::unique_ptr<ServerThreadEngine> _engine;

    void cleanSite();
};

}
