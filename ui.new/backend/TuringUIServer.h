#pragma once

#include "FileUtils.h"
#include "ServerType.h"

namespace ui {

class ServerCommandEngine;

class TuringUIServer {
public:
    TuringUIServer(const FileUtils::Path& outDir);
    ~TuringUIServer();

    bool start();
    bool startDev();
    void terminate();
    ServerType waitServerDone();

    int getReturnCode(ServerType serverType) const;
    void getOutput(ServerType serverType, std::string& output) const;

private:
    FileUtils::Path _outDir;
    std::unique_ptr<ServerCommandEngine> _engine;

    void cleanSite();
};

}
