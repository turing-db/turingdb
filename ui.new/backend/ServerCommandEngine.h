#pragma once

#include "FileUtils.h"
#include "BoostProcess.h"
#include "ServerType.h"

namespace ui {

class ServerCommand;

class ServerCommandEngine {
public:
    ServerCommandEngine(const FileUtils::Path& rootDir);
    ~ServerCommandEngine();

    void run();
    void runDev();
    void terminate();
    ServerType waitServerDone();
    int getReturnCode(ServerType serverType) const;
    void getOutput(ServerType serverType, std::string& output) const;

private:
    std::array<std::unique_ptr<ServerCommand>, 4> _servers;
    ProcessGroup _group;
    FileUtils::Path _rootDir;
};

}
