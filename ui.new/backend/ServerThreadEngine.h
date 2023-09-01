#pragma once

#include "Command.h"
#include "FileUtils.h"
#include "ServerType.h"

namespace ui {

class ServerThread;

class ServerThreadEngine {
public:
    ServerThreadEngine(const FileUtils::Path& rootDir);
    void run();
    void runDev();
    void wait();
    int getReturnCode(ServerType serverType) const;
    void getOutput(ServerType serverType, std::string& output) const;

private:
    std::array<std::unique_ptr<ServerThread>, 4> _threads;
    FileUtils::Path _rootDir;
};

}
