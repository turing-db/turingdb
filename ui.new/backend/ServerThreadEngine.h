#pragma once

#include "Command.h"
#include "ServerThread.h"
#include "FileUtils.h"

namespace ui {

class ServerThreadEngine {
public:
    enum class ServerType : uint8_t {
        BIOSERVER = 0,
        FLASK,
        REACT,
        TAILWIND
    };

    ServerThreadEngine(const FileUtils::Path& rootDir);
    void run();
    void runDev();
    void wait();
    int getReturnCode() const;

private:
    std::array<std::unique_ptr<ServerThread>, 4> _threads;
    FileUtils::Path _rootDir;
    int _returnCode = 0;
};

}
