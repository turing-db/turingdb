#pragma once

#include "FileUtils.h"

namespace ui {

class ServerThreadEngine;

class TuringUIServer {
public:
    TuringUIServer(const FileUtils::Path& outDir);
    ~TuringUIServer();

    void start();
    void startDev();
    void wait();

    int getReturnCode() const;

private:
    FileUtils::Path _outDir;
    int _returnCode = 0;
    std::unique_ptr<ServerThreadEngine> _engine;

    void cleanSite();
};

}
