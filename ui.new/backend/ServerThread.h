#pragma once

#include <thread>

#include "FileUtils.h"

namespace ui {

class ServerThread {
public:
    virtual ~ServerThread();

    void run();
    void runDev();
    void join();
    int getReturnCode() const { return _returnCode; }
    const FileUtils::Path& getLogFilePath() const { return _logFilePath; }

protected:
    int _returnCode = 0;
    FileUtils::Path _logFilePath;

private:
    std::thread _thread;

    virtual void task() {};
    virtual void devTask() {};
};

}
