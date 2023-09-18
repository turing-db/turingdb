#pragma once


#include <boost/process.hpp>

#include "BoostProcess.h"
#include "FileUtils.h"

namespace ui {

class ServerCommand {
public:
    ServerCommand(const std::string& name);
    virtual ~ServerCommand();

    virtual void run(ProcessGroup& group) {};
    virtual void runDev(ProcessGroup& group) {};
    void terminate();

    int getReturnCode() const;
    const FileUtils::Path& getLogFilePath() const { return _logFilePath; }
    bool isDone();
    const std::string& getName() const { return _name; }

protected:
    FileUtils::Path _logFilePath;
    const std::string _name;
    ProcessChild _process;
};

}
