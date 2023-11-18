#pragma once

#include <memory>

#include "FileUtils.h"
#include "BoostProcess.h"

class RegressJob {
public:
    using Path = FileUtils::Path;

    RegressJob(const Path& path);
    ~RegressJob();

    const Path& getPath() const { return _path; }

    bool start(ProcessGroup& group);
    bool isRunning();
    void terminate();
    void wait();

    int getExitCode() const;

private:
    const Path _path;
    ProcessChild _process;
};
