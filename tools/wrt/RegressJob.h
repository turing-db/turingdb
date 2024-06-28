#pragma once

#include <memory>

#include "FileUtils.h"

class Process;

class RegressJob {
public:
    using Path = FileUtils::Path;

    RegressJob(const Path& path);
    ~RegressJob();

    const Path& getPath() const { return _path; }

    bool start();
    bool isRunning();
    void terminate();
    void wait();

    int getExitCode() const;
    Path getLogPath() const { return _path / "run.log"; }

private:
    const Path _path;
    std::unique_ptr<Process> _process;
};
