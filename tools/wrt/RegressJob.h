#pragma once

#include <memory>
#include <boost/asio/deadline_timer.hpp>

#include "FileUtils.h"
#include "BoostProcess.h"

class RegressJob {
public:
    using Path = FileUtils::Path;
    using DeadlineTimer = std::unique_ptr<boost::asio::deadline_timer>;

    RegressJob(const Path& path);
    ~RegressJob();

    const Path& getPath() const { return _path; }

    bool start(ProcessGroup& group);
    bool isRunning();
    void setTimer(DeadlineTimer&& timer) { _timer = std::move(timer); }
    void terminate();
    void wait();

    int getExitCode() const;

private:
    const Path _path;
    ProcessChild _process;
    DeadlineTimer _timer;
};
