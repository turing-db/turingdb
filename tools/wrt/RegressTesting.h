#pragma once

#include <filesystem>
#include <vector>
#include <queue>
#include <list>

#include <boost/asio/io_context.hpp>
#include "BoostProcess.h"

class RegressJob;

class RegressTesting {
public:
    using Path = std::filesystem::path;

    RegressTesting(const Path& reportPath);
    ~RegressTesting();

    void setTimeout(size_t seconds) { _timeout = seconds; }

    void run();
    void clean();

private:
    const Path _reportDir;
    int _concurrency {1};
    size_t _timeout {3600};
    bool _error {false};
    ProcessGroup _processGroup;
    std::vector<Path> _testPaths;
    std::vector<Path> _testFail;
    std::vector<Path> _testSuccess;
    std::queue<Path> _testWaitQueue;
    std::list<RegressJob*> _runningTests;
    boost::asio::io_context _ioContext;

    void analyzeDir(const Path& dir);
    void analyzeTest(const Path& dir);
    void runTests();
    void writeTestResults();
    void cleanDir(const Path& dir);
    void populateRunQueue();
    void processTestTermination(RegressJob* job);
    void createTimerForJob(RegressJob* job);
};
