#pragma once

#include <filesystem>
#include <vector>
#include <queue>
#include <list>

class RegressJob;

class RegressTesting {
public:
    using Path = std::filesystem::path;

    RegressTesting(const Path& reportPath);
    ~RegressTesting();

    void setCleanIfSuccess(bool enabled) { _cleanIfSuccess = enabled; }
    void setConcurrency(size_t jobs) { _concurrency = jobs; }
    void setOutputOnFailure(bool v) { _outputOnFailure = v; }

    void run();
    void clean();
    void terminate();

    bool hasFail() const { return !_testFail.empty(); }

private:
    const Path _reportDir;
    size_t _concurrency {1};
    bool _error {false};
    bool _cleanIfSuccess {true};
    bool _outputOnFailure {false};
    std::vector<Path> _testPaths;
    std::vector<Path> _testFail;
    std::vector<Path> _testSuccess;
    std::queue<Path> _testWaitQueue;
    std::list<RegressJob*> _runningTests;

    void analyzeDir(const Path& dir);
    void analyzeTest(const Path& dir);
    void runTests();
    void writeTestResults();
    void cleanDir(const Path& dir, bool keepTopLevelWRT=false);
    void populateRunQueue();
    void processTestTermination(RegressJob* job);
};
