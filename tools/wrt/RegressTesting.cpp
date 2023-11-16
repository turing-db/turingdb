#include "RegressTesting.h"

#include <stdlib.h>
#include <sstream>
#include <string>
#include <thread>
#include <chrono>

#include <boost/process.hpp>

#include "FileUtils.h"
#include "RegressJob.h"

#include "TimerStat.h"
#include "BioLog.h"
#include "MsgCommon.h"
#include "MsgWRT.h"

using namespace Log;
using namespace std::chrono_literals;

RegressTesting::RegressTesting(const Path& reportDir)
    : _reportDir(reportDir),
    _processGroup(std::make_unique<boost::process::group>())
{
}

RegressTesting::~RegressTesting() {
}

void RegressTesting::run() {
    // Check that TURING_HOME exists
    const char* turingHome = getenv("TURING_HOME");
    if (!turingHome) {
        BioLog::log(msg::ERROR_INCORRECT_ENV_SETUP());
        return;
    }

    const auto currentDir = FileUtils::cwd();
    
    {
        TimerStat timerStat("Analyze tests");
        analyzeDir(currentDir);
        if (_error) {
            BioLog::echo("Some tests have errors, please fix test setup errors and retry.");
            return;
        }
    }

    BioLog::echo("\nTests detected: "+std::to_string(_testPaths.size())+"\n");

    runTests();

    writeTestResults();
}

void RegressTesting::clean() {
    TimerStat timerStat("Clean tests");
    const auto currentDir = FileUtils::cwd();

    BioLog::echo("Cleaning all tests");
    cleanDir(currentDir);
}

void RegressTesting::analyzeDir(const Path& dir) {
    const auto testListFile = dir/"test.list";
    if (!FileUtils::exists(testListFile)) {
        analyzeTest(dir);
        return;
    }

    std::string testList;
    if (!FileUtils::readContent(testListFile, testList)) {
        BioLog::log(msg::ERROR_FAILED_TO_OPEN_FOR_READ() << testListFile.string());
        _error = true;
        return;
    }

    std::istringstream f(testList);
    std::string line;
    Path testPath;
    while (std::getline(f, line)) {
        if (!line.empty()) {
            testPath = dir/line;
            if (!FileUtils::isDirectory(testPath)) {
                BioLog::log(msg::ERROR_DIRECTORY_NOT_EXISTS() << testPath.string());
                _error = true;
                continue;
            }

            analyzeDir(testPath);
        }
    }
}

void RegressTesting::analyzeTest(const Path& dir) {
    const auto runScriptPath = dir/"run.sh";
    if (!FileUtils::exists(runScriptPath)) {
        BioLog::log(msg::ERROR_FILE_NOT_EXISTS() << runScriptPath.string());
        _error = true;
        return;
    }

    _testPaths.push_back(dir);
}

void RegressTesting::runTests() {
    TimerStat timerStat("Run tests");

    // Add all detected tests to the wait queue
    for (const auto& path : _testPaths) {
        _testWaitQueue.push(path);
    }

    if (_testWaitQueue.empty()) {
        return;
    }

    // Put first job in the run set
    // We get a first job to run until we found one that can start
    populateRunQueue();

    while (!_runningTests.empty()) {
        _ioContext.poll();

        // Check if a job is finished
        auto it = _runningTests.begin();
        while (it != _runningTests.end()) {
            RegressJob* job = *it;
            if (!job->isRunning()) {
                processTestTermination(job);

                const auto toBeRemoved = it;
                ++it;
                _runningTests.erase(toBeRemoved);
                delete job;
            } else {
                it++;
            }
        }

        populateRunQueue();

        std::this_thread::sleep_for(200ms);
    }
}

void RegressTesting::writeTestResults() {
    // Tests summary
    std::string summary;
    summary += "Tests passed: " + std::to_string(_testSuccess.size()) + "\n";
    summary += "Tests failed: " + std::to_string(_testFail.size()) + "\n";
    summary += "Total tests detected: " + std::to_string(_testPaths.size()) + "\n";

    BioLog::echo("");
    BioLog::echo(summary);
    
    const auto testSumFile = _reportDir/"wrt.sum";
    if (!FileUtils::writeFile(testSumFile, summary)) {
        BioLog::log(msg::ERROR_FAILED_TO_WRITE_FILE() << testSumFile.string());
        return;
    }
}

void RegressTesting::cleanDir(const Path& dir, bool keepTopLevelWRT) {
    if (!FileUtils::isDirectory(dir)) {
        return;
    }

    for (const auto& entry : std::filesystem::directory_iterator(dir)) {
        const auto entryPath = entry.path();
        if (FileUtils::isDirectory(entryPath)) {
            if (keepTopLevelWRT && entryPath.filename() == "wrt.out") {
                continue;
            }

            if (entryPath.extension() == ".out") {
                if (!FileUtils::removeDirectory(entryPath)) {
                    BioLog::log(msg::ERROR_FAILED_TO_REMOVE_DIRECTORY()
                                << entryPath.string());
                }
            }

            cleanDir(entryPath, false);
        } else {
            const auto name = entryPath.filename();
            if (name == "wrt.sum"
                || name == "run.log"
                || name == "cmd.sh") {

                if (!FileUtils::removeFile(entryPath)) {
                    BioLog::log(msg::ERROR_FAILED_TO_REMOVE_FILE()
                                << entryPath.string());
                }
            }
        }
    }
}

void RegressTesting::populateRunQueue() {
    const int capacity = _concurrency - _runningTests.size();
    int jobRank = 0;

    while (jobRank < capacity && !_testWaitQueue.empty()) {
        const auto firstTest = _testWaitQueue.front();
        _testWaitQueue.pop();
        RegressJob* job = new RegressJob(firstTest);
        if (!job->start(_processGroup)) {
            continue;
        }

        // Register timer
        createTimerForJob(job);

        _runningTests.push_back(job);
        jobRank++;
    }
}

void RegressTesting::processTestTermination(RegressJob* job) {
    job->wait();

    const int exitCode = job->getExitCode();
    const auto& path = job->getPath();
    if (exitCode == 0) {
        BioLog::echo("Pass: "+path.string());
        _testSuccess.emplace_back(path);

        if (_cleanIfSuccess) {
            cleanDir(path, true);
        }
    } else {
        BioLog::echo("Fail: "+path.string());
        _testFail.emplace_back(path);
    }
}

void RegressTesting::createTimerForJob(RegressJob* job) {
    const auto interval = boost::posix_time::seconds(_timeout);
    auto timer = std::make_unique<boost::asio::deadline_timer>(_ioContext, interval);
    timer->async_wait([job](const boost::system::error_code& error){
        BioLog::echo("Timeout: "+job->getPath().string());
        job->terminate();
    });
    job->setTimer(std::move(timer));
}
