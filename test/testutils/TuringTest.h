#pragma once

#include <gtest/gtest.h>
#include <spdlog/spdlog.h>

#include "FileUtils.h"
#include "LogSetup.h"
#include "PerfStat.h"
#include "TuringTestMain.h"

namespace turing::test {

class TuringTest : public ::testing::Test {
public:
    TuringTest() = default;
    ~TuringTest() override = default;

    TuringTest(const TuringTest&) = delete;
    TuringTest(TuringTest&&) = delete;
    TuringTest& operator=(const TuringTest&) = delete;
    TuringTest& operator=(TuringTest&&) = delete;

protected:
    void SetUp() final {
        const testing::TestInfo* const testInfo =
            testing::UnitTest::GetInstance()->current_test_info();

        _testSuiteName = testInfo->test_suite_name();
        _testName = testInfo->name();
        _outDir = _testSuiteName;
        _outDir += "_";
        _outDir += _testName;
        _outDir += ".out";
        _logPath = FileUtils::Path(_outDir) / "log";
        _perfPath = FileUtils::Path(_outDir) / "perf";

        if (FileUtils::exists(_outDir)) {
            FileUtils::removeDirectory(_outDir);
        }
        FileUtils::createDirectory(_outDir);

        LogSetup::setupLogFileBacked(_logPath.string());
        PerfStat::init(_perfPath);

        {
            const char* home = getenv("HOME");
            if (home) {
                if (FileUtils::exists(home)) {
                    const auto graphsDir = FileUtils::Path(home) / "graphs_v2";
                    if (!FileUtils::exists(graphsDir)) {
                        FileUtils::createDirectory(graphsDir);
                    }
                }
            }
        }

        initialize();
    }

    virtual void initialize() {}
    virtual void terminate() {}

    void TearDown() final {
        terminate();
        PerfStat::destroy();
    }

    std::string _outDir;
    std::string _testSuiteName;
    std::string _testName;
    FileUtils::Path _logPath;
    FileUtils::Path _perfPath;
};
}
