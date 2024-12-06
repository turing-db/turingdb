#include <gtest/gtest.h>
#include <spdlog/spdlog.h>

#include "FileUtils.h"
#include "LogSetup.h"
#include "PerfStat.h"

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

        _outDir = testInfo->test_suite_name();
        _outDir += "_";
        _outDir += testInfo->name();
        _outDir += ".out";
        _logPath = FileUtils::Path(_outDir) / "log";
        _perfPath = FileUtils::Path(_outDir) / "perf";

        if (FileUtils::exists(_outDir)) {
            FileUtils::removeDirectory(_outDir);
        }
        FileUtils::createDirectory(_outDir);

        LogSetup::setupLogFileBacked(_logPath.string());
        PerfStat::init(_perfPath);

        initialize();
    }

    virtual void initialize() {}
    virtual void terminate() {}

    void TearDown() final {
        terminate();
        PerfStat::destroy();
    }

    std::string _outDir;
    FileUtils::Path _logPath;
    FileUtils::Path _perfPath;
};
