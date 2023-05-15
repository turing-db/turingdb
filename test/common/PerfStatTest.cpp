#include "PerfStat.h"
#include "TimerStat.h"
#include "FileUtils.h"
#include "gtest/gtest.h"

#include <thread>

using namespace std::literals;

namespace db {

class PerfStatTest : public ::testing::Test {

protected:
    void SetUp() override {
        const testing::TestInfo* const testInfo =
            testing::UnitTest::GetInstance()->current_test_info();

        _logPath = testInfo->test_suite_name();
        _logPath += ".perf";
        _logPath = FileUtils::abspath(_logPath).string();

        if (FileUtils::exists(_logPath)) {
            FileUtils::removeFile(_logPath);
        }
    }

    void TearDown() override {}

    PerfStat::Path _logPath{};
};

TEST_F(PerfStatTest, MeasurePerfs) {
    PerfStat::init(_logPath);

    {
        TimerStat timer("50ms scope");
        std::this_thread::sleep_for(50ms);
    }

    {
        TimerStat timer("70ms scope");
        std::this_thread::sleep_for(70ms);
    }

    {
        TimerStat timer("20ms scope");
        std::this_thread::sleep_for(20ms);
    }

    PerfStat::destroy();

    std::ifstream logFile(_logPath);
    ASSERT_TRUE(logFile);

    std::array<float, 3> durations = { 0.050f, 0.070f, 0.020f };
    size_t i = 0;
    for (std::string line; std::getline(logFile, line);) {
        std::stringstream linestream{line};

        size_t j = 0;
        for (std::string word; std::getline(linestream, word, ' '); ) {
            if (j == 2) {
                std::stringstream wordstream{word};
                float duration = 0.f;
                wordstream >> duration;
                ASSERT_GE(duration, durations[i]);
                break;
            }
            j++;
        }

        i++;
    }
}

}
