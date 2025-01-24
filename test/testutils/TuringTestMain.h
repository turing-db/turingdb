#include <gtest/gtest.h>
#include <spdlog/fmt/bundled/core.h>

#include "Time.h"

namespace turing::test {

using TuringMainConfig = void (*)();

class TuringEventListener : public testing::TestEventListener {
public:
    ~TuringEventListener() override = default;
    TuringEventListener() = default;
    TuringEventListener(const TuringEventListener&) = default;
    TuringEventListener(TuringEventListener&&) = default;
    TuringEventListener& operator=(const TuringEventListener&) = default;
    TuringEventListener& operator=(TuringEventListener&&) = default;

    void OnTestProgramStart(const testing::UnitTest&) override {
        _t0 = Clock::now();

        _today = Clock::to_time_t(_t0);

        std::stringstream buffer;
        buffer << std::put_time(std::localtime(&_today), "%a %b %d %H:%M:%S %Y");

        fmt::print("- Test program started: {}\n", buffer.view());
    }

    void OnTestProgramEnd(const testing::UnitTest&) override {
        const auto t1 = Clock::now();
        fmt::print("- Test program elapsed time: {} s\n", duration<Seconds>(_t0, t1));
    }

    void OnTestIterationStart(const testing::UnitTest&, int) override {}
    void OnEnvironmentsSetUpStart(const testing::UnitTest&) override {}
    void OnEnvironmentsSetUpEnd(const testing::UnitTest&) override {}
    void OnTestSuiteStart(const testing::TestSuite&) override {}
    void OnTestStart(const testing::TestInfo&) override {}
    void OnTestDisabled(const testing::TestInfo&) override {}
    void OnTestPartResult(const testing::TestPartResult&) override {}
    void OnTestEnd(const testing::TestInfo&) override {}
    void OnTestSuiteEnd(const testing::TestSuite&) override {}
    void OnTestCaseEnd(const testing::TestCase&) override {}
    void OnEnvironmentsTearDownStart(const testing::UnitTest&) override {}
    void OnEnvironmentsTearDownEnd(const testing::UnitTest&) override {}
    void OnTestIterationEnd(const testing::UnitTest&, int) override {}

private:
    TimePoint _t0;
    std::time_t _today {};
};


inline int turingTestMain(int argc, char** argv, TuringMainConfig config = nullptr) {
    testing::GTEST_FLAG(print_time) = true;
    testing::GTEST_FLAG(repeat) = 500;
    testing::GTEST_FLAG(shuffle) = true;

    if (config) {
        config();
    }

    ::testing::InitGoogleTest(&argc, argv);

    testing::TestEventListeners& listeners = testing::UnitTest::GetInstance()->listeners();
    delete listeners.Release(listeners.default_result_printer());

    TuringEventListener* listener = new TuringEventListener;
    listeners.Append(listener);

    return RUN_ALL_TESTS();
}

}
