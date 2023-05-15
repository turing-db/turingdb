#pragma once

#include <chrono>
#include <fstream>

class TimerStat {
public:
    using Clock = std::chrono::high_resolution_clock;
    using TimePoint = std::chrono::time_point<Clock>;
    using SecondsPeriod = std::chrono::seconds::period;
    using FloatSeconds = std::chrono::duration<float, SecondsPeriod>;

    TimerStat(const TimerStat& other) = delete;
    TimerStat(TimerStat&& other) = delete;
    TimerStat& operator=(const TimerStat& other) = delete;
    TimerStat& operator=(TimerStat&& other) = delete;

    TimerStat();
    TimerStat(const std::string& msg);
    ~TimerStat();

private:
    TimePoint _start;
    std::string _msg = "PerfStat";
};
