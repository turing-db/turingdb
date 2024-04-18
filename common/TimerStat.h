#pragma once

#include "Time.h"
#include <string>

class TimerStat {
public:
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
