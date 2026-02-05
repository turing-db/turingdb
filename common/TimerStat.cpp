#include "TimerStat.h"

#include "PerfStat.h"

using namespace std::literals;
using FloatDuration = std::chrono::duration<float>;

TimerStat::TimerStat()
    : _start(Clock::now())
{
}

TimerStat::TimerStat(const std::string& msg)
    : _start(Clock::now()),
      _msg(msg)
{
}

TimerStat::~TimerStat() {
    if (!PerfStat::getInstance()) {
        return;
    }

    PerfStat* perfStatInst = PerfStat::getInstance();
    std::ofstream& file = perfStatInst->_outStream;
    if (file.is_open()) {
        const float seconds = duration<Seconds>(_start, Clock::now());

        const auto [reserved, physical] = perfStatInst->getMemInMegabytes();

        file << '[' << _msg << "] "
             << "[vmem=" << reserved << "MB, vrss=" << physical << "MB] "
             << std::to_string(seconds) << " s\n";
    }
}
