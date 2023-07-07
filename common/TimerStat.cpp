#include "TimerStat.h"
#include "PerfStat.h"
#include "FileUtils.h"

using namespace std::literals;
using Clock = std::chrono::system_clock;
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
        const auto duration = Clock::now() - _start;
        const float seconds = std::chrono::duration_cast<FloatSeconds>(duration).count();
        const size_t memUsage = perfStatInst->getReservedMemInMegabytes();

        file << '[' << _msg << "] " 
             << "[vmem " << memUsage << " MB] "
             << std::to_string(seconds) << " s\n";
    }
}
