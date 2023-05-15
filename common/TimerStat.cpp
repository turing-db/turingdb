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
    std::ofstream& file = PerfStat::getInstance()->_outStream;
    if (file.is_open()) {
        auto duration = Clock::now() - _start;
        float v = std::chrono::duration_cast<FloatSeconds>(duration).count();

        file << '[' << _msg << "] " << std::to_string(v)
               << " s\n";
    }
}
