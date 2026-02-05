#pragma once

#include <chrono>

using Clock = std::chrono::high_resolution_clock;
using TimePoint = std::chrono::time_point<Clock>;
using Unit = std::chrono::seconds::period;
using Seconds = std::chrono::duration<float, Unit>;
using Milliseconds = std::chrono::duration<float, std::milli>;
using Microseconds = std::chrono::duration<float, std::micro>;
using Nanoseconds = std::chrono::duration<float, std::nano>;

template <typename T>
concept DurationType = std::is_same_v<T, Seconds>
                    || std::is_same_v<T, Milliseconds>
                    || std::is_same_v<T, Microseconds>
                    || std::is_same_v<T, Nanoseconds>;

template <DurationType T>
float duration(TimePoint t0, TimePoint t1) {
    const T duration = t1 - t0;
    return duration.count();
}
