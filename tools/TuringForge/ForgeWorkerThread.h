#pragma once

#include "ForgeErrorCounter.h"
#include "HdrHistogram.h"
#include "LocalMemory.h"
#include "ForgeConfig.h"
#include "TuringTime.h"

namespace forge {

class ForgeWorkerThread {
public:
    explicit ForgeWorkerThread(const ForgeConfig* config);
    ~ForgeWorkerThread();

    // Allocate the histograms' bucket storage. Call once before the worker runs.
    void init();

    const ForgeConfig* getConfig() const { return _config; }

    HdrHistogram* getWallClockHistogram() { return &_wallClockHistogram; }
    HdrHistogram* getEngineHistogram() { return &_engineHistogram; }
    HdrHistogram* getChangeCycleHistogram() { return &_changeCycleHistogram; }
    const HdrHistogram* getWallClockHistogram() const { return &_wallClockHistogram; }
    const HdrHistogram* getEngineHistogram() const { return &_engineHistogram; }
    const HdrHistogram* getChangeCycleHistogram() const { return &_changeCycleHistogram; }

    ForgeErrorCounter* getErrorCounter() { return &_errors; }
    const ForgeErrorCounter* getErrorCounter() const { return &_errors; }

    db::LocalMemory* getMemory() { return &_mem; }

    // Start of the measurement window that commences after the warmup period is over
    TimePoint getMeasurementStart() const { return _measurementStart; }
    void setMeasurementStart(TimePoint measurementStart) { _measurementStart = measurementStart; }

    bool isConnected() { return _isConnected; }
    void setConnected() { _isConnected = true; };

private:
    const ForgeConfig* _config {nullptr};
    HdrHistogram _wallClockHistogram {1, 3600000000, 3};   // client-side wall-clock latency (us)
    HdrHistogram _engineHistogram {1, 3600000000, 3};      // server-reported engine time (us)
    HdrHistogram _changeCycleHistogram {1, 3600000000, 3}; // change-cycle latency (us)
    ForgeErrorCounter _errors;
    db::LocalMemory _mem;
    TimePoint _measurementStart {};
    bool _isConnected {false};
};
}
