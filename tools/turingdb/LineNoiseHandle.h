#pragma once

#include <atomic>
#include <memory>

#include <linenoise.h>

#include "LogSetup.h"

namespace db {

class LineNoiseHandle {
public:
    static constexpr uint32_t BUFSIZE = 4096;

    LineNoiseHandle();
    ~LineNoiseHandle();

    void initProtectedLogger(const std::shared_ptr<LogSetup::ConsoleSink>& consoleSink);
    linenoiseState* getState() { return &_lineNoiseState; }

    void startEditing(const char* prompt);
    void stopEditing();

    bool isActive() { return _lineNoiseActive.load(); }

private:
    struct linenoiseState _lineNoiseState {};
    char _lineNoiseBuffer[BUFSIZE] {};
    std::atomic<bool> _lineNoiseActive {false};
};

}
