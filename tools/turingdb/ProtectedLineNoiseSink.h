#pragma once

#include <mutex>

#include "spdlog/sinks/base_sink.h"
#include "linenoise.h"

/*
 * When DBServer Threads Log To stdout this can cause race condition with linenoise
 * also outputting to stdout. The solution here is to make use of the new line noise
 * async api.
 *
 * The ProctedLineNoiseSink is designed to be  wrapper around the current console sink
 * we create in LogSetup.cpp. Before we log anything we hide the LineNoise buffer and
 * then reinstate it after the log (this is done in the sink_it_() method).
 */

namespace db {

class ProtectedLineNoiseSink : public spdlog::sinks::base_sink<std::mutex> {
public:
    ProtectedLineNoiseSink(struct linenoiseState& ls,
                           const std::atomic<bool>& active,
                           const std::shared_ptr<spdlog::sinks::sink>& inner);

private:
    struct linenoiseState& _ls;
    const std::atomic<bool>& _active;
    std::shared_ptr<spdlog::sinks::sink> _inner;

    void sink_it_(const spdlog::details::log_msg& msg) override;
    void flush_() override;
};

}
