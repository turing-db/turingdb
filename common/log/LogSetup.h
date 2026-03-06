#pragma once

#include <spdlog/sinks/stdout_color_sinks.h>
#include <string>

class LogSetup {
public:
    using ConsoleSink = spdlog::sinks::stdout_color_sink_mt;
    LogSetup() = delete;

    static std::shared_ptr<ConsoleSink> setupLogFileBacked(const std::string& path,
                                                           bool truncate = true);
    static void setupLogConsole();

    static void logFlush();
};
