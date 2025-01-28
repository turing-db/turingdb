#include "LogSetup.h"

#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/basic_file_sink.h>

namespace {

void setLogPattern(std::shared_ptr<spdlog::logger> logger) {
    logger->set_pattern("[%Y-%m-%d %T] [%l] %v");
}

}

void LogSetup::setupLogFileBacked(const std::string& path) {
    auto consoleSink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    auto fileSink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(path, true);
    spdlog::sinks_init_list sinkList = {consoleSink, fileSink};
    auto logger = std::make_shared<spdlog::logger>("log_sink", sinkList.begin(), sinkList.end());
    setLogPattern(logger);
    logger->flush_on(spdlog::level::info);
    spdlog::set_default_logger(logger);
}

void LogSetup::setupLogConsole() {
    setLogPattern(spdlog::default_logger());
}

void LogSetup::logFlush() {
    spdlog::default_logger()->flush();
}
