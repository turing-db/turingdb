#include "LineNoiseHandle.h"

#include <spdlog/spdlog.h>

#include "ProtectedLineNoiseSink.h"

using namespace db;

LineNoiseHandle::LineNoiseHandle()
{
}

LineNoiseHandle::~LineNoiseHandle(){
}

void LineNoiseHandle::initProtectedLogger(const std::shared_ptr<LogSetup::ConsoleSink>& consoleSink) {
    // Install spdlog guard sink so log messages hide/restore the prompt
    auto guardSink = std::make_shared<ProtectedLineNoiseSink>(_lineNoiseState,
                                                              _lineNoiseActive,
                                                              consoleSink);

    std::vector<spdlog::sink_ptr> sinkList;
    sinkList.emplace_back(guardSink);
    for (auto& sink : spdlog::default_logger()->sinks()) {
        if (sink == consoleSink) {
            continue;
        }
        sinkList.emplace_back(sink);
    }
    auto logger = std::make_shared<spdlog::logger>("log_sink",
                                                   sinkList.begin(),
                                                   sinkList.end());
    logger->set_level(spdlog::default_logger()->level());
    spdlog::set_default_logger(logger);
}
