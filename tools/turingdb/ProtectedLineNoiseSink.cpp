#include "ProtectedLineNoiseSink.h"

using namespace db;

ProtectedLineNoiseSink::ProtectedLineNoiseSink(struct linenoiseState& ls,
                                               const std::atomic<bool>& active,
                                               const std::shared_ptr<spdlog::sinks::sink>& inner)
    : _ls(ls),
    _active(active),
    _inner(inner) 
{
}

void ProtectedLineNoiseSink::sink_it_(const spdlog::details::log_msg& msg) {
    if (_active) {
        linenoiseHide(&_ls);
    }
    _inner->log(msg);
    if (_active) {
        linenoiseShow(&_ls);
    }
}

void ProtectedLineNoiseSink::flush_() {
    _inner->flush();
}
