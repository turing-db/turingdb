#include "ProtectedLineNoiseSink.h"

using namespace db;

ProtectedLineNoiseSink::ProtectedLineNoiseSink(struct linenoiseState& ls,
                                               const std::atomic<bool>& active,
                                               const std::shared_ptr<spdlog::sinks::sink>& inner)
    : _ls(&ls),
    _active(&active),
    _inner(inner) 
{
}

void ProtectedLineNoiseSink::detach() {
    const std::lock_guard<std::mutex> lock(mutex_);

    _ls = nullptr;
    _active = nullptr;
}

void ProtectedLineNoiseSink::sink_it_(const spdlog::details::log_msg& msg) {
    if (isEditing()) {
        linenoiseHide(_ls);
    }
    _inner->log(msg);
    if (isEditing()) {
        linenoiseShow(_ls);
    }
}

bool ProtectedLineNoiseSink::isEditing() const {
    return _active != nullptr && _active->load();
}

void ProtectedLineNoiseSink::flush_() {
    _inner->flush();
}
