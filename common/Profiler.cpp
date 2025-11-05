#include "Profiler.h"

#include <mutex>
#include <map>

#include <spdlog/fmt/bundled/core.h>

#include "TuringTime.h"

struct ProfileData {
    Profiler::ProfileID _id;
    std::string_view _message;
    TimePoint _startTime;
    TimePoint _endTime;
    bool _finished = false;
    size_t _nesting = 0;
};

class ProfilerInstance {
public:
    Profiler::ProfileID start(std::string_view message) {
        std::scoped_lock guard(_mutex);
        _profilers[_nextID] = ProfileData {
            ._id = _nextID,
            ._message = message,
            ._startTime = Clock::now(),
            ._nesting = _nesting++,
        };

        return _nextID++;
    }

    void stop(Profiler::ProfileID id) {
        std::scoped_lock guard(_mutex);
        auto profile = _profilers.find(id);

        if (profile == _profilers.end()) {
            return;
        }

        profile->second._endTime = Clock::now();
        profile->second._finished = true;
        _nesting--;
    }

    void dump(std::string& out) {
        std::scoped_lock guard(_mutex);
        for (auto& [id, profile] : _profilers) {
            if (!profile._finished) {
                out += fmt::format("{1:>{0}} [{2}]: running\n", profile._nesting * 2, ' ', profile._message);
                continue;
            }

            const auto dur = duration<Microseconds>(profile._startTime, profile._endTime);
            if (dur < 1000.0f) {
                out += fmt::format("{1:>{0}} [{2}]: {3} us\n", profile._nesting * 2, ' ', profile._message, dur);
            } else if (dur < 1000.0f * 1000.0f) {
                out += fmt::format("{1:>{0}} [{2}]: {3} ms\n", profile._nesting * 2, ' ', profile._message, dur / 1000.0f);
            } else {
                out += fmt::format("{1:>{0}} [{2}]: {3} s\n", profile._nesting * 2, ' ', profile._message, dur / 1000.0f / 1000.0f);
            }
        }
    }

    void clear() {
        std::scoped_lock guard(_mutex);
        _nesting = 0;
        _profilers.clear();
    }

private:
    mutable std::mutex _mutex;
    Profiler::ProfileID _nextID = 0;
    size_t _nesting {0};
    std::map<Profiler::ProfileID, ProfileData> _profilers;
};

static ProfilerInstance _instance;

Profiler::ProfileID Profiler::startImpl(std::string_view message) {
    return _instance.start(message);
}

void Profiler::stopImpl(ProfileID id) {
    _instance.stop(id);
}

void Profiler::dumpImpl(std::string& out) {
    _instance.dump(out);
}

void Profiler::clearImpl() {
    _instance.clear();
}
