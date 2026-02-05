#include "Profiler.h"

#include <algorithm>
#include <map>
#include <mutex>
#include <unordered_map>
#include <vector>

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
        const auto profile = _profilers.find(id);

        if (profile == _profilers.end()) {
            return;
        }

        profile->second._endTime = Clock::now();
        profile->second._finished = true;
        _nesting--;
    }

    void dump(std::string& out) {
        std::scoped_lock guard(_mutex);
        _timings.clear();

        for (auto& [id, profile] : _profilers) {
            if (!profile._finished) {
                out += fmt::format("{1:>{0}} [{2}]: running\n", profile._nesting * 2, ' ', profile._message);
                continue;
            }

            const auto dur = duration<Microseconds>(profile._startTime, profile._endTime);
            _timings[profile._message] += dur;
        }

        _timingsSorted.resize(_timings.size());

        size_t i = 0;
        float maxProfiled = 0.0f;
        for (auto& [message, dur] : _timings) {
            _timingsSorted[i++] = {message, dur};
            maxProfiled = std::max(maxProfiled, dur);
        }

        std::sort(_timingsSorted.begin(), _timingsSorted.end(), [](auto& a, auto& b) {
            return a.second < b.second;
        });

        for (auto& [message, dur] : _timingsSorted) {
            if (dur < 1000.0f) {
                out += fmt::format("[{}]: {:.3f} us ({:.2f} %)\n", message, dur, dur / maxProfiled * 100.0f);
            } else if (dur < 1000.0f * 1000.0f) {
                out += fmt::format("[{}]: {:.3f} ms ({:.2f} %)\n", message, dur / 1000.0f, dur / maxProfiled * 100.0f);
            } else {
                out += fmt::format("[{}]: {:.3f} s ({:.2f} %)\n", message, dur / 1000.0f / 1000.0f, dur / maxProfiled * 100.0f);
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
    std::unordered_map<std::string_view, float> _timings;
    std::vector<std::pair<std::string_view, float>> _timingsSorted;
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
