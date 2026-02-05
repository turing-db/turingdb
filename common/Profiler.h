#pragma once

#ifndef TURING_PROFILE
#define TURING_PROFILE false
#endif

#include <string_view>
#include <stdint.h>

class Profiler {
public:
    using ProfileID = uint64_t;

    static ProfileID start(std::string_view message) {
        if constexpr (!_profiling) {
            return 0;
        } else {
            return startImpl(message);
        }
    }

    static void stop(ProfileID id) {
        if constexpr (!_profiling) {
            return;
        } else {
            stopImpl(id);
        }
    }

    static void dump(std::string& out) {
        if constexpr (!_profiling) {
            return;
        } else {
            dumpImpl(out);
        }
    }

    static void clear() {
        if constexpr (!_profiling) {
            return;
        } else {
            clearImpl();
        }
    }

    static void dumpAndClear(std::string& out) {
        dump(out);
        clear();
    }

private:
    static constexpr bool _profiling = TURING_PROFILE;

    static ProfileID startImpl(std::string_view message);
    static void stopImpl(ProfileID);
    static void dumpImpl(std::string& out);
    static void clearImpl();
};

class Profile {
public:
    Profile(std::string_view message)
        : _id(Profiler::start(message))
    {
    }

    ~Profile() {
        Profiler::stop(_id);
    }

    Profile(const Profile&) = delete;
    Profile(Profile&&) = delete;
    Profile& operator=(const Profile&) = delete;
    Profile& operator=(Profile&&) = delete;

    Profiler::ProfileID getID() const {
        return _id;
    }

private:
    Profiler::ProfileID _id = UINT64_MAX;
};
