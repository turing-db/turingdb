#pragma once

#include <atomic>
#include <functional>
#include <thread>

#include "Path.h"

namespace db {

class SystemEventHandler {
public:
    ~SystemEventHandler();

    SystemEventHandler(const SystemEventHandler&) = delete;
    SystemEventHandler(SystemEventHandler&&) = delete;
    SystemEventHandler& operator=(const SystemEventHandler&) = delete;
    SystemEventHandler& operator=(SystemEventHandler&&) = delete;

    static bool initialize(const fs::Path& socketPath);
    static void terminate();

    static void setOnStop(const std::function<void()>& onStop);

    static bool requestStop(const fs::Path& socketPath);

private:
    static std::unique_ptr<SystemEventHandler> _instance;
    fs::Path _socketPath;
    std::function<void()> _onStop;
    std::atomic<bool> _running {false};
    std::thread _thread;

    explicit SystemEventHandler(const fs::Path& socketPath);

    bool initializeImpl();
};

}
