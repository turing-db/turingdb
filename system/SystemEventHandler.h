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
    static void setReady();

    /** @brief Connects to the server's Unix socket and sends a STOP command.
     *
     * @param socketPath Path to the server's Unix domain socket file.
     * @return true if the server acknowledged with "OK", false otherwise.
     *
     * @warning Must only be called from a separate process (e.g. turingdb stop).
     *          Calling it from within the server process is unsafe: this function
     *          temporarily chdirs to the socket directory, which would affect all
     *          relative-path operations in the server. If the chdir restore ever
     *          failed, the server would be left with a corrupted working directory.
     */
    static bool requestStop(const fs::Path& socketPath);

private:
    static std::unique_ptr<SystemEventHandler> _instance;
    fs::Path _socketPath;
    std::function<void()> _onStop;
    std::atomic<bool> _running {false};
    std::atomic<bool> _ready {false};
    std::thread _thread;
    int _sockFd = -1;

    explicit SystemEventHandler(const fs::Path& socketPath);

    bool initializeImpl();
};

}
