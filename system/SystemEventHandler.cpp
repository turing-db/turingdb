#include "SystemEventHandler.h"

#include <signal.h>
#include <unistd.h>
#include <sys/fcntl.h>
#include <sys/socket.h>
#include <sys/signal.h>
#include <sys/un.h>
#include <sys/poll.h>

#include <spdlog/spdlog.h>

#include "ThreadName.h"

using namespace db;

namespace {

struct SignalPipes {
    int _read {-1};
    int _write {-1};

    int* data() {
        return &_read;
    }
} _signalFd {};

int _sockFd = -1;

void sigHandler(int signal) {
    if (_signalFd._write == -1) {
        return;
    }

    uint64_t value = 1;
    if (::write(_signalFd._write, &value, sizeof(value)) != sizeof(value)) {
        // Error, but cannot be checked in signal handler
    }
}

}

std::unique_ptr<SystemEventHandler> SystemEventHandler::_instance = nullptr;

SystemEventHandler::SystemEventHandler(const fs::Path& socketPath)
    : _socketPath(socketPath),
    _onStop([] {})
{
}

SystemEventHandler::~SystemEventHandler() {
    terminate();
}

bool SystemEventHandler::initialize(const fs::Path& socketPath) {
    if (_instance) {
        terminate();
        _instance.reset();
        return false;
    }

    _instance = std::unique_ptr<SystemEventHandler>(new SystemEventHandler(socketPath));

    const bool res = _instance->initializeImpl();
    if (!res) {
        SystemEventHandler::terminate();
    }

    return res;
}

void SystemEventHandler::terminate() {
    if (!_instance) {
        return;
    }

    if (!_instance->_running.exchange(false)) {
        return;
    }

    uint64_t value = 1;
    if (::write(_signalFd._write, &value, sizeof(value)) != sizeof(value)) {
        spdlog::error("Failed to wake signalling thread: {}", strerror(errno));
    }

    _instance->_thread.join();

    if (_signalFd._read != -1) {
        ::close(_signalFd._read);
    }

    if (_signalFd._write != -1) {
        ::close(_signalFd._write);
    }

    if (_sockFd != -1) {
        ::close(_sockFd);
    }

    _signalFd._read = -1;
    _signalFd._write = -1;
    _sockFd = -1;
    _instance->_socketPath.rm();
}

void SystemEventHandler::setOnStop(const std::function<void()>& onStop) {
    if (!_instance) {
        return;
    }

    _instance->_onStop = onStop;
}

bool SystemEventHandler::requestStop(const fs::Path& socketPath) {
    if (!socketPath.exists()) {
        return false;
    }

    const int sockFd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (sockFd < 0) {
        return false;
    }

    sockaddr_un addr {};
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, socketPath.c_str(), sizeof(addr.sun_path) - 1);

    const int res = ::connect(sockFd, (sockaddr*)&addr, sizeof(addr));
    if (res < 0) {
        ::close(sockFd);
        return false;
    }

    constexpr std::string_view stop = "STOP";
    const ssize_t nwrite = ::write(sockFd, stop.data(), stop.size());

    if (nwrite != stop.size()) {
        ::close(sockFd);
        return false;
    }

    // read 'OK'
    char buf[3] {};
    const ssize_t nread = ::read(sockFd, buf, sizeof(buf) - 1);

    if (nread != 2) {
        fmt::println("Failed to read 'OK' {}", nread);
        ::close(sockFd);
        return false;
    }

    ::close(sockFd);

    const std::string_view response {buf, (size_t)nread};

    return response == "OK";
}

bool SystemEventHandler::initializeImpl() {
    // Creating signal event fd (for SIGINT/SIGTERM)
    if (::pipe(_signalFd.data()) < 0) {
        return false;
    }

    // Set non-blocking on both ends
    ::fcntl(_signalFd._read, F_SETFL, O_NONBLOCK);
    ::fcntl(_signalFd._write, F_SETFL, O_NONBLOCK);

    // Creating communication socket (for PING/STOP)
    _sockFd = ::socket(AF_UNIX, SOCK_STREAM, 0);
    if (_sockFd < 0) {
        return false;
    }

    // Removing existing socket file
    if (_socketPath.exists()) {
        if (const fs::Result<void> res = _socketPath.rm(); !res) {
            return false;
        }
    }

    // Binding socket to file
    ::sockaddr_un addr {};
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, _socketPath.c_str(), sizeof(addr.sun_path) - 1);

    const int bindRes = ::bind(_sockFd, (sockaddr*)&addr, sizeof(addr));

    if (bindRes < 0 || ::listen(_sockFd, 4) < 0) {
        return false;
    }

    // Setting up signal handler
    struct sigaction sa {};
    sa.sa_handler = sigHandler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;

    if (::sigaction(SIGINT, &sa, NULL) == -1) {
        return false;
    }
    if (::sigaction(SIGTERM, &sa, NULL) == -1) {
        return false;
    }

    // Starting communication thread
    _running.store(true);

    _thread = std::thread([this]() {
        ThreadName::set("tdb.com");

        std::string cmd;

        while (_running) {
            ::pollfd pfds[2] = {
                {_sockFd,   POLLIN, 0},
                {_signalFd._read, POLLIN, 0},
            };

            const int ret = ::poll(pfds, 2, -1);

            if (ret < 0) {
                if (errno == EINTR) {
                    continue;
                }

                _onStop();
                break;
            }

            // Signal received
            if (pfds[1].revents & POLLIN) {
                uint64_t val = 0;
                [[maybe_unused]] const int res = ::read(_signalFd._read, &val, sizeof(val));
                _onStop();
                break;
            }

            // Socket connection
            if (pfds[0].revents & POLLIN) {
                const int client = ::accept(_sockFd, nullptr, nullptr);
                if (client < 0) {
                    continue;
                }

                char buf[64] {};
                const ssize_t n = ::read(client, buf, sizeof(buf) - 1);
                if (n <= 0) {
                    ::close(client);
                    continue;
                }

                cmd.assign(buf, n);

                if (cmd == "PING") {
                    [[maybe_unused]] const int res = ::write(client, "PONG", 4);
                } else if (cmd == "STOP") {
                    [[maybe_unused]] const int res = ::write(client, "OK", 2);
                    ::close(client);
                    _onStop();
                    break;
                }

                ::close(client);
            }
        }
    });

    return true;
}
