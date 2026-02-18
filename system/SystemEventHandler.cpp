#include "SystemEventHandler.h"

#include <sys/eventfd.h>
#include <sys/socket.h>
#include <sys/signal.h>
#include <sys/un.h>
#include <sys/poll.h>

using namespace db;

namespace {

int _signalFd {-1};
int _sockFd {-1};

void sigHandler(int signal) {
    if (_signalFd == -1) {
        return;
    }

    uint64_t value = 1;
    if (::write(_signalFd, &value, sizeof(value)) != sizeof(value)) {
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
    _running.store(false);

    if (_thread.joinable()) {
        _thread.join();
    }

    if (_signalFd != -1) {
        ::close(_signalFd);
    }

    if (_sockFd != -1) {
        ::close(_sockFd);
    }

    _instance = nullptr;
}

bool SystemEventHandler::initialize(const fs::Path& socketPath) {
    if (_instance) {
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

    _instance->_running.store(false);

    uint64_t value = 1;
    if (::write(_signalFd, &value, sizeof(value)) != sizeof(value)) {
        fmt::println("Failed to wake signalling thread: {}", strerror(errno));
    }

    _instance->_thread.join();
    _instance.reset();
}

void SystemEventHandler::setOnStop(const std::function<void()>& onStop) {
    if (!_instance) {
        return;
    }

    _instance->_onStop = onStop;
}

bool SystemEventHandler::initializeImpl() {
    // Creating signal event fd (for SIGINT/SIGTERM)
    _signalFd = ::eventfd(0, EFD_NONBLOCK);
    if (_signalFd < 0) {
        return false;
    }

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
    struct sigaction sa{};
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
        std::string cmd;

        while (_running) {
            ::pollfd pfds[2] = {
                {_sockFd,   POLLIN, 0},
                {_signalFd, POLLIN, 0},
            };

            const int ret = ::poll(pfds, 2, -1);

            if (ret < 0) {
                if (errno == EINTR) {
                    continue;
                }

                _running = false;
                _onStop();
                break;
            }

            // Signal received
            if (pfds[1].revents & POLLIN) {
                uint64_t val = 0;
                ::read(_signalFd, &val, sizeof(val));
                _running = false;
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
                    ::write(client, "PONG", 4);
                } else if (cmd == "STOP") {
                    ::write(client, "OK", 2);
                    ::close(client);
                    _running = false;
                    _onStop();
                    break;
                }

                ::close(client);
            }
        }
    });

    pthread_setname_np(_thread.native_handle(), "tdb.com");

    return true;
}
