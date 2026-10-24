#include "DemonReadiness.h"

#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include <spdlog/fmt/fmt.h>

#include "TuringException.h"

namespace {

constexpr char READY_TAG = 'R';
constexpr char FAILED_TAG = 'F';

}

DemonReadiness::DemonReadiness()
{
}

DemonReadiness::~DemonReadiness() {
    closeReadEnd();
    closeWriteEnd();
}

void DemonReadiness::open() {
    int sockets[2] {-1, -1};
    if (::socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) != 0) {
        throw TuringException(fmt::format("Could not create the demon readiness channel: {}", strerror(errno)));
    }

    _readSocket = sockets[0];
    _writeSocket = sockets[1];

    ::fcntl(_readSocket, F_SETFD, FD_CLOEXEC);
    ::fcntl(_writeSocket, F_SETFD, FD_CLOEXEC);
}

void DemonReadiness::keepReadEnd() {
    closeWriteEnd();
}

void DemonReadiness::keepWriteEnd() {
    closeReadEnd();
}

void DemonReadiness::notifyReady() {
    if (_writeSocket == -1) {
        return;
    }

    send({&READY_TAG, 1});
    closeWriteEnd();
}

void DemonReadiness::notifyFailure(std::string_view error) {
    if (_writeSocket == -1) {
        return;
    }

    send({&FAILED_TAG, 1});
    send(error);
    closeWriteEnd();
}

DemonStartResult DemonReadiness::waitForDemon(std::string& error) {
    char tag = 0;
    if (receive(&tag, 1) == 0) {
        return DemonStartResult::Died;
    } else if (tag == READY_TAG) {
        return DemonStartResult::Ready;
    }

    char buffer[4096];
    size_t received = 0;
    while ((received = receive(buffer, sizeof(buffer))) > 0) {
        error.append(buffer, received);
    }

    return DemonStartResult::Failed;
}

void DemonReadiness::send(std::string_view data) {
    while (!data.empty()) {
        const ssize_t sent = ::send(_writeSocket, data.data(), data.size(), MSG_NOSIGNAL);

        if (sent < 0 && errno == EINTR) {
            continue;
        } else if (sent <= 0) {
            return;
        }

        data.remove_prefix(static_cast<size_t>(sent));
    }
}

size_t DemonReadiness::receive(char* buffer, size_t size) {
    while (true) {
        const ssize_t received = ::recv(_readSocket, buffer, size, 0);

        if (received >= 0) {
            return static_cast<size_t>(received);
        } else if (errno != EINTR) {
            return 0;
        }
    }
}

void DemonReadiness::closeReadEnd() {
    if (_readSocket != -1) {
        ::close(_readSocket);
        _readSocket = -1;
    }
}

void DemonReadiness::closeWriteEnd() {
    if (_writeSocket != -1) {
        ::close(_writeSocket);
        _writeSocket = -1;
    }
}
