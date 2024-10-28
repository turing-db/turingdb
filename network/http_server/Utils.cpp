#include "Utils.h"

#include <spdlog/spdlog.h>
#include <arpa/inet.h>
#include <cstring>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>

namespace net::utils {

static_assert(INET_ADDRSTRLEN == ADDR_LEN);

bool setNoDelay(Socket s, bool enable) {
    int opt = enable;
    return setsockopt(s, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt)) != -1;
}

bool setReuseAddress(ServerSocket s, bool enable) {
    int opt = enable;
    return setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) != -1;
}

bool setReusePort(ServerSocket s, bool enable) {
    int opt = enable;
    return setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) != -1;
}

bool setKeepAlive(DataSocket s, bool enable) {
    int opt = enable;
    return setsockopt(s, SOL_SOCKET, SO_KEEPALIVE, &opt, sizeof(int)) != -1
        && setKeepAliveIdle(s, 1)
        && setKeepAliveInterval(s, 1)
        && setKeepAliveCount(s, 10);
}

bool setKeepAliveIdle(DataSocket s, int idleTime) {
    return setsockopt(s, IPPROTO_TCP, TCP_KEEPIDLE, &idleTime, sizeof(int)) != -1;
}

bool setKeepAliveInterval(DataSocket s, int interval) {
    return setsockopt(s, IPPROTO_TCP, TCP_KEEPINTVL, &interval, sizeof(int)) != -1;
}

bool setKeepAliveCount(DataSocket s, int count) {
    return setsockopt(s, IPPROTO_TCP, TCP_KEEPCNT, &count, sizeof(int)) != -1;
}

bool setNonBlock(Socket s, bool enable) {
    int flags = fcntl(s, F_GETFL, 0);
    if (flags == -1) {
        return false;
    }

    return enable
             ? fcntl(s, F_SETFL, flags | O_NONBLOCK) != -1
             : fcntl(s, F_SETFL, flags & ~O_NONBLOCK) != -1;
}


inline constexpr socklen_t socketAddrLen = sizeof(struct sockaddr_in);

bool bind(ServerSocket s, const char* address, uint32_t port) {
    sockaddr_in socketAddr {
        .sin_family = AF_INET,
        .sin_port = htons(port),
        .sin_addr = {.s_addr = inet_addr(address)},
    };

    return bind(s, (struct sockaddr*)(&socketAddr), socketAddrLen) != -1;
}

bool listen(ServerSocket s) {
    return ::listen(s, 32) != -1;
}

bool epollAdd(EpollInstance instance, Socket fd, EpollEvent& event) {
    return epoll_ctl(instance, EPOLL_CTL_ADD, fd, &event) != -1;
}

bool epollMod(EpollInstance instance, Socket fd, EpollEvent& event) {
    return epoll_ctl(instance, EPOLL_CTL_MOD, fd, &event) != -1;
}

bool epollDel(EpollInstance instance, Socket fd, EpollEvent& event) {
    return epoll_ctl(instance, EPOLL_CTL_DEL, fd, &event) != -1;
}

void logError(const char* title) {
    spdlog::error("[{}]: {}", title, strerror(errno));
}

StringAddress getStringAddress(uint32_t intAddr) {
    StringAddress addr {};
    inet_ntop(AF_INET, &intAddr, addr.data(), INET_ADDRSTRLEN);
    return addr;
}

}
